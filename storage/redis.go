package storage

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/redis/go-redis/v9"

	"prxs/common"
)

// RegistrationRecord tracks an active provider session.
// This is duplicated from cmd/registry/main.go to avoid circular dependencies.
type RegistrationRecord struct {
	LastSeen    time.Time
	ServiceCard common.ServiceCard
	StakeProof  *common.StakeProof
	AddrInfo    peer.AddrInfo
}

// FreezedStake represents a stake that is temporarily frozen during unregistration.
// This is duplicated from cmd/registry/main.go to avoid circular dependencies.
type FreezedStake struct {
	ID        string `json:"id"`
	PeerID    string `json:"peer_id"` // stored as string for JSON serialization
	CreatedAt int64  `json:"created_at"`
}

// RedisStorage handles all Redis operations for registry state persistence.
type RedisStorage struct {
	client *redis.Client
	ttl    time.Duration
}

// NewRedisStorage creates a new Redis storage instance.
// If addr is empty, returns nil (Redis is disabled).
func NewRedisStorage(addr string, ttl time.Duration) (*RedisStorage, error) {
	if addr == "" {
		return nil, nil
	}

	client := redis.NewClient(&redis.Options{
		Addr: addr,
		DB:   0,
	})

	// Test connection
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := client.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("failed to connect to Redis at %s: %v", addr, err)
	}

	log.Printf("[Storage] Redis connected: addr=%s\n", addr)

	return &RedisStorage{
		client: client,
		ttl:    ttl,
	}, nil
}

// SaveRegistration stores a registration record in Redis.
func (r *RedisStorage) SaveRegistration(ctx context.Context, pid peer.ID, record *RegistrationRecord) error {
	if r == nil || r.client == nil {
		return nil
	}

	data, err := json.Marshal(record)
	if err != nil {
		return fmt.Errorf("failed to marshal registration: %v", err)
	}

	key := fmt.Sprintf("registration:%s", pid.String())
	if err := r.client.Set(ctx, key, data, r.ttl).Err(); err != nil {
		return fmt.Errorf("failed to save to redis: %v", err)
	}

	// Also maintain a service index in Redis (set of peer IDs for each service name)
	serviceKey := fmt.Sprintf("service:%s", record.ServiceCard.Name)
	if err := r.client.SAdd(ctx, serviceKey, pid.String()).Err(); err != nil {
		return fmt.Errorf("failed to add to service index: %v", err)
	}
	r.client.Expire(ctx, serviceKey, r.ttl)

	return nil
}

// LoadRegistration retrieves a registration record from Redis.
func (r *RedisStorage) LoadRegistration(ctx context.Context, pid peer.ID) (*RegistrationRecord, error) {
	if r == nil || r.client == nil {
		return nil, fmt.Errorf("redis not configured")
	}

	key := fmt.Sprintf("registration:%s", pid.String())
	data, err := r.client.Get(ctx, key).Bytes()
	if err != nil {
		return nil, err
	}

	var record RegistrationRecord
	if err := json.Unmarshal(data, &record); err != nil {
		return nil, fmt.Errorf("failed to unmarshal registration: %v", err)
	}

	return &record, nil
}

// DeleteRegistration removes a registration record from Redis.
func (r *RedisStorage) DeleteRegistration(ctx context.Context, pid peer.ID, serviceName string) error {
	if r == nil || r.client == nil {
		return nil
	}

	key := fmt.Sprintf("registration:%s", pid.String())
	if err := r.client.Del(ctx, key).Err(); err != nil {
		log.Printf("[Storage] Failed to delete registration from Redis: %v", err)
	}

	// Remove from service index
	serviceKey := fmt.Sprintf("service:%s", serviceName)
	if err := r.client.SRem(ctx, serviceKey, pid.String()).Err(); err != nil {
		log.Printf("[Storage] Failed to remove from service index in Redis: %v", err)
	}

	return nil
}

// RestoreAllRegistrations retrieves all registrations from Redis.
// This is used during startup to restore the registry state.
func (r *RedisStorage) RestoreAllRegistrations(ctx context.Context) (map[peer.ID]*RegistrationRecord, error) {
	if r == nil || r.client == nil {
		return nil, fmt.Errorf("redis not configured")
	}

	registrations := make(map[peer.ID]*RegistrationRecord)
	now := time.Now()
	restoredCount := 0
	skippedCount := 0

	// Scan for all registration keys
	iter := r.client.Scan(ctx, 0, "registration:*", 0).Iterator()
	for iter.Next(ctx) {
		key := iter.Val()
		data, err := r.client.Get(ctx, key).Bytes()
		if err != nil {
			log.Printf("[Storage] Warning: Failed to read key %s: %v", key, err)
			continue
		}

		var record RegistrationRecord
		if err := json.Unmarshal(data, &record); err != nil {
			log.Printf("[Storage] Warning: Failed to unmarshal record for key %s: %v", key, err)
			continue
		}

		// Skip stale records (older than 90 seconds, matching the GC logic)
		if now.Sub(record.LastSeen) > 90*time.Second {
			skippedCount++
			log.Printf("[Storage] Skipping stale registration: %s (last seen: %s)", key, record.LastSeen.Format(time.RFC3339))
			continue
		}

		// Extract peer ID from key (format: "registration:<peerID>")
		peerIDStr := strings.TrimPrefix(key, "registration:")
		pid, err := peer.Decode(peerIDStr)
		if err != nil {
			log.Printf("[Storage] Warning: Failed to decode peer ID from key %s: %v", key, err)
			continue
		}

		registrations[pid] = &record
		restoredCount++
	}

	if err := iter.Err(); err != nil {
		return nil, fmt.Errorf("redis scan error: %v", err)
	}

	log.Printf("[Storage] Restored %d registrations from Redis (%d stale records skipped)", restoredCount, skippedCount)
	return registrations, nil
}

// SavePeerStakes saves the stake IDs for a specific peer to Redis.
func (r *RedisStorage) SavePeerStakes(ctx context.Context, pid peer.ID, stakes []string) error {
	if r == nil || r.client == nil {
		return nil
	}

	key := fmt.Sprintf("peer_stakes:%s", pid.String())

	// Delete the old list and set the new one
	pipe := r.client.Pipeline()
	pipe.Del(ctx, key)
	if len(stakes) > 0 {
		pipe.RPush(ctx, key, stringSliceToInterface(stakes)...)
	}

	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("failed to save peer stakes: %v", err)
	}

	return nil
}

// LoadPeerStakes loads the stake IDs for a specific peer from Redis.
func (r *RedisStorage) LoadPeerStakes(ctx context.Context, pid peer.ID) ([]string, error) {
	if r == nil || r.client == nil {
		return nil, fmt.Errorf("redis not configured")
	}

	key := fmt.Sprintf("peer_stakes:%s", pid.String())
	stakes, err := r.client.LRange(ctx, key, 0, -1).Result()
	if err != nil {
		if err == redis.Nil {
			return []string{}, nil
		}
		return nil, fmt.Errorf("failed to load peer stakes: %v", err)
	}

	return stakes, nil
}

// DeletePeerStakes removes the stake IDs for a specific peer from Redis.
func (r *RedisStorage) DeletePeerStakes(ctx context.Context, pid peer.ID) error {
	if r == nil || r.client == nil {
		return nil
	}

	key := fmt.Sprintf("peer_stakes:%s", pid.String())
	if err := r.client.Del(ctx, key).Err(); err != nil {
		log.Printf("[Storage] Failed to delete peer stakes from Redis: %v", err)
	}

	return nil
}

// SaveFreezedPeerStakes saves the frozen stakes for a specific peer to Redis.
func (r *RedisStorage) SaveFreezedPeerStakes(ctx context.Context, pid peer.ID, stakes []FreezedStake) error {
	if r == nil || r.client == nil {
		return nil
	}

	key := fmt.Sprintf("freezed_peer_stakes:%s", pid.String())

	// Delete old data
	if err := r.client.Del(ctx, key).Err(); err != nil {
		return fmt.Errorf("failed to clear freezed peer stakes: %v", err)
	}

	// Save new data if any
	if len(stakes) > 0 {
		data, err := json.Marshal(stakes)
		if err != nil {
			return fmt.Errorf("failed to marshal freezed peer stakes: %v", err)
		}
		if err := r.client.Set(ctx, key, data, 0).Err(); err != nil {
			return fmt.Errorf("failed to save freezed peer stakes: %v", err)
		}
	}

	return nil
}

// LoadFreezedPeerStakes loads the frozen stakes for a specific peer from Redis.
func (r *RedisStorage) LoadFreezedPeerStakes(ctx context.Context, pid peer.ID) ([]FreezedStake, error) {
	if r == nil || r.client == nil {
		return nil, fmt.Errorf("redis not configured")
	}

	key := fmt.Sprintf("freezed_peer_stakes:%s", pid.String())
	data, err := r.client.Get(ctx, key).Bytes()
	if err != nil {
		if err == redis.Nil {
			return []FreezedStake{}, nil
		}
		return nil, fmt.Errorf("failed to load freezed peer stakes: %v", err)
	}

	var stakes []FreezedStake
	if err := json.Unmarshal(data, &stakes); err != nil {
		return nil, fmt.Errorf("failed to unmarshal freezed peer stakes: %v", err)
	}

	return stakes, nil
}

// DeleteFreezedPeerStakes removes the frozen stakes for a specific peer from Redis.
func (r *RedisStorage) DeleteFreezedPeerStakes(ctx context.Context, pid peer.ID) error {
	if r == nil || r.client == nil {
		return nil
	}

	key := fmt.Sprintf("freezed_peer_stakes:%s", pid.String())
	if err := r.client.Del(ctx, key).Err(); err != nil {
		log.Printf("[Storage] Failed to delete freezed peer stakes from Redis: %v", err)
	}

	return nil
}

// SaveFreezedStakes saves the global list of all frozen stakes to Redis.
func (r *RedisStorage) SaveFreezedStakes(ctx context.Context, stakes []FreezedStake) error {
	if r == nil || r.client == nil {
		return nil
	}

	key := "freezed_stakes"
	data, err := json.Marshal(stakes)
	if err != nil {
		return fmt.Errorf("failed to marshal freezed stakes: %v", err)
	}

	if err := r.client.Set(ctx, key, data, 0).Err(); err != nil {
		return fmt.Errorf("failed to save freezed stakes: %v", err)
	}

	return nil
}

// LoadFreezedStakes loads the global list of all frozen stakes from Redis.
func (r *RedisStorage) LoadFreezedStakes(ctx context.Context) ([]FreezedStake, error) {
	if r == nil || r.client == nil {
		return nil, fmt.Errorf("redis not configured")
	}

	key := "freezed_stakes"
	data, err := r.client.Get(ctx, key).Bytes()
	if err != nil {
		if err == redis.Nil {
			return []FreezedStake{}, nil
		}
		return nil, fmt.Errorf("failed to load freezed stakes: %v", err)
	}

	var stakes []FreezedStake
	if err := json.Unmarshal(data, &stakes); err != nil {
		return nil, fmt.Errorf("failed to unmarshal freezed stakes: %v", err)
	}

	return stakes, nil
}

// RestoreAllPeerStakes retrieves all peer stakes from Redis.
// Returns a map of peer.ID to slice of stake IDs.
func (r *RedisStorage) RestoreAllPeerStakes(ctx context.Context) (map[peer.ID][]string, error) {
	if r == nil || r.client == nil {
		return nil, fmt.Errorf("redis not configured")
	}

	peerStakes := make(map[peer.ID][]string)

	// Scan for all peer_stakes keys
	iter := r.client.Scan(ctx, 0, "peer_stakes:*", 0).Iterator()
	for iter.Next(ctx) {
		key := iter.Val()

		// Extract peer ID from key (format: "peer_stakes:<peerID>")
		peerIDStr := strings.TrimPrefix(key, "peer_stakes:")
		pid, err := peer.Decode(peerIDStr)
		if err != nil {
			log.Printf("[Storage] Warning: Failed to decode peer ID from key %s: %v", key, err)
			continue
		}

		stakes, err := r.LoadPeerStakes(ctx, pid)
		if err != nil {
			log.Printf("[Storage] Warning: Failed to load stakes for peer %s: %v", pid.ShortString(), err)
			continue
		}

		if len(stakes) > 0 {
			peerStakes[pid] = stakes
		}
	}

	if err := iter.Err(); err != nil {
		return nil, fmt.Errorf("redis scan error: %v", err)
	}

	log.Printf("[Storage] Restored stakes for %d peers from Redis", len(peerStakes))
	return peerStakes, nil
}

// RestoreAllFreezedPeerStakes retrieves all freezed peer stakes from Redis.
// Returns a map of peer.ID to slice of FreezedStake.
func (r *RedisStorage) RestoreAllFreezedPeerStakes(ctx context.Context) (map[peer.ID][]FreezedStake, error) {
	if r == nil || r.client == nil {
		return nil, fmt.Errorf("redis not configured")
	}

	freezedPeerStakes := make(map[peer.ID][]FreezedStake)

	// Scan for all freezed_peer_stakes keys
	iter := r.client.Scan(ctx, 0, "freezed_peer_stakes:*", 0).Iterator()
	for iter.Next(ctx) {
		key := iter.Val()

		// Extract peer ID from key (format: "freezed_peer_stakes:<peerID>")
		peerIDStr := strings.TrimPrefix(key, "freezed_peer_stakes:")
		pid, err := peer.Decode(peerIDStr)
		if err != nil {
			log.Printf("[Storage] Warning: Failed to decode peer ID from key %s: %v", key, err)
			continue
		}

		stakes, err := r.LoadFreezedPeerStakes(ctx, pid)
		if err != nil {
			log.Printf("[Storage] Warning: Failed to load freezed stakes for peer %s: %v", pid.ShortString(), err)
			continue
		}

		if len(stakes) > 0 {
			freezedPeerStakes[pid] = stakes
		}
	}

	if err := iter.Err(); err != nil {
		return nil, fmt.Errorf("redis scan error: %v", err)
	}

	log.Printf("[Storage] Restored freezed stakes for %d peers from Redis", len(freezedPeerStakes))
	return freezedPeerStakes, nil
}

// stringSliceToInterface converts a string slice to an interface slice for Redis commands.
func stringSliceToInterface(strs []string) []interface{} {
	result := make([]interface{}, len(strs))
	for i, s := range strs {
		result[i] = s
	}
	return result
}

// SaveEVMNonce records an EVM nonce as used.
func (r *RedisStorage) SaveEVMNonce(ctx context.Context, walletAddr string, nonce int64) error {
	if r == nil || r.client == nil {
		return nil
	}

	key := fmt.Sprintf("evm_nonce:%s|%d", strings.ToLower(walletAddr), nonce)
	if err := r.client.Set(ctx, key, "1", 0).Err(); err != nil {
		return fmt.Errorf("failed to save EVM nonce: %v", err)
	}

	return nil
}

// CheckEVMNonce checks if an EVM nonce has been used.
func (r *RedisStorage) CheckEVMNonce(ctx context.Context, walletAddr string, nonce int64) (bool, error) {
	if r == nil || r.client == nil {
		return false, nil
	}

	key := fmt.Sprintf("evm_nonce:%s|%d", strings.ToLower(walletAddr), nonce)
	exists, err := r.client.Exists(ctx, key).Result()
	if err != nil {
		return false, fmt.Errorf("failed to check EVM nonce: %v", err)
	}

	return exists > 0, nil
}

// SaveWalletBinding saves a wallet-to-peerID binding.
func (r *RedisStorage) SaveWalletBinding(ctx context.Context, walletAddr string, peerID string) error {
	if r == nil || r.client == nil {
		return nil
	}

	key := fmt.Sprintf("wallet_binding:%s", strings.ToLower(walletAddr))
	if err := r.client.Set(ctx, key, peerID, 0).Err(); err != nil {
		return fmt.Errorf("failed to save wallet binding: %v", err)
	}

	return nil
}

// LoadWalletBinding loads a wallet-to-peerID binding.
func (r *RedisStorage) LoadWalletBinding(ctx context.Context, walletAddr string) (string, error) {
	if r == nil || r.client == nil {
		return "", nil
	}

	key := fmt.Sprintf("wallet_binding:%s", strings.ToLower(walletAddr))
	peerID, err := r.client.Get(ctx, key).Result()
	if err != nil {
		if err == redis.Nil {
			return "", nil
		}
		return "", fmt.Errorf("failed to load wallet binding: %v", err)
	}

	return peerID, nil
}

// RestoreAllWalletBindings retrieves all wallet bindings from Redis.
func (r *RedisStorage) RestoreAllWalletBindings(ctx context.Context) (map[string]string, error) {
	if r == nil || r.client == nil {
		return nil, nil
	}

	bindings := make(map[string]string)

	iter := r.client.Scan(ctx, 0, "wallet_binding:*", 0).Iterator()
	for iter.Next(ctx) {
		key := iter.Val()

		walletAddr := strings.TrimPrefix(key, "wallet_binding:")
		peerID, err := r.client.Get(ctx, key).Result()
		if err != nil {
			log.Printf("[Storage] Warning: Failed to load wallet binding for %s: %v", walletAddr, err)
			continue
		}

		bindings[walletAddr] = peerID
	}

	if err := iter.Err(); err != nil {
		return nil, fmt.Errorf("redis scan error: %v", err)
	}

	log.Printf("[Storage] Restored %d wallet bindings from Redis", len(bindings))
	return bindings, nil
}

// RestoreAllEVMNonces retrieves all used EVM nonces from Redis.
func (r *RedisStorage) RestoreAllEVMNonces(ctx context.Context) (map[string]bool, error) {
	if r == nil || r.client == nil {
		return nil, nil
	}

	nonces := make(map[string]bool)

	iter := r.client.Scan(ctx, 0, "evm_nonce:*", 0).Iterator()
	for iter.Next(ctx) {
		key := iter.Val()
		nonceKey := strings.TrimPrefix(key, "evm_nonce:")
		nonces[nonceKey] = true
	}

	if err := iter.Err(); err != nil {
		return nil, fmt.Errorf("redis scan error: %v", err)
	}

	log.Printf("[Storage] Restored %d EVM nonces from Redis", len(nonces))
	return nonces, nil
}

// --- Credits System ---

// GetBalance retrieves the credit balance for a peer.
func (r *RedisStorage) GetBalance(ctx context.Context, peerID string) (*common.CreditAccount, error) {
	if r == nil || r.client == nil {
		return nil, fmt.Errorf("redis not configured")
	}

	key := fmt.Sprintf("credit:balance:%s", peerID)
	data, err := r.client.Get(ctx, key).Bytes()
	if err != nil {
		if err == redis.Nil {
			// Return zero balance if not found
			return &common.CreditAccount{
				PeerID:    peerID,
				Balance:   0,
				UpdatedAt: time.Now().Unix(),
			}, nil
		}
		return nil, fmt.Errorf("failed to get balance: %v", err)
	}

	var account common.CreditAccount
	if err := json.Unmarshal(data, &account); err != nil {
		return nil, fmt.Errorf("failed to unmarshal balance: %v", err)
	}

	return &account, nil
}

// SetBalance sets the credit balance for a peer.
func (r *RedisStorage) SetBalance(ctx context.Context, peerID string, amount float64) error {
	if r == nil || r.client == nil {
		return fmt.Errorf("redis not configured")
	}

	account := common.CreditAccount{
		PeerID:    peerID,
		Balance:   amount,
		UpdatedAt: time.Now().Unix(),
	}

	data, err := json.Marshal(account)
	if err != nil {
		return fmt.Errorf("failed to marshal balance: %v", err)
	}

	key := fmt.Sprintf("credit:balance:%s", peerID)
	if err := r.client.Set(ctx, key, data, 0).Err(); err != nil {
		return fmt.Errorf("failed to save balance: %v", err)
	}

	log.Printf("[Storage] Set balance for %s: %.4f", peerID[:12], amount)
	return nil
}

// AddBalance adds credits to a peer's balance (deposit).
func (r *RedisStorage) AddBalance(ctx context.Context, peerID string, amount float64) (*common.CreditAccount, error) {
	if r == nil || r.client == nil {
		return nil, fmt.Errorf("redis not configured")
	}

	if amount <= 0 {
		return nil, fmt.Errorf("amount must be positive")
	}

	// Get current balance
	account, err := r.GetBalance(ctx, peerID)
	if err != nil {
		return nil, err
	}

	// Update balance
	account.Balance += amount
	account.UpdatedAt = time.Now().Unix()

	// Save
	if err := r.SetBalance(ctx, peerID, account.Balance); err != nil {
		return nil, err
	}

	log.Printf("[Storage] Deposited %.4f credits to %s, new balance: %.4f", amount, peerID[:12], account.Balance)
	return account, nil
}

// ChargeBalance transfers credits from one peer to another.
// Returns the transaction record and error.
func (r *RedisStorage) ChargeBalance(ctx context.Context, fromPeer, toPeer string, amount float64, service string) (*common.CreditTransaction, error) {
	if r == nil || r.client == nil {
		return nil, fmt.Errorf("redis not configured")
	}

	if amount <= 0 {
		return nil, fmt.Errorf("amount must be positive")
	}

	// Get sender balance
	fromAccount, err := r.GetBalance(ctx, fromPeer)
	if err != nil {
		return nil, fmt.Errorf("failed to get sender balance: %v", err)
	}

	if fromAccount.Balance < amount {
		return nil, fmt.Errorf("insufficient balance: have %.4f, need %.4f", fromAccount.Balance, amount)
	}

	// Get receiver balance
	toAccount, err := r.GetBalance(ctx, toPeer)
	if err != nil {
		return nil, fmt.Errorf("failed to get receiver balance: %v", err)
	}

	// Deduct from sender
	fromAccount.Balance -= amount
	if err := r.SetBalance(ctx, fromPeer, fromAccount.Balance); err != nil {
		return nil, fmt.Errorf("failed to update sender balance: %v", err)
	}

	// Add to receiver
	toAccount.Balance += amount
	if err := r.SetBalance(ctx, toPeer, toAccount.Balance); err != nil {
		// Try to rollback sender balance
		_ = r.SetBalance(ctx, fromPeer, fromAccount.Balance+amount)
		return nil, fmt.Errorf("failed to update receiver balance: %v", err)
	}

	// Create transaction record
	txID := fmt.Sprintf("tx-%d-%s", time.Now().UnixNano(), fromPeer[:8])
	tx := &common.CreditTransaction{
		ID:        txID,
		From:      fromPeer,
		To:        toPeer,
		Amount:    amount,
		Service:   service,
		Timestamp: time.Now().Unix(),
	}

	// Save transaction
	if err := r.saveTransaction(ctx, tx); err != nil {
		log.Printf("[Storage] Warning: failed to save transaction record: %v", err)
	}

	log.Printf("[Storage] Charged %.4f credits: %s -> %s for service %s", amount, fromPeer[:12], toPeer[:12], service)
	return tx, nil
}

// saveTransaction saves a transaction record and adds it to both peers' transaction lists.
func (r *RedisStorage) saveTransaction(ctx context.Context, tx *common.CreditTransaction) error {
	data, err := json.Marshal(tx)
	if err != nil {
		return fmt.Errorf("failed to marshal transaction: %v", err)
	}

	// Save transaction record
	txKey := fmt.Sprintf("credit:tx:%s", tx.ID)
	if err := r.client.Set(ctx, txKey, data, 0).Err(); err != nil {
		return fmt.Errorf("failed to save transaction: %v", err)
	}

	// Add to sender's transaction list
	fromListKey := fmt.Sprintf("credit:tx_list:%s", tx.From)
	if err := r.client.LPush(ctx, fromListKey, tx.ID).Err(); err != nil {
		log.Printf("[Storage] Warning: failed to add tx to sender list: %v", err)
	}

	// Add to receiver's transaction list
	toListKey := fmt.Sprintf("credit:tx_list:%s", tx.To)
	if err := r.client.LPush(ctx, toListKey, tx.ID).Err(); err != nil {
		log.Printf("[Storage] Warning: failed to add tx to receiver list: %v", err)
	}

	return nil
}

// GetTransactions retrieves the transaction history for a peer.
func (r *RedisStorage) GetTransactions(ctx context.Context, peerID string, limit int) ([]common.CreditTransaction, error) {
	if r == nil || r.client == nil {
		return nil, fmt.Errorf("redis not configured")
	}

	if limit <= 0 {
		limit = 50
	}

	listKey := fmt.Sprintf("credit:tx_list:%s", peerID)
	txIDs, err := r.client.LRange(ctx, listKey, 0, int64(limit-1)).Result()
	if err != nil {
		if err == redis.Nil {
			return []common.CreditTransaction{}, nil
		}
		return nil, fmt.Errorf("failed to get transaction list: %v", err)
	}

	transactions := make([]common.CreditTransaction, 0, len(txIDs))
	for _, txID := range txIDs {
		txKey := fmt.Sprintf("credit:tx:%s", txID)
		data, err := r.client.Get(ctx, txKey).Bytes()
		if err != nil {
			log.Printf("[Storage] Warning: failed to get transaction %s: %v", txID, err)
			continue
		}

		var tx common.CreditTransaction
		if err := json.Unmarshal(data, &tx); err != nil {
			log.Printf("[Storage] Warning: failed to unmarshal transaction %s: %v", txID, err)
			continue
		}

		transactions = append(transactions, tx)
	}

	return transactions, nil
}

// --- Settlement (Withdrawals) ---

// CreateWithdrawal creates a new withdrawal request
func (r *RedisStorage) CreateWithdrawal(
	ctx context.Context,
	peerID string,
	amount float64,
	walletAddress string,
	chain string,
	amountWei string,
) (*common.WithdrawalRequest, error) {
	now := time.Now().Unix()
	shortID := peerID
	if len(peerID) > 10 {
		shortID = peerID[:10]
	}

	wdr := &common.WithdrawalRequest{
		ID:            fmt.Sprintf("wdr-%d-%s", now, shortID),
		PeerID:        peerID,
		Amount:        amount,
		AmountWei:     amountWei,
		WalletAddress: walletAddress,
		Chain:         chain,
		Status:        "pending",
		TxHash:        "",
		CreatedAt:     now,
		UpdatedAt:     now,
		ErrorMessage:  "",
	}

	// Store withdrawal request
	wdrKey := fmt.Sprintf("credit:withdrawal:%s", wdr.ID)
	data, err := json.Marshal(wdr)
	if err != nil {
		return nil, err
	}

	if err := r.client.Set(ctx, wdrKey, data, 0).Err(); err != nil {
		return nil, err
	}

	// Add to peer's withdrawal list
	listKey := fmt.Sprintf("credit:withdrawal_list:%s", peerID)
	if err := r.client.RPush(ctx, listKey, wdr.ID).Err(); err != nil {
		return nil, err
	}

	// Add to pending set
	if err := r.client.SAdd(ctx, "credit:withdrawal_pending", wdr.ID).Err(); err != nil {
		return nil, err
	}

	return wdr, nil
}

// GetWithdrawal retrieves a withdrawal request by ID
func (r *RedisStorage) GetWithdrawal(
	ctx context.Context,
	requestID string,
) (*common.WithdrawalRequest, error) {
	wdrKey := fmt.Sprintf("credit:withdrawal:%s", requestID)
	data, err := r.client.Get(ctx, wdrKey).Bytes()
	if err == redis.Nil {
		return nil, fmt.Errorf("withdrawal not found")
	}
	if err != nil {
		return nil, err
	}

	var wdr common.WithdrawalRequest
	if err := json.Unmarshal(data, &wdr); err != nil {
		return nil, err
	}

	return &wdr, nil
}

// UpdateWithdrawalStatus updates the status of a withdrawal request
func (r *RedisStorage) UpdateWithdrawalStatus(
	ctx context.Context,
	requestID string,
	status string,
	txHash string,
	errorMsg string,
) error {
	wdr, err := r.GetWithdrawal(ctx, requestID)
	if err != nil {
		return err
	}

	wdr.Status = status
	wdr.TxHash = txHash
	wdr.ErrorMessage = errorMsg
	wdr.UpdatedAt = time.Now().Unix()

	// Update withdrawal request
	wdrKey := fmt.Sprintf("credit:withdrawal:%s", requestID)
	data, err := json.Marshal(wdr)
	if err != nil {
		return err
	}

	if err := r.client.Set(ctx, wdrKey, data, 0).Err(); err != nil {
		return err
	}

	// Remove from pending set if completed or failed
	if status == "completed" || status == "failed" {
		if err := r.client.SRem(ctx, "credit:withdrawal_pending", requestID).Err(); err != nil {
			log.Printf("[Storage] Warning: failed to remove from pending set: %v", err)
		}
	}

	return nil
}

// GetWithdrawals retrieves withdrawal history for a peer
func (r *RedisStorage) GetWithdrawals(
	ctx context.Context,
	peerID string,
	limit int,
) ([]common.WithdrawalRequest, error) {
	listKey := fmt.Sprintf("credit:withdrawal_list:%s", peerID)

	// Get withdrawal IDs (most recent first)
	var wdrIDs []string
	var err error
	if limit > 0 {
		wdrIDs, err = r.client.LRange(ctx, listKey, -int64(limit), -1).Result()
	} else {
		wdrIDs, err = r.client.LRange(ctx, listKey, 0, -1).Result()
	}

	if err == redis.Nil {
		return []common.WithdrawalRequest{}, nil
	}
	if err != nil {
		return nil, err
	}

	// Reverse to show most recent first
	for i, j := 0, len(wdrIDs)-1; i < j; i, j = i+1, j-1 {
		wdrIDs[i], wdrIDs[j] = wdrIDs[j], wdrIDs[i]
	}

	withdrawals := make([]common.WithdrawalRequest, 0, len(wdrIDs))
	for _, wdrID := range wdrIDs {
		wdrKey := fmt.Sprintf("credit:withdrawal:%s", wdrID)
		data, err := r.client.Get(ctx, wdrKey).Bytes()
		if err != nil {
			log.Printf("[Storage] Warning: failed to get withdrawal %s: %v", wdrID, err)
			continue
		}

		var wdr common.WithdrawalRequest
		if err := json.Unmarshal(data, &wdr); err != nil {
			log.Printf("[Storage] Warning: failed to unmarshal withdrawal %s: %v", wdrID, err)
			continue
		}

		withdrawals = append(withdrawals, wdr)
	}

	return withdrawals, nil
}

// GetPendingWithdrawals retrieves all pending withdrawal requests
func (r *RedisStorage) GetPendingWithdrawals(ctx context.Context) ([]common.WithdrawalRequest, error) {
	// Get all pending withdrawal IDs
	wdrIDs, err := r.client.SMembers(ctx, "credit:withdrawal_pending").Result()
	if err == redis.Nil {
		return []common.WithdrawalRequest{}, nil
	}
	if err != nil {
		return nil, err
	}

	withdrawals := make([]common.WithdrawalRequest, 0, len(wdrIDs))
	for _, wdrID := range wdrIDs {
		wdr, err := r.GetWithdrawal(ctx, wdrID)
		if err != nil {
			log.Printf("[Storage] Warning: failed to get pending withdrawal %s: %v", wdrID, err)
			continue
		}

		withdrawals = append(withdrawals, *wdr)
	}

	return withdrawals, nil
}

// --- Blockchain Deposits ---

// RecordDeposit stores a deposit record and credits the peer
func (r *RedisStorage) RecordDeposit(
	ctx context.Context,
	txHash string,
	fromAddress string,
	peerID string,
	amountWei string,
	amountCredits float64,
	blockNumber uint64,
) (*common.DepositRecord, error) {
	now := time.Now().Unix()
	shortTx := txHash
	if len(txHash) > 12 {
		shortTx = txHash[:12]
	}

	deposit := &common.DepositRecord{
		ID:            fmt.Sprintf("dep-%d-%s", now, shortTx),
		TxHash:        txHash,
		FromAddress:   fromAddress,
		PeerID:        peerID,
		AmountWei:     amountWei,
		AmountCredits: amountCredits,
		BlockNumber:   blockNumber,
		Timestamp:     now,
	}

	// Store deposit record
	depKey := fmt.Sprintf("credit:deposit:%s", deposit.ID)
	data, err := json.Marshal(deposit)
	if err != nil {
		return nil, err
	}

	if err := r.client.Set(ctx, depKey, data, 0).Err(); err != nil {
		return nil, err
	}

	// Add to peer's deposit list
	listKey := fmt.Sprintf("credit:deposit_list:%s", peerID)
	if err := r.client.RPush(ctx, listKey, deposit.ID).Err(); err != nil {
		return nil, err
	}

	// Mark tx as processed (prevent double processing)
	processedKey := fmt.Sprintf("credit:deposit_tx:%s", txHash)
	if err := r.client.Set(ctx, processedKey, deposit.ID, 0).Err(); err != nil {
		return nil, err
	}

	// Add credits to peer balance
	_, err = r.AddBalance(ctx, peerID, amountCredits)
	if err != nil {
		return nil, fmt.Errorf("failed to credit balance: %w", err)
	}

	return deposit, nil
}

// IsDepositProcessed checks if a deposit tx has already been processed
func (r *RedisStorage) IsDepositProcessed(ctx context.Context, txHash string) (bool, error) {
	processedKey := fmt.Sprintf("credit:deposit_tx:%s", txHash)
	exists, err := r.client.Exists(ctx, processedKey).Result()
	if err != nil {
		return false, err
	}
	return exists > 0, nil
}

// GetDeposit retrieves a deposit record by ID
func (r *RedisStorage) GetDeposit(ctx context.Context, depositID string) (*common.DepositRecord, error) {
	depKey := fmt.Sprintf("credit:deposit:%s", depositID)
	data, err := r.client.Get(ctx, depKey).Bytes()
	if err == redis.Nil {
		return nil, fmt.Errorf("deposit not found")
	}
	if err != nil {
		return nil, err
	}

	var deposit common.DepositRecord
	if err := json.Unmarshal(data, &deposit); err != nil {
		return nil, err
	}

	return &deposit, nil
}

// GetDeposits retrieves deposit history for a peer
func (r *RedisStorage) GetDeposits(ctx context.Context, peerID string, limit int) ([]common.DepositRecord, error) {
	listKey := fmt.Sprintf("credit:deposit_list:%s", peerID)

	var depIDs []string
	var err error
	if limit > 0 {
		depIDs, err = r.client.LRange(ctx, listKey, -int64(limit), -1).Result()
	} else {
		depIDs, err = r.client.LRange(ctx, listKey, 0, -1).Result()
	}

	if err == redis.Nil {
		return []common.DepositRecord{}, nil
	}
	if err != nil {
		return nil, err
	}

	// Reverse to show most recent first
	for i, j := 0, len(depIDs)-1; i < j; i, j = i+1, j-1 {
		depIDs[i], depIDs[j] = depIDs[j], depIDs[i]
	}

	deposits := make([]common.DepositRecord, 0, len(depIDs))
	for _, depID := range depIDs {
		dep, err := r.GetDeposit(ctx, depID)
		if err != nil {
			log.Printf("[Storage] Warning: failed to get deposit %s: %v", depID, err)
			continue
		}
		deposits = append(deposits, *dep)
	}

	return deposits, nil
}

// GetLastProcessedBlock returns the last block number that was processed for deposits
func (r *RedisStorage) GetLastProcessedBlock(ctx context.Context) (uint64, error) {
	data, err := r.client.Get(ctx, "credit:deposit_last_block").Result()
	if err == redis.Nil {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}

	var block uint64
	_, err = fmt.Sscanf(data, "%d", &block)
	return block, err
}

// SetLastProcessedBlock stores the last block number that was processed
func (r *RedisStorage) SetLastProcessedBlock(ctx context.Context, blockNumber uint64) error {
	return r.client.Set(ctx, "credit:deposit_last_block", fmt.Sprintf("%d", blockNumber), 0).Err()
}

// Close closes the Redis connection.
func (r *RedisStorage) Close() error {
	if r == nil || r.client == nil {
		return nil
	}
	return r.client.Close()
}
