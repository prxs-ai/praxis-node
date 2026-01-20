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

// Close closes the Redis connection.
func (r *RedisStorage) Close() error {
	if r == nil || r.client == nil {
		return nil
	}
	return r.client.Close()
}
