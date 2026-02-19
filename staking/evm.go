package staking

import (
	"context"
	"fmt"
	"log"
	"math/big"
	"strings"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/accounts"
	"github.com/ethereum/go-ethereum/accounts/abi"
	ethcommon "github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/ethclient"

	"prxs/storage"
)

const stakingABI = `[{"inputs":[{"internalType":"address","name":"_staker","type":"address"}],"name":"isValidStaker","outputs":[{"internalType":"bool","name":"hasStake","type":"bool"}],"stateMutability":"view","type":"function"}]`

type contractCaller interface {
	CallContract(ctx context.Context, msg ethereum.CallMsg, blockNumber *big.Int) ([]byte, error)
	ChainID(ctx context.Context) (*big.Int, error)
	Close()
}

type EVMStakingVerifier struct {
	client          contractCaller
	contractAddress ethcommon.Address
	contractABI     abi.ABI
	chainID         int64

	cacheMu    sync.RWMutex
	stakeCache map[ethcommon.Address]*stakeCacheEntry
	cacheTTL   time.Duration

	walletBindingsMu sync.RWMutex
	walletBindings   map[ethcommon.Address]string

	usedNoncesMu sync.RWMutex
	usedNonces   map[string]bool

	storage *storage.RedisStorage
}

type stakeCacheEntry struct {
	isValid   bool
	expiresAt time.Time
}

func NewEVMStakingVerifier(rpcURL string, contractAddr string, chainID int64, cacheTTL time.Duration, redisStorage *storage.RedisStorage) (*EVMStakingVerifier, error) {
	client, err := ethclient.Dial(rpcURL)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to EVM RPC: %w", err)
	}

	return newEVMStakingVerifier(client, contractAddr, chainID, cacheTTL, redisStorage)
}

func newEVMStakingVerifier(client contractCaller, contractAddr string, chainID int64, cacheTTL time.Duration, redisStorage *storage.RedisStorage) (*EVMStakingVerifier, error) {
	if client == nil {
		return nil, fmt.Errorf("client is nil")
	}
	if chainID <= 0 {
		return nil, fmt.Errorf("invalid chain ID: %d", chainID)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	rpcChainID, err := client.ChainID(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch chain ID from RPC: %w", err)
	}
	if rpcChainID == nil {
		return nil, fmt.Errorf("RPC returned nil chain ID")
	}
	if rpcChainID.Int64() != chainID {
		return nil, fmt.Errorf("RPC chain ID mismatch: expected %d, got %d", chainID, rpcChainID.Int64())
	}

	parsedABI, err := abi.JSON(strings.NewReader(stakingABI))
	if err != nil {
		return nil, fmt.Errorf("failed to parse staking ABI: %w", err)
	}

	v := &EVMStakingVerifier{
		client:          client,
		contractAddress: ethcommon.HexToAddress(contractAddr),
		contractABI:     parsedABI,
		chainID:         chainID,
		stakeCache:      make(map[ethcommon.Address]*stakeCacheEntry),
		cacheTTL:        cacheTTL,
		walletBindings:  make(map[ethcommon.Address]string),
		usedNonces:      make(map[string]bool),
		storage:         redisStorage,
	}

	if redisStorage != nil {
		ctx := context.Background()

		if nonces, err := redisStorage.RestoreAllEVMNonces(ctx); err == nil && nonces != nil {
			v.usedNonces = nonces
			log.Printf("[EVM] Restored %d nonces from Redis", len(nonces))
		}

		if bindings, err := redisStorage.RestoreAllWalletBindings(ctx); err == nil && bindings != nil {
			for wallet, peerID := range bindings {
				v.walletBindings[ethcommon.HexToAddress(wallet)] = peerID
			}
			log.Printf("[EVM] Restored %d wallet bindings from Redis", len(bindings))
		}
	}

	return v, nil
}

func (v *EVMStakingVerifier) IsValidStaker(ctx context.Context, walletAddr ethcommon.Address) (bool, error) {
	if v.cacheTTL > 0 {
		v.cacheMu.RLock()
		if entry, ok := v.stakeCache[walletAddr]; ok && time.Now().Before(entry.expiresAt) {
			v.cacheMu.RUnlock()
			return entry.isValid, nil
		}
		v.cacheMu.RUnlock()
	}

	data, err := v.contractABI.Pack("isValidStaker", walletAddr)
	if err != nil {
		return false, fmt.Errorf("failed to pack call data: %w", err)
	}

	result, err := v.client.CallContract(ctx, ethereum.CallMsg{
		To:   &v.contractAddress,
		Data: data,
	}, nil)
	if err != nil {
		return false, fmt.Errorf("contract call failed: %w", err)
	}

	var isValid bool
	if err := v.contractABI.UnpackIntoInterface(&isValid, "isValidStaker", result); err != nil {
		return false, fmt.Errorf("failed to unpack result: %w", err)
	}

	if v.cacheTTL > 0 {
		v.cacheMu.Lock()
		v.stakeCache[walletAddr] = &stakeCacheEntry{
			isValid:   isValid,
			expiresAt: time.Now().Add(v.cacheTTL),
		}
		v.cacheMu.Unlock()
	}

	return isValid, nil
}

func (v *EVMStakingVerifier) VerifyWalletSignature(walletAddr string, signature []byte, message string) (bool, error) {
	if len(signature) != 65 {
		return false, fmt.Errorf("invalid signature length: expected 65, got %d", len(signature))
	}

	messageHash := accounts.TextHash([]byte(message))

	sig := make([]byte, 65)
	copy(sig, signature)

	if sig[64] >= 27 {
		sig[64] -= 27
	}

	pubKey, err := crypto.SigToPub(messageHash, sig)
	if err != nil {
		return false, fmt.Errorf("failed to recover public key: %w", err)
	}

	recoveredAddr := crypto.PubkeyToAddress(*pubKey)
	expectedAddr := ethcommon.HexToAddress(walletAddr)

	return recoveredAddr == expectedAddr, nil
}

func (v *EVMStakingVerifier) CheckAndRecordNonce(walletAddr string, nonce int64) (bool, error) {
	key := fmt.Sprintf("%s|%d", strings.ToLower(walletAddr), nonce)

	v.usedNoncesMu.Lock()
	defer v.usedNoncesMu.Unlock()

	if v.usedNonces[key] {
		return false, nil
	}

	v.usedNonces[key] = true

	if v.storage != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := v.storage.SaveEVMNonce(ctx, walletAddr, nonce); err != nil {
			log.Printf("[EVM] Warning: Failed to persist nonce to Redis: %v", err)
		}
	}

	return true, nil
}

func (v *EVMStakingVerifier) IsNonceUsedAsHeartbeat(walletAddr string, nonce int64, expectedPeerID string) bool {
	v.walletBindingsMu.RLock()
	defer v.walletBindingsMu.RUnlock()

	addr := ethcommon.HexToAddress(walletAddr)
	if boundPeerID, ok := v.walletBindings[addr]; ok {
		return boundPeerID == expectedPeerID
	}
	return false
}

func (v *EVMStakingVerifier) RecordWalletBinding(walletAddr string, peerID string) error {
	v.walletBindingsMu.Lock()
	defer v.walletBindingsMu.Unlock()

	addr := ethcommon.HexToAddress(walletAddr)
	if existingPeerID, ok := v.walletBindings[addr]; ok {
		if existingPeerID == peerID {
			return nil
		}
		return fmt.Errorf("wallet %s already bound to peer %s", addr.Hex(), existingPeerID)
	}

	for existingWallet, existingPeerID := range v.walletBindings {
		if existingPeerID == peerID {
			return fmt.Errorf("peer %s already bound to wallet %s", peerID, existingWallet.Hex())
		}
	}

	v.walletBindings[addr] = peerID

	if v.storage != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := v.storage.SaveWalletBinding(ctx, walletAddr, peerID); err != nil {
			log.Printf("[EVM] Warning: Failed to persist wallet binding to Redis: %v", err)
		}
	}

	return nil
}

func (v *EVMStakingVerifier) GetBoundPeerID(walletAddr string) (string, bool) {
	v.walletBindingsMu.RLock()
	defer v.walletBindingsMu.RUnlock()

	addr := ethcommon.HexToAddress(walletAddr)
	peerID, ok := v.walletBindings[addr]
	return peerID, ok
}

func (v *EVMStakingVerifier) GetBoundWallet(peerID string) (string, bool) {
	v.walletBindingsMu.RLock()
	defer v.walletBindingsMu.RUnlock()

	for walletAddr, boundPeerID := range v.walletBindings {
		if boundPeerID == peerID {
			return walletAddr.Hex(), true
		}
	}
	return "", false
}

func (v *EVMStakingVerifier) InvalidateCache(walletAddr ethcommon.Address) {
	v.cacheMu.Lock()
	defer v.cacheMu.Unlock()
	delete(v.stakeCache, walletAddr)
}

func (v *EVMStakingVerifier) Close() {
	if v.client != nil {
		v.client.Close()
	}
}

func (v *EVMStakingVerifier) GetChainID() *big.Int {
	return big.NewInt(v.chainID)
}
