package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"math/big"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gin-contrib/cors"
	"github.com/gin-gonic/gin"
	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/p2p/discovery/routing"
	dutil "github.com/libp2p/go-libp2p/p2p/discovery/util"
	ma "github.com/multiformats/go-multiaddr"

	"prxs/common"
	"prxs/staking"
	"prxs/storage"

	ethcommon "github.com/ethereum/go-ethereum/common"
)

// RegistrationRecord tracks an active provider session.
type RegistrationRecord struct {
	LastSeen    time.Time
	ServiceCard common.ServiceCard
	StakeProof  *common.StakeProof
	AddrInfo    peer.AddrInfo
}

// freezedStake represents a stake that is temporarily frozen during unregistration.
type freezedStake struct {
	ID        string
	PeerID    peer.ID
	CreatedAt int64 // Unix timestamp
}

// UNFREEZE_DELAY is the duration (in seconds) a stake must remain frozen before unfreezing.
const UNFREEZE_DELAY = 7 * 86400 // 7 days

type evmStakingVerifier interface {
	GetChainID() *big.Int
	VerifyWalletSignature(walletAddr string, signature []byte, message string) (bool, error)
	GetBoundPeerID(walletAddr string) (string, bool)
	GetBoundWallet(peerID string) (string, bool)
	IsValidStaker(ctx context.Context, walletAddr ethcommon.Address) (bool, error)
	CheckAndRecordNonce(walletAddr string, nonce int64) (bool, error)
	IsNonceUsedAsHeartbeat(walletAddr string, nonce int64, expectedPeerID string) bool
	RecordWalletBinding(walletAddr string, peerID string) error
}

type RegistryNode struct {
	Host host.Host

	Registrations map[peer.ID]*RegistrationRecord
	ServiceIndex  map[string][]peer.ID

	mu sync.Mutex

	minStake          float64
	peerStakes        map[peer.ID][]string
	freezedPeerStakes map[peer.ID][]freezedStake
	freezedStakes     []freezedStake
	stakeMu           sync.Mutex

	qdrant       *QdrantClient
	storage      *storage.RedisStorage
	embeddingDim int
	embedder     *EmbeddingClient

	stakingMode        string
	evmVerifier        evmStakingVerifier
	evmContractAddress ethcommon.Address

	// Settlement
	ethClient             *EthereumClient
	settlementEnabled     bool
	settlementRate        float64
	settlementMinAmount   float64
	settlementConfirms    int
	settlementChain       string

	// Deposit
	depositEnabled  bool
	depositContract string
}

func main() {
	// Structured timestamps for all logs
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)

	port := flag.Int("port", 4001, "port")
	apiPort := flag.Int("api-port", 8080, "REST API port (default: 8080, avoid restricted ports like 6000)")
	bootstrap := flag.String("bootstrap", "", "bootstrap multiaddr")
	keyFile := flag.String("key", "", "path to key file (e.g. registry.key)")
	devMode := flag.Bool("dev", true, "Enable LAN/Dev mode")
	announceIP := flag.String("announce", "", "Public IP to announce (e.g., 164.90.233.252) - for Docker/NAT environments")
	minStake := flag.Float64("min-stake", 10.0, "minimum stake required to register (mock mode only)")
	qdrantEnabled := flag.Bool("qdrant-enabled", false, "enable Qdrant semantic index")
	qdrantURL := flag.String("qdrant-url", "http://localhost:6333", "Qdrant base URL")
	qdrantCollection := flag.String("qdrant-collection", "prxs_services", "Qdrant collection name")
	redisAddr := flag.String("redis", "", "Redis address (e.g., localhost:6379) - if set, registrations are stored in both memory and Redis")
	embeddingDim := flag.Int("embedding-dim", 1536, "Embedding dimension (e.g., 1536 for text-embedding-3-small)")
	embeddingModel := flag.String("embedding-model", "text-embedding-3-small", "Embedding model name (used for query embeddings)")
	embeddingBaseURL := flag.String("embedding-base-url", "https://api.openai.com/v1", "Embedding API base URL")
	embeddingAPIKey := flag.String("embedding-api-key", "", "Embedding API key (default: OPENAI_API_KEY env)")
	stakingMode := flag.String("staking-mode", "mock", "staking verification mode: mock, evm, or hybrid")
	evmRPCURL := flag.String("evm-rpc-url", "", "EVM RPC URL for on-chain stake verification")
	evmChainID := flag.Int64("evm-chain-id", 1, "EVM chain ID (default: 1 Mainnet, use 11155111 for Sepolia)")
	stakingContract := flag.String("staking-contract", "", "PRXSStaking contract address for on-chain verification")
	stakeCacheTTL := flag.Duration("stake-cache-ttl", 60*time.Second, "TTL for stake verification cache")

	// Settlement configuration
	settlementEnabled := flag.Bool("settlement-enabled", false, "enable settlement (credit withdrawal to blockchain)")
	settlementRPCURL := flag.String("settlement-rpc-url", "", "Settlement RPC URL (e.g., https://sepolia.infura.io/v3/YOUR_KEY)")
	settlementPrivateKey := flag.String("settlement-private-key", "", "Settlement wallet private key (hex, without 0x)")
	settlementRate := flag.Float64("settlement-rate", 0.0001, "1 credit = X ETH (default: 0.0001)")
	settlementMinAmount := flag.Float64("settlement-min-amount", 10.0, "minimum credits for withdrawal")
	settlementConfirms := flag.Int("settlement-confirms", 3, "number of block confirmations to wait")
	settlementChain := flag.String("settlement-chain", "sepolia", "blockchain for settlement")

	// Deposit configuration (blockchain deposit -> credits)
	depositEnabled := flag.Bool("deposit-enabled", false, "enable blockchain deposits (ETH -> credits)")
	depositContract := flag.String("deposit-contract", "", "PRXSDeposit contract address")

	flag.Parse()

	// Load Key if specified, otherwise generate ephemeral
	var privKey crypto.PrivKey
	var err error

	if *keyFile != "" {
		privKey, err = common.LoadOrGenerateKey(*keyFile)
		if err != nil {
			log.Fatalf("Failed to load key: %v", err)
		}
	} else {
		// Ephemeral key
		privKey, _, _ = crypto.GenerateKeyPairWithReader(crypto.RSA, 2048, rand.Reader)
	}

	// Get LLM configuration from environment (LLM_API_KEY > OPENAI_API_KEY)
	key := *embeddingAPIKey
	if key == "" {
		key = os.Getenv("LLM_API_KEY")
		if key == "" {
			key = os.Getenv("OPENAI_API_KEY")
		}
	}

	// Auto-detect base URL for OpenRouter if key starts with sk-or-
	baseURL := *embeddingBaseURL
	if *embeddingBaseURL == "https://api.openai.com/v1" && strings.HasPrefix(key, "sk-or-") {
		baseURL = "https://openrouter.ai/api/v1"
	}

	startRegistry(*port, *apiPort, *bootstrap, *devMode, *minStake, privKey, *announceIP, *qdrantURL, *qdrantCollection, *qdrantEnabled, *redisAddr, *embeddingDim, *embeddingModel, baseURL, key, *stakingMode, *evmRPCURL, *evmChainID, *stakingContract, *stakeCacheTTL, *settlementEnabled, *settlementRPCURL, *settlementPrivateKey, *settlementRate, *settlementMinAmount, *settlementConfirms, *settlementChain, *depositEnabled, *depositContract)
}

func startRegistry(port int, apiPort int, bootstrapAddr string, devMode bool, minStake float64, privKey crypto.PrivKey, announceIP string, qdrantURL, qdrantCollection string, qdrantEnabled bool, redisAddr string, embeddingDim int, embeddingModel, embeddingBaseURL, embeddingAPIKey string, stakingMode string, evmRPCURL string, evmChainID int64, stakingContract string, stakeCacheTTL time.Duration, settlementEnabled bool, settlementRPCURL string, settlementPrivateKey string, settlementRate float64, settlementMinAmount float64, settlementConfirms int, settlementChain string, depositEnabled bool, depositContract string) {

	ctx := context.Background()

	var h host.Host
	var err error

	if announceIP != "" {
		log.Printf("[Reg] Using announce IP: %s", announceIP)
		h, err = libp2p.New(common.CommonLibp2pOptionsWithAnnounce(port, privKey, announceIP)...)
	} else {
		h, err = libp2p.New(common.CommonLibp2pOptions(port, privKey)...)
	}
	if err != nil {
		log.Fatal(err)
	}

	if embeddingDim <= 0 {
		log.Fatalf("embedding-dim must be > 0 (got %d)", embeddingDim)
	}

	var qdrant *QdrantClient
	if qdrantEnabled && qdrantURL != "" && qdrantCollection != "" {
		qdrant = NewQdrantClient(qdrantURL, qdrantCollection)
		fmt.Printf("[Reg] Qdrant enabled: url=%s collection=%s dim=%d\n", qdrantURL, qdrantCollection, embeddingDim)
	}

	var embedder *EmbeddingClient
	if qdrantEnabled {
		if embeddingAPIKey == "" {
			log.Fatalf("embedding API key required for semantic search (set -embedding-api-key or OPENAI_API_KEY)")
		}
		embedder = NewEmbeddingClient(embeddingAPIKey, embeddingModel, embeddingBaseURL, embeddingDim)
	}

	redisStorage, err := storage.NewRedisStorage(redisAddr, 120*time.Second)
	if err != nil {
		log.Printf("[Reg] Warning: Failed to initialize Redis storage: %v", err)
		log.Printf("[Reg] Continuing without Redis persistence (in-memory only)")
		redisStorage = nil
	}

	var evmVerifier evmStakingVerifier
	var evmContractAddress ethcommon.Address
	if stakingMode == "evm" || stakingMode == "hybrid" {
		if evmRPCURL == "" || stakingContract == "" {
			log.Fatalf("[Reg] EVM staking mode requires -evm-rpc-url and -staking-contract flags")
		}
		if !ethcommon.IsHexAddress(stakingContract) {
			log.Fatalf("[Reg] Invalid staking contract address: %s", stakingContract)
		}
		evmContractAddress = ethcommon.HexToAddress(stakingContract)
		evmVerifier, err = staking.NewEVMStakingVerifier(evmRPCURL, stakingContract, evmChainID, stakeCacheTTL, redisStorage)
		if err != nil {
			log.Fatalf("[Reg] Failed to initialize EVM staking verifier: %v", err)
		}
		log.Printf("[Reg] EVM staking enabled: chain=%d contract=%s mode=%s", evmChainID, evmContractAddress.Hex(), stakingMode)
	}

	// Initialize Ethereum client for settlement if enabled
	var ethClient *EthereumClient
	if settlementEnabled {
		if settlementRPCURL == "" || settlementPrivateKey == "" {
			log.Fatalf("[Settlement] Error: settlement-rpc-url and settlement-private-key are required when settlement is enabled")
		}

		ethClient, err = NewEthereumClient(settlementRPCURL, settlementPrivateKey)
		if err != nil {
			log.Fatalf("[Settlement] Failed to initialize Ethereum client: %v", err)
		}

		log.Printf("[Settlement] Ethereum client initialized: chain=%s wallet=%s", settlementChain, ethClient.GetAddress())

		// Check balance
		balance, err := ethClient.GetBalance(ctx)
		if err != nil {
			log.Printf("[Settlement] Warning: Failed to get wallet balance: %v", err)
		} else {
			ethBalance := new(big.Float).Quo(new(big.Float).SetInt(balance), big.NewFloat(1e18))
			log.Printf("[Settlement] Wallet balance: %s ETH", ethBalance.Text('f', 6))
		}
	}

	reg := &RegistryNode{
		Host:               h,
		Registrations:      make(map[peer.ID]*RegistrationRecord),
		ServiceIndex:       make(map[string][]peer.ID),
		minStake:           minStake,
		peerStakes:         make(map[peer.ID][]string),
		freezedPeerStakes:  make(map[peer.ID][]freezedStake),
		freezedStakes:      make([]freezedStake, 0),
		qdrant:             qdrant,
		storage:            redisStorage,
		embeddingDim:       embeddingDim,
		embedder:           embedder,
		stakingMode:        stakingMode,
		evmVerifier:        evmVerifier,
		evmContractAddress: evmContractAddress,
		// Settlement
		ethClient:           ethClient,
		settlementEnabled:   settlementEnabled,
		settlementRate:      settlementRate,
		settlementMinAmount: settlementMinAmount,
		settlementConfirms:  settlementConfirms,
		settlementChain:     settlementChain,
		// Deposit
		depositEnabled:  depositEnabled,
		depositContract: depositContract,
	}

	// Restore state from Redis if enabled
	if redisStorage != nil {
		if err := reg.restoreStateFromRedis(ctx); err != nil {
			log.Printf("[Reg] Warning: Failed to restore state from Redis: %v", err)
		}

		// Rebuild Qdrant index from restored registrations
		if reg.qdrant != nil {
			if err := reg.reindexQdrant(ctx); err != nil {
				log.Printf("[Reg] Warning: Failed to reindex Qdrant: %v", err)
			}
		}
	}

	// Set Stream Handler for Registry Interactions
	h.SetStreamHandler(common.RegistryProtocolID, reg.handleStream)

	// Setup DHT to advertise "I AM THE REGISTRY"
	peers := []string{}
	if bootstrapAddr != "" {
		peers = append(peers, bootstrapAddr)
	}

	kademliaDHT, err := common.SetupDHT(ctx, h, peers, devMode)
	if err != nil {
		log.Fatal(err)
	}

	// Advertise existence so Providers/Clients can find us
	fmt.Println("REGISTRY ONLINE.")
	common.PrintMyAddresses(h)

	go func() {
		rd := routing.NewRoutingDiscovery(kademliaDHT)
		for {
			// We advertise on the INFRASTRUCTURE key, not a service key
			dutil.Advertise(ctx, rd, common.RegistryRendezvous)
			log.Println("[Reg] Advertised presence on DHT")
			time.Sleep(1 * time.Minute)
		}
	}()

	// Garbage Collection Loop (Remove dead providers)
	go reg.gcLoop()

	// Stake Unfreezer Loop (Unfreeze stakes after delay)
	go reg.stakeUnfreezer()

	// Settlement Withdrawal Processor (if enabled)
	if settlementEnabled {
		go reg.withdrawalProcessor()
	}

	// Deposit Watcher (if enabled)
	if depositEnabled && ethClient != nil && depositContract != "" {
		log.Printf("[Deposit] Watcher enabled: contract=%s", depositContract)
		go reg.depositWatcher()
	}

	// Start REST API server
	go func() {
		router := reg.setupRESTAPI()
		apiPortStr := fmt.Sprintf(":%d", apiPort)
		fmt.Printf("[Reg] Starting REST API server on %s\n", apiPortStr)
		if err := router.Run(apiPortStr); err != nil {
			log.Printf("[Reg] REST API server error: %v\n", err)
		}
	}()

	select {}
}

// gcLoop removes providers who haven't sent a heartbeat in 90 seconds.
func (r *RegistryNode) gcLoop() {
	ticker := time.NewTicker(10 * time.Second)
	for range ticker.C {
		r.mu.Lock()
		now := time.Now()
		for pid, record := range r.Registrations {
			if now.Sub(record.LastSeen) > 90*time.Second {
				log.Printf("[Reg] 💀 Pruning dead provider: %s (last seen %s)\n", pid.ShortString(), record.LastSeen.Format(time.RFC3339))
				delete(r.Registrations, pid)
				r.removeFromIndex(pid, record.ServiceCard.Name)

				// Also delete from Redis if enabled
				if err := r.storage.DeleteRegistration(context.Background(), pid, record.ServiceCard.Name); err != nil {
					log.Printf("[Reg] Warning: Failed to delete registration from Redis: %v", err)
				}
			}
		}
		r.mu.Unlock()
	}
}

// stakeUnfreezer periodically checks for frozen stakes that are eligible for unfreezing.
func (r *RegistryNode) stakeUnfreezer() {
	ticker := time.NewTicker(5 * time.Minute)
	for range ticker.C {
		r.stakeMu.Lock()
		now := time.Now().Unix()

		// Find stakes that can be unfrozen
		newFreezedStakes := make([]freezedStake, 0)
		unfrozenCount := 0
		modifiedPeers := make(map[peer.ID]bool) // Track which peers had changes

		for _, frozen := range r.freezedStakes {
			if frozen.CreatedAt < now-UNFREEZE_DELAY {
				// This stake is eligible for unfreezing
				unfrozenCount++
				log.Printf("[Reg] ❄️  Unfreezing stake %s for peer %s (frozen at %s)\n",
					frozen.ID, frozen.PeerID.ShortString(), time.Unix(frozen.CreatedAt, 0).Format(time.RFC3339))

				// Remove from freezedPeerStakes
				if peerFrozen, exists := r.freezedPeerStakes[frozen.PeerID]; exists {
					newPeerFrozen := make([]freezedStake, 0)
					for _, pf := range peerFrozen {
						if pf.ID != frozen.ID {
							newPeerFrozen = append(newPeerFrozen, pf)
						}
					}
					if len(newPeerFrozen) == 0 {
						delete(r.freezedPeerStakes, frozen.PeerID)
						modifiedPeers[frozen.PeerID] = true // Mark for Redis deletion
					} else {
						r.freezedPeerStakes[frozen.PeerID] = newPeerFrozen
						modifiedPeers[frozen.PeerID] = true // Mark for Redis update
					}
				}
			} else {
				// Keep this stake frozen
				newFreezedStakes = append(newFreezedStakes, frozen)
			}
		}

		r.freezedStakes = newFreezedStakes

		if unfrozenCount > 0 {
			log.Printf("[Reg] 🔓 Unfroze %d stake(s)\n", unfrozenCount)

			// Persist changes to Redis
			// Update global freezedStakes
			if err := r.storage.SaveFreezedStakes(context.Background(), convertToStorageFreezedStakeSlice(r.freezedStakes)); err != nil {
				log.Printf("[Reg] Warning: Failed to save global freezed stakes to Redis: %v", err)
			}

			// Update or delete freezedPeerStakes for affected peers
			for pid := range modifiedPeers {
				if peerFrozen, exists := r.freezedPeerStakes[pid]; exists {
					if err := r.storage.SaveFreezedPeerStakes(context.Background(), pid, convertToStorageFreezedStakeSlice(peerFrozen)); err != nil {
						log.Printf("[Reg] Warning: Failed to save freezed peer stakes to Redis: %v", err)
					}
				} else {
					if err := r.storage.DeleteFreezedPeerStakes(context.Background(), pid); err != nil {
						log.Printf("[Reg] Warning: Failed to delete freezed peer stakes from Redis: %v", err)
					}
				}
			}
		}

		r.stakeMu.Unlock()
	}
}

// --- Index helpers ---

func (r *RegistryNode) addToIndex(pid peer.ID, serviceName string) {
	list := r.ServiceIndex[serviceName]
	for _, id := range list {
		if id == pid {
			return
		}
	}
	r.ServiceIndex[serviceName] = append(list, pid)
}

func (r *RegistryNode) removeFromIndex(pid peer.ID, serviceName string) {
	list := r.ServiceIndex[serviceName]
	newList := make([]peer.ID, 0, len(list))
	for _, id := range list {
		if id != pid {
			newList = append(newList, id)
		}
	}
	r.ServiceIndex[serviceName] = newList
}

// --- Storage conversion helpers ---

// convertToStorageRecord converts main.RegistrationRecord to storage.RegistrationRecord
func (r *RegistryNode) convertToStorageRecord(record *RegistrationRecord) *storage.RegistrationRecord {
	if record == nil {
		return nil
	}
	return &storage.RegistrationRecord{
		LastSeen:    record.LastSeen,
		ServiceCard: record.ServiceCard,
		StakeProof:  record.StakeProof,
		AddrInfo:    record.AddrInfo,
	}
}

// convertFromStorageRecord converts storage.RegistrationRecord to main.RegistrationRecord
func (r *RegistryNode) convertFromStorageRecord(record *storage.RegistrationRecord) *RegistrationRecord {
	if record == nil {
		return nil
	}
	return &RegistrationRecord{
		LastSeen:    record.LastSeen,
		ServiceCard: record.ServiceCard,
		StakeProof:  record.StakeProof,
		AddrInfo:    record.AddrInfo,
	}
}

// convertToStorageFreezedStake converts main.freezedStake to storage.FreezedStake
func convertToStorageFreezedStake(fs freezedStake) storage.FreezedStake {
	return storage.FreezedStake{
		ID:        fs.ID,
		PeerID:    fs.PeerID.String(),
		CreatedAt: fs.CreatedAt,
	}
}

// convertFromStorageFreezedStake converts storage.FreezedStake to main.freezedStake
func convertFromStorageFreezedStake(sfs storage.FreezedStake) (freezedStake, error) {
	pid, err := peer.Decode(sfs.PeerID)
	if err != nil {
		return freezedStake{}, fmt.Errorf("failed to decode peer ID: %v", err)
	}
	return freezedStake{
		ID:        sfs.ID,
		PeerID:    pid,
		CreatedAt: sfs.CreatedAt,
	}, nil
}

// convertToStorageFreezedStakeSlice converts a slice of main.freezedStake to storage.FreezedStake
func convertToStorageFreezedStakeSlice(fsList []freezedStake) []storage.FreezedStake {
	result := make([]storage.FreezedStake, len(fsList))
	for i, fs := range fsList {
		result[i] = convertToStorageFreezedStake(fs)
	}
	return result
}

// convertFromStorageFreezedStakeSlice converts a slice of storage.FreezedStake to main.freezedStake
func convertFromStorageFreezedStakeSlice(sfsList []storage.FreezedStake) ([]freezedStake, error) {
	result := make([]freezedStake, 0, len(sfsList))
	for _, sfs := range sfsList {
		fs, err := convertFromStorageFreezedStake(sfs)
		if err != nil {
			log.Printf("[Reg] Warning: Failed to convert freezed stake: %v", err)
			continue
		}
		result = append(result, fs)
	}
	return result, nil
}

// restoreStateFromRedis restores the registry state from Redis on startup.
// It loads all registrations, rebuilds the ServiceIndex, and restores stake data.
func (r *RegistryNode) restoreStateFromRedis(ctx context.Context) error {
	if r.storage == nil {
		return nil
	}

	log.Println("[Reg] Restoring state from Redis...")

	// Load all registrations from Redis
	storageRecords, err := r.storage.RestoreAllRegistrations(ctx)
	if err != nil {
		return fmt.Errorf("failed to restore registrations: %v", err)
	}

	// Convert storage records to main records and rebuild the index
	r.mu.Lock()
	for pid, storageRecord := range storageRecords {
		record := r.convertFromStorageRecord(storageRecord)
		r.Registrations[pid] = record
		r.addToIndex(pid, record.ServiceCard.Name)
	}
	r.mu.Unlock()

	if len(storageRecords) > 0 {
		log.Printf("[Reg] ✅ Restored %d active registrations", len(storageRecords))
		serviceCount := len(r.ServiceIndex)
		if serviceCount > 0 {
			log.Printf("[Reg] Services available: %d unique services", serviceCount)
		}
	} else {
		log.Println("[Reg] No registrations found in Redis")
	}

	// Load stake data from Redis
	r.stakeMu.Lock()
	defer r.stakeMu.Unlock()

	// Restore peerStakes
	peerStakes, err := r.storage.RestoreAllPeerStakes(ctx)
	if err != nil {
		log.Printf("[Reg] Warning: Failed to restore peer stakes from Redis: %v", err)
	} else {
		r.peerStakes = peerStakes
		log.Printf("[Reg] ✅ Restored stakes for %d peers", len(peerStakes))
	}

	// Restore freezedPeerStakes
	freezedPeerStakes, err := r.storage.RestoreAllFreezedPeerStakes(ctx)
	if err != nil {
		log.Printf("[Reg] Warning: Failed to restore freezed peer stakes from Redis: %v", err)
	} else {
		// Convert storage.FreezedStake to main.freezedStake
		convertedFreezedPeerStakes := make(map[peer.ID][]freezedStake)
		for pid, sfsList := range freezedPeerStakes {
			fsList, err := convertFromStorageFreezedStakeSlice(sfsList)
			if err != nil {
				log.Printf("[Reg] Warning: Error converting freezed peer stakes for %s: %v", pid.ShortString(), err)
				continue
			}
			convertedFreezedPeerStakes[pid] = fsList
		}
		r.freezedPeerStakes = convertedFreezedPeerStakes
		log.Printf("[Reg] ✅ Restored freezed stakes for %d peers", len(convertedFreezedPeerStakes))
	}

	// Restore freezedStakes (global list)
	freezedStakes, err := r.storage.LoadFreezedStakes(ctx)
	if err != nil {
		log.Printf("[Reg] Warning: Failed to restore global freezed stakes from Redis: %v", err)
	} else {
		fsList, err := convertFromStorageFreezedStakeSlice(freezedStakes)
		if err != nil {
			log.Printf("[Reg] Warning: Error converting global freezed stakes: %v", err)
		} else {
			r.freezedStakes = fsList
			log.Printf("[Reg] ✅ Restored %d global freezed stakes", len(fsList))
		}
	}

	return nil
}

func (r *RegistryNode) validateEmbedding(vec []float32) error {
	if r.embeddingDim <= 0 {
		return fmt.Errorf("registry embedding dim not configured")
	}
	if len(vec) != r.embeddingDim {
		return fmt.Errorf("embedding length %d does not match required %d", len(vec), r.embeddingDim)
	}
	return nil
}

// reindexQdrant upserts all current registrations into Qdrant (used on startup restore).
func (r *RegistryNode) reindexQdrant(ctx context.Context) error {
	if r.qdrant == nil {
		return nil
	}

	type item struct {
		pid    peer.ID
		record *RegistrationRecord
	}
	entries := []item{}

	r.mu.Lock()
	for pid, rec := range r.Registrations {
		entries = append(entries, item{pid: pid, record: rec})
	}
	r.mu.Unlock()

	for _, it := range entries {
		if err := r.validateEmbedding(it.record.ServiceCard.Embedding); err != nil {
			log.Printf("[Reg] Skipping Qdrant reindex for %s: %v\n", it.pid.ShortString(), err)
			continue
		}

		payload := map[string]interface{}{
			"service_name": it.record.ServiceCard.Name,
			"peer_id":      it.pid.String(),
			"description":  it.record.ServiceCard.Description,
			"tags":         it.record.ServiceCard.Tags,
			"version":      it.record.ServiceCard.Version,
			"cost_per_op":  it.record.ServiceCard.CostPerOp,
		}
		pointID := fmt.Sprintf("%s:%s", it.pid.String(), it.record.ServiceCard.Name)

		if err := r.qdrant.UpsertService(pointID, it.record.ServiceCard.Embedding, payload); err != nil {
			log.Printf("[Reg] Qdrant upsert failed for %s: %v\n", pointID, err)
		}
	}

	return nil
}

// checkStakeValidity verifies signature and amount, but DOES NOT check replay/nonce.
// This is used for both new registrations and verifying stored heartbeats.
func (r *RegistryNode) checkStakeValidity(remote peer.ID, proof *common.StakeProof) error {
	if proof == nil {
		return fmt.Errorf("stake proof required")
	}

	if proof.Staker != "" && proof.Staker != remote.String() {
		return fmt.Errorf("stake staker mismatch (expected %s got %s)", remote.ShortString(), proof.Staker)
	}

	if proof.IsEVMMode() {
		return r.checkEVMStakeValidity(remote, proof)
	}

	return r.checkMockStakeValidity(remote, proof)
}

func (r *RegistryNode) checkMockStakeValidity(remote peer.ID, proof *common.StakeProof) error {
	if r.stakingMode == "evm" {
		return fmt.Errorf("mock stake proofs not accepted (registry in EVM-only mode)")
	}

	if proof.Amount < r.minStake {
		return fmt.Errorf("stake too low: have %.2f need %.2f", proof.Amount, r.minStake)
	}

	pubKey := r.Host.Peerstore().PubKey(remote)
	if pubKey == nil {
		return fmt.Errorf("missing pubkey for %s", remote.ShortString())
	}

	payload := fmt.Sprintf("%s|%f|%d|%d|%s", proof.TxHash, proof.Amount, proof.Nonce, proof.Timestamp, proof.ChainID)
	digest := sha256.Sum256([]byte(payload))
	if ok, err := pubKey.Verify(digest[:], proof.Signature); err != nil || !ok {
		if err != nil {
			return fmt.Errorf("stake signature verify failed: %v", err)
		}
		return fmt.Errorf("stake signature invalid")
	}

	return nil
}

func (r *RegistryNode) checkEVMStakeValidity(remote peer.ID, proof *common.StakeProof) error {
	if r.evmVerifier == nil {
		return fmt.Errorf("EVM verification not configured")
	}

	if proof.WalletAddress == "" {
		return fmt.Errorf("wallet address required for EVM mode")
	}

	if len(proof.WalletSignature) == 0 {
		return fmt.Errorf("wallet signature required for EVM mode")
	}

	expectedChainID := r.evmVerifier.GetChainID().Int64()
	if proof.EvmChainID != expectedChainID {
		return fmt.Errorf("chain ID mismatch: proof has %d, registry expects %d", proof.EvmChainID, expectedChainID)
	}

	if proof.StakingContract == "" {
		return fmt.Errorf("staking contract required for EVM mode")
	}
	if !ethcommon.IsHexAddress(proof.StakingContract) {
		return fmt.Errorf("staking contract address invalid")
	}
	if (r.evmContractAddress == ethcommon.Address{}) {
		return fmt.Errorf("registry staking contract not configured")
	}
	proofContract := ethcommon.HexToAddress(proof.StakingContract)
	if proofContract != r.evmContractAddress {
		return fmt.Errorf("staking contract mismatch: proof has %s, registry expects %s", proofContract.Hex(), r.evmContractAddress.Hex())
	}

	if proof.WalletExpiresAt > 0 && time.Now().Unix() > proof.WalletExpiresAt {
		return fmt.Errorf("wallet binding signature expired")
	}

	expectedMessage := proof.GetWalletBindingMessage()
	valid, err := r.evmVerifier.VerifyWalletSignature(proof.WalletAddress, proof.WalletSignature, expectedMessage)
	if err != nil {
		return fmt.Errorf("wallet signature verification failed: %v", err)
	}
	if !valid {
		return fmt.Errorf("wallet signature invalid")
	}

	if existingPeerID, bound := r.evmVerifier.GetBoundPeerID(proof.WalletAddress); bound {
		if existingPeerID != remote.String() {
			return fmt.Errorf("wallet %s already bound to peer %s", proof.WalletAddress, existingPeerID)
		}
	}

	if boundWallet, bound := r.evmVerifier.GetBoundWallet(remote.String()); bound {
		walletAddr := ethcommon.HexToAddress(proof.WalletAddress)
		boundAddr := ethcommon.HexToAddress(boundWallet)
		if boundAddr != walletAddr {
			return fmt.Errorf("peer %s already bound to wallet %s", remote.ShortString(), boundAddr.Hex())
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	walletAddr := ethcommon.HexToAddress(proof.WalletAddress)
	isValid, err := r.evmVerifier.IsValidStaker(ctx, walletAddr)
	if err != nil {
		return fmt.Errorf("on-chain stake verification failed: %v", err)
	}
	if !isValid {
		return fmt.Errorf("wallet %s has no valid stake on-chain", proof.WalletAddress)
	}

	return nil
}

func (r *RegistryNode) isHeartbeat(existing *RegistrationRecord, isRegistered bool, proof *common.StakeProof) bool {
	if !isRegistered || existing == nil || existing.StakeProof == nil || proof == nil {
		return false
	}

	existingIsEVM := existing.StakeProof.IsEVMMode()
	incomingIsEVM := proof.IsEVMMode()

	if existingIsEVM || incomingIsEVM {
		if !existingIsEVM || !incomingIsEVM {
			return false
		}
		if existing.StakeProof.WalletAddress == "" || proof.WalletAddress == "" {
			return false
		}
		existingAddr := ethcommon.HexToAddress(existing.StakeProof.WalletAddress)
		incomingAddr := ethcommon.HexToAddress(proof.WalletAddress)
		return existingAddr == incomingAddr &&
			existing.StakeProof.WalletNonce == proof.WalletNonce
	}

	return existing.StakeProof.TxHash == proof.TxHash
}

func (r *RegistryNode) applyHeartbeat(remote peer.ID, providerInfo *peer.AddrInfo) bool {
	r.mu.Lock()
	defer r.mu.Unlock()

	entry, ok := r.Registrations[remote]
	if !ok {
		return false
	}

	entry.LastSeen = time.Now()
	if providerInfo != nil {
		entry.AddrInfo = *providerInfo
	}

	storageRecord := r.convertToStorageRecord(entry)
	if err := r.storage.SaveRegistration(context.Background(), remote, storageRecord); err != nil {
		log.Printf("[Reg] Warning: Failed to save heartbeat to Redis: %v", err)
	}

	return true
}

func (r *RegistryNode) checkAndRecordReplay(remotePeer peer.ID, proof *common.StakeProof) (string, error) {
	var key string

	if proof.IsEVMMode() {
		key = fmt.Sprintf("evm|%s|%d", strings.ToLower(proof.WalletAddress), proof.WalletNonce)

		if r.evmVerifier != nil {
			isNew, err := r.evmVerifier.CheckAndRecordNonce(proof.WalletAddress, proof.WalletNonce)
			if err != nil {
				return "", fmt.Errorf("nonce check failed: %w", err)
			}
			if !isNew {
				if r.evmVerifier.IsNonceUsedAsHeartbeat(proof.WalletAddress, proof.WalletNonce, remotePeer.String()) {
					return key, nil
				}
				return "", fmt.Errorf("wallet nonce already used (replay detected)")
			}
		}
	} else {
		key = fmt.Sprintf("mock|%s|%d", proof.TxHash, proof.Nonce)

		r.stakeMu.Lock()
		defer r.stakeMu.Unlock()

		stakes := r.peerStakes[remotePeer]
		for _, existingKey := range stakes {
			if existingKey == key {
				return "", fmt.Errorf("stake proof already used (replay detected)")
			}
		}
		r.peerStakes[remotePeer] = append(stakes, key)

		if err := r.storage.SavePeerStakes(context.Background(), remotePeer, r.peerStakes[remotePeer]); err != nil {
			log.Printf("[Reg] Warning: Failed to save peer stakes to Redis: %v", err)
		}
	}

	return key, nil
}

func (r *RegistryNode) handleStream(stream network.Stream) {
	defer stream.Close()
	rw := bufio.NewReadWriter(bufio.NewReader(stream), bufio.NewWriter(stream))

	var req common.RegistryRequest
	if err := json.NewDecoder(rw).Decode(&req); err != nil {
		return
	}

	resp := common.RegistryResponse{Success: false}
	remotePeer := stream.Conn().RemotePeer()

	switch req.Method {
	case "register":
		r.mu.Lock()
		existing, isRegistered := r.Registrations[remotePeer]
		r.mu.Unlock()

		isHeartbeat := r.isHeartbeat(existing, isRegistered, req.StakeProof)

		if isHeartbeat {
			if r.applyHeartbeat(remotePeer, req.ProviderInfo) {
				log.Printf("[Reg] ❤️ Heartbeat received: %s\n", remotePeer.ShortString())
				resp.Success = true
			}
		} else {
			if err := r.checkStakeValidity(remotePeer, req.StakeProof); err != nil {
				resp.Error = err.Error()
				log.Printf("[Reg] ❌ Stake Invalid: %v\n", err)
				break
			}

			replayKey, replayErr := r.checkAndRecordReplay(remotePeer, req.StakeProof)
			if replayErr != nil {
				resp.Error = replayErr.Error()
				log.Printf("[Reg] ❌ Replay Attack: %s\n", resp.Error)
				break
			}

			if req.StakeProof.IsEVMMode() && r.evmVerifier != nil {
				if err := r.evmVerifier.RecordWalletBinding(req.StakeProof.WalletAddress, remotePeer.String()); err != nil {
					resp.Error = err.Error()
					log.Printf("[Reg] ❌ Wallet Binding Rejected: %v\n", err)
					break
				}
			}

			_ = replayKey

			var embedding []float32
			if r.qdrant != nil {
				if err := r.validateEmbedding(req.Card.Embedding); err != nil {
					resp.Error = fmt.Sprintf("invalid embedding: %v", err)
					log.Printf("[Reg] ❌ Embedding invalid: %v\n", err)
					break
				}
				embedding = req.Card.Embedding
			}

			r.mu.Lock()

			// Remove old index entry if service name changed
			if isRegistered && existing.ServiceCard.Name != req.Card.Name {
				r.removeFromIndex(remotePeer, existing.ServiceCard.Name)
			}

			if req.ProviderInfo != nil {
				newRecord := &RegistrationRecord{
					LastSeen:    time.Now(),
					ServiceCard: req.Card,
					StakeProof:  req.StakeProof,
					AddrInfo:    *req.ProviderInfo,
				}
				r.Registrations[remotePeer] = newRecord
				r.addToIndex(remotePeer, req.Card.Name)

				// Save to Redis if enabled
				storageRecord := r.convertToStorageRecord(newRecord)
				if err := r.storage.SaveRegistration(context.Background(), remotePeer, storageRecord); err != nil {
					log.Printf("[Reg] Warning: Failed to save registration to Redis: %v", err)
				}
			}

			log.Printf("[Reg] ✅ New Registration: %s (Service: %s)\n", remotePeer.ShortString(), req.Card.Name)
			resp.Success = true

			r.mu.Unlock()

			// Optional: index in Qdrant for semantic search
			if r.qdrant != nil && len(embedding) > 0 {
				payload := map[string]interface{}{
					"service_name": req.Card.Name,
					"peer_id":      remotePeer.String(),
					"description":  req.Card.Description,
					"tags":         req.Card.Tags,
					"version":      req.Card.Version,
					"cost_per_op":  req.Card.CostPerOp,
				}
				pointID := fmt.Sprintf("%s:%s", remotePeer.String(), req.Card.Name)
				if err := r.qdrant.UpsertService(pointID, embedding, payload); err != nil {
					log.Printf("[Reg] Qdrant upsert error: %v\n", err)
				}
			}
		}

	case "find":
		r.mu.Lock()
		results := []peer.AddrInfo{}
		query := strings.ToLower(req.Query)

		for name, peerIDs := range r.ServiceIndex {
			if strings.Contains(strings.ToLower(name), query) {
				for _, pid := range peerIDs {
					if reg, ok := r.Registrations[pid]; ok {
						results = append(results, reg.AddrInfo)
					}
				}
			}
		}
		resp.Providers = results
		resp.Success = true
		log.Printf("[Reg] Served query '%s' -> %d providers\n", req.Query, len(results))
		r.mu.Unlock()

	case "unregister":
		r.handleUnregister(remotePeer, &req, &resp)

	default:
		resp.Error = "Unknown method"
	}

	_ = json.NewEncoder(rw).Encode(resp)
	_ = rw.Flush()
}

func (r *RegistryNode) handleUnregister(remotePeer peer.ID, req *common.RegistryRequest, resp *common.RegistryResponse) {
	if req.StakeProof == nil {
		resp.Error = "stake proof required for unregister"
		log.Printf("[Reg] ❌ Unregister failed: no stake proof provided\n")
		return
	}

	// Get the stake key based on mode
	var stakeKey string
	if req.StakeProof.IsEVMMode() {
		stakeKey = fmt.Sprintf("evm|%s|%d", strings.ToLower(req.StakeProof.WalletAddress), req.StakeProof.WalletNonce)
	} else {
		stakeKey = fmt.Sprintf("mock|%s|%d", req.StakeProof.TxHash, req.StakeProof.Nonce)
	}

	// For EVM mode, just remove from registrations (stake is on-chain)
	if req.StakeProof.IsEVMMode() {
		r.mu.Lock()
		if registration, exists := r.Registrations[remotePeer]; exists {
			serviceName := registration.ServiceCard.Name

			// Remove from the service index
			r.removeFromIndex(remotePeer, serviceName)

			// Remove from registrations
			delete(r.Registrations, remotePeer)

			// Remove from Redis storage
			if err := r.storage.DeleteRegistration(context.Background(), remotePeer, serviceName); err != nil {
				log.Printf("[Reg] Warning: Failed to delete registration from Redis: %v", err)
			}

			// Remove from Qdrant if enabled
			if r.qdrant != nil {
				pointID := fmt.Sprintf("%s:%s", remotePeer.String(), serviceName)
				if err := r.qdrant.RemoveService(pointID); err != nil {
					log.Printf("[Reg] Warning: Qdrant remove error: %v\n", err)
				}
			}

			log.Printf("[Reg] 🗑️ Removed EVM service %s for peer %s\n", serviceName, remotePeer.ShortString())
		}
		r.mu.Unlock()

		resp.Success = true
		log.Printf("[Reg] ✅ EVM unregister complete for %s (stake managed on-chain)\n", remotePeer.ShortString())
		return
	}

	// Mock mode: freeze the stake
	r.stakeMu.Lock()

	// Find and remove the stake from peerStakes
	stakes, exists := r.peerStakes[remotePeer]
	if !exists {
		r.stakeMu.Unlock()
		resp.Error = "no stakes found for peer"
		log.Printf("[Reg] ❌ Unregister failed: no stakes for %s\n", remotePeer.ShortString())
		return
	}

	// Check if the stake exists and remove it from peerStakes
	found := false
	newStakes := make([]string, 0, len(stakes))
	for _, existingKey := range stakes {
		if existingKey == stakeKey {
			found = true
		} else {
			newStakes = append(newStakes, existingKey)
		}
	}

	if !found {
		r.stakeMu.Unlock()
		resp.Error = "stake not found for this peer"
		log.Printf("[Reg] ❌ Unregister failed: stake %s not found for %s\n", stakeKey, remotePeer.ShortString())
		return
	}

	// Update peerStakes (or delete if no stakes left)
	if len(newStakes) == 0 {
		delete(r.peerStakes, remotePeer)
		// Remove from Redis
		if err := r.storage.DeletePeerStakes(context.Background(), remotePeer); err != nil {
			log.Printf("[Reg] Warning: Failed to delete peer stakes from Redis: %v", err)
		}
	} else {
		r.peerStakes[remotePeer] = newStakes
		// Update in Redis
		if err := r.storage.SavePeerStakes(context.Background(), remotePeer, newStakes); err != nil {
			log.Printf("[Reg] Warning: Failed to save peer stakes to Redis: %v", err)
		}
	}

	// Create frozen stake
	now := time.Now().Unix()
	frozen := freezedStake{
		ID:        stakeKey,
		PeerID:    remotePeer,
		CreatedAt: now,
	}

	// Add to freezedPeerStakes
	r.freezedPeerStakes[remotePeer] = append(r.freezedPeerStakes[remotePeer], frozen)

	// Persist freezedPeerStakes to Redis
	if err := r.storage.SaveFreezedPeerStakes(context.Background(), remotePeer, convertToStorageFreezedStakeSlice(r.freezedPeerStakes[remotePeer])); err != nil {
		log.Printf("[Reg] Warning: Failed to save freezed peer stakes to Redis: %v", err)
	}

	// Add to global freezedStakes list
	r.freezedStakes = append(r.freezedStakes, frozen)

	// Persist global freezedStakes to Redis
	if err := r.storage.SaveFreezedStakes(context.Background(), convertToStorageFreezedStakeSlice(r.freezedStakes)); err != nil {
		log.Printf("[Reg] Warning: Failed to save global freezed stakes to Redis: %v", err)
	}

	r.stakeMu.Unlock()

	// Remove service from registrations and index
	r.mu.Lock()
	if registration, exists := r.Registrations[remotePeer]; exists {
		serviceName := registration.ServiceCard.Name

		// Remove from the service index
		r.removeFromIndex(remotePeer, serviceName)

		// Remove from registrations
		delete(r.Registrations, remotePeer)

		// Remove from Redis storage
		if err := r.storage.DeleteRegistration(context.Background(), remotePeer, serviceName); err != nil {
			log.Printf("[Reg] Warning: Failed to delete registration from Redis: %v", err)
		}

		// Remove from Qdrant if enabled
		if r.qdrant != nil {
			pointID := fmt.Sprintf("%s:%s", remotePeer.String(), serviceName)
			if err := r.qdrant.RemoveService(pointID); err != nil {
				log.Printf("[Reg] Warning: Qdrant remove error: %v\n", err)
			}
		}

		log.Printf("[Reg] 🗑️ Removed service %s for peer %s\n", serviceName, remotePeer.ShortString())
	}
	r.mu.Unlock()

	resp.Success = true
	log.Printf("[Reg] 🧊 Unregistered stake %s for %s: frozen until %s\n",
		stakeKey, remotePeer.ShortString(), time.Unix(now+UNFREEZE_DELAY, 0).Format(time.RFC3339))
}

// setupRESTAPI configures the Gin router with read-only endpoints for Services
func (r *RegistryNode) setupRESTAPI() *gin.Engine {
	gin.SetMode(gin.ReleaseMode)
	router := gin.Default()

	// Enable CORS for frontend
	router.Use(cors.New(cors.Config{
		AllowOrigins:     []string{"http://localhost:5173", "http://localhost:3000", "http://127.0.0.1:5173"},
		AllowMethods:     []string{"GET", "POST", "OPTIONS"},
		AllowHeaders:     []string{"Origin", "Content-Type", "Accept", "Authorization"},
		ExposeHeaders:    []string{"Content-Length"},
		AllowCredentials: true,
		MaxAge:           12 * time.Hour,
	}))

	router.GET("/health", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"status": "ok"})
	})

	api := router.Group("/api/v1")
	{
		api.GET("/services", r.getAllServices)
		api.GET("/services_full", r.getAllServicesFull)

		// GET services by name (query parameter)
		api.GET("/services/search", r.searchServices)

		// GET specific service by exact name
		api.GET("/services/:name", r.getServiceByName)

		// GET semantic search (optional; Qdrant-backed)
		api.GET("/services/semantic_search", r.semanticSearchServices)

		// GET registry info (Peer ID and multiaddr)
		api.GET("/registry/info", r.getRegistryInfo)

		// Credits API (Agent-to-Agent payments)
		api.GET("/credits/:peerId", r.getBalance)
		api.POST("/credits/deposit", r.depositCredits)
		api.POST("/credits/charge", r.chargeCredits)
		api.GET("/credits/:peerId/history", r.getTransactionHistory)

		// Settlement API (Withdraw credits to blockchain)
		api.POST("/credits/withdraw", r.requestWithdrawal)
		api.GET("/credits/withdraw/:requestId", r.getWithdrawal)
		api.GET("/credits/withdrawals/:peerId", r.getWithdrawalHistory)

		// Blockchain Deposit API (ETH -> credits)
		api.GET("/deposit/info", r.getDepositInfo)
		api.GET("/deposit/history/:peerId", r.getDepositHistory)
	}

	return router
}

// getAllServices returns all registered services
func (r *RegistryNode) getAllServices(c *gin.Context) {
	r.mu.Lock()
	defer r.mu.Unlock()

	view := make(map[string][]peer.AddrInfo)
	for _, reg := range r.Registrations {
		name := reg.ServiceCard.Name
		view[name] = append(view[name], reg.AddrInfo)
	}

	c.JSON(http.StatusOK, gin.H{
		"services": view,
		"count":    len(view),
	})
}

// getAllServicesFull returns all services with their ServiceCard and providers.
func (r *RegistryNode) getAllServicesFull(c *gin.Context) {
	r.mu.Lock()
	defer r.mu.Unlock()

	view := make(map[string]gin.H)
	for _, reg := range r.Registrations {
		name := reg.ServiceCard.Name
		entry, ok := view[name]
		if !ok {
			entry = gin.H{
				"card":      reg.ServiceCard,
				"providers": []peer.AddrInfo{},
			}
		}
		providers := entry["providers"].([]peer.AddrInfo)
		providers = append(providers, reg.AddrInfo)
		entry["providers"] = providers
		view[name] = entry
	}

	c.JSON(http.StatusOK, gin.H{
		"services": view,
		"count":    len(view),
	})
}

// searchServices searches for services by name (partial match)
func (r *RegistryNode) searchServices(c *gin.Context) {
	query := c.Query("q")
	if query == "" {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": "query parameter 'q' is required",
		})
		return
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	results := make(map[string][]peer.AddrInfo)
	queryLower := strings.ToLower(query)

	for name, peerIDs := range r.ServiceIndex {
		if strings.Contains(strings.ToLower(name), queryLower) {
			for _, pid := range peerIDs {
				if reg, ok := r.Registrations[pid]; ok {
					results[name] = append(results[name], reg.AddrInfo)
				}
			}
		}
	}

	c.JSON(http.StatusOK, gin.H{
		"query":    query,
		"services": results,
		"count":    len(results),
	})
}

// getServiceByName returns providers for a specific service name
func (r *RegistryNode) getServiceByName(c *gin.Context) {
	serviceName := c.Param("name")

	r.mu.Lock()
	defer r.mu.Unlock()

	providers := []peer.AddrInfo{}
	if peerIDs, ok := r.ServiceIndex[serviceName]; ok {
		for _, pid := range peerIDs {
			if reg, ok := r.Registrations[pid]; ok {
				providers = append(providers, reg.AddrInfo)
			}
		}
	}

	if len(providers) == 0 {
		c.JSON(http.StatusNotFound, gin.H{
			"error": fmt.Sprintf("service '%s' not found", serviceName),
		})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"service":   serviceName,
		"providers": providers,
		"count":     len(providers),
	})
}

// semanticSearchServices exposes a Qdrant-backed semantic search endpoint.
// GET /api/v1/services/semantic_search?q=...&k=5
func (r *RegistryNode) semanticSearchServices(c *gin.Context) {
	if r.qdrant == nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": "semantic search is not enabled (no Qdrant configured)",
		})
		return
	}
	if r.embedder == nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": "semantic search is not configured with an embedder (set embedding API key/model)",
		})
		return
	}

	query := c.Query("q")
	if strings.TrimSpace(query) == "" {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": "query parameter 'q' is required",
		})
		return
	}

	kStr := c.DefaultQuery("k", "5")
	k, err := strconv.Atoi(kStr)
	if err != nil || k <= 0 {
		k = 5
	}

	vector, err := r.embedder.EmbedText(c.Request.Context(), query)
	if err != nil {
		log.Printf("[Reg] Embed error: %v\n", err)
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": fmt.Sprintf("failed to embed query: %v", err),
		})
		return
	}

	results, err := r.qdrant.Search(vector, k)
	if err != nil {
		log.Printf("[Reg] Qdrant search error: %v\n", err)
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": fmt.Sprintf("qdrant search failed: %v", err),
		})
		return
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	type apiResult struct {
		ServiceName string             `json:"service_name"`
		Score       float64            `json:"score"`
		Card        common.ServiceCard `json:"card"`
		Providers   []peer.AddrInfo    `json:"providers"`
	}

	apiResults := make([]apiResult, 0, len(results))

	for _, hit := range results {
		payload := hit.Payload
		serviceName, _ := payload["service_name"].(string)
		peerIDStr, _ := payload["peer_id"].(string)
		if serviceName == "" || peerIDStr == "" {
			continue
		}

		pid, err := peer.Decode(peerIDStr)
		if err != nil {
			continue
		}

		reg, ok := r.Registrations[pid]
		if !ok || reg.ServiceCard.Name != serviceName {
			continue
		}

		apiResults = append(apiResults, apiResult{
			ServiceName: serviceName,
			Score:       hit.Score,
			Card:        reg.ServiceCard,
			Providers:   []peer.AddrInfo{reg.AddrInfo},
		})
	}

	c.JSON(http.StatusOK, gin.H{
		"query":   query,
		"results": apiResults,
		"count":   len(apiResults),
	})
}

// getRegistryInfo returns the registry's Peer ID and multiaddrs
// GET /api/v1/registry/info
func (r *RegistryNode) getRegistryInfo(c *gin.Context) {
	addrs := make([]string, 0, len(r.Host.Addrs()))
	for _, addr := range r.Host.Addrs() {
		addrs = append(addrs, fmt.Sprintf("%s/p2p/%s", addr, r.Host.ID()))
	}

	// Find UDP port for bootstrap (prefer QUIC)
	var bootstrapAddr string
	for _, addr := range r.Host.Addrs() {
		protocols := addr.Protocols()
		if len(protocols) > 0 && protocols[0].Code == ma.P_IP4 {
			port, err := addr.ValueForProtocol(ma.P_UDP)
			if err != nil {
				port, _ = addr.ValueForProtocol(ma.P_TCP)
			}
			if port != "" {
				bootstrapAddr = fmt.Sprintf("/ip4/127.0.0.1/udp/%s/quic-v1/p2p/%s", port, r.Host.ID())
				break
			}
		}
	}
	if bootstrapAddr == "" && len(addrs) > 0 {
		// Fallback to first addr
		bootstrapAddr = addrs[0]
	}

	c.JSON(http.StatusOK, gin.H{
		"peer_id":    r.Host.ID().String(),
		"multiaddrs": addrs,
		"bootstrap":  bootstrapAddr,
	})
}

// --- Credits API (Agent-to-Agent payments) ---

// getBalance returns the credit balance for a peer
// GET /api/v1/credits/:peerId
func (r *RegistryNode) getBalance(c *gin.Context) {
	peerID := c.Param("peerId")
	if peerID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "peerId is required"})
		return
	}

	if r.storage == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "credits system requires Redis storage"})
		return
	}

	account, err := r.storage.GetBalance(c.Request.Context(), peerID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, account)
}

// depositCredits adds credits to a peer's balance
// POST /api/v1/credits/deposit
func (r *RegistryNode) depositCredits(c *gin.Context) {
	if r.storage == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "credits system requires Redis storage"})
		return
	}

	var req common.CreditDepositRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid request body"})
		return
	}

	if req.PeerID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "peer_id is required"})
		return
	}

	if req.Amount <= 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "amount must be positive"})
		return
	}

	account, err := r.storage.AddBalance(c.Request.Context(), req.PeerID, req.Amount)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	log.Printf("[Reg] Deposited %.4f credits to %s", req.Amount, req.PeerID[:12])
	c.JSON(http.StatusOK, gin.H{
		"success": true,
		"account": account,
	})
}

// chargeCredits transfers credits from one peer to another
// POST /api/v1/credits/charge
func (r *RegistryNode) chargeCredits(c *gin.Context) {
	if r.storage == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "credits system requires Redis storage"})
		return
	}

	var req common.CreditChargeRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid request body"})
		return
	}

	if req.FromPeer == "" || req.ToPeer == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "from_peer and to_peer are required"})
		return
	}

	if req.Amount <= 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "amount must be positive"})
		return
	}

	tx, err := r.storage.ChargeBalance(c.Request.Context(), req.FromPeer, req.ToPeer, req.Amount, req.Service)
	if err != nil {
		// Check if insufficient balance
		if strings.Contains(err.Error(), "insufficient balance") {
			c.JSON(http.StatusPaymentRequired, gin.H{"error": err.Error()})
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	log.Printf("[Reg] Charged %.4f credits: %s -> %s for %s", req.Amount, req.FromPeer[:12], req.ToPeer[:12], req.Service)
	c.JSON(http.StatusOK, gin.H{
		"success":     true,
		"transaction": tx,
	})
}

// getTransactionHistory returns the transaction history for a peer
// GET /api/v1/credits/:peerId/history
func (r *RegistryNode) getTransactionHistory(c *gin.Context) {
	peerID := c.Param("peerId")
	if peerID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "peerId is required"})
		return
	}

	if r.storage == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "credits system requires Redis storage"})
		return
	}

	limitStr := c.DefaultQuery("limit", "50")
	limit, err := strconv.Atoi(limitStr)
	if err != nil || limit <= 0 {
		limit = 50
	}

	transactions, err := r.storage.GetTransactions(c.Request.Context(), peerID, limit)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"peer_id":      peerID,
		"transactions": transactions,
		"count":        len(transactions),
	})
}

// --- Settlement (Withdraw credits to blockchain) ---

// requestWithdrawal handles POST /api/v1/credits/withdraw
func (r *RegistryNode) requestWithdrawal(c *gin.Context) {
	if !r.settlementEnabled {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "settlement not enabled"})
		return
	}

	if r.storage == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "settlement requires Redis storage"})
		return
	}

	var req common.WithdrawRequest
	if err := c.BindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid request: " + err.Error()})
		return
	}

	// Validate request
	if req.PeerID == "" || req.Amount <= 0 || req.WalletAddress == "" || req.Chain == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "peer_id, amount, wallet_address, and chain are required"})
		return
	}

	// Check chain
	if req.Chain != r.settlementChain {
		c.JSON(http.StatusBadRequest, gin.H{"error": fmt.Sprintf("unsupported chain: %s (only %s supported)", req.Chain, r.settlementChain)})
		return
	}

	// Validate wallet address
	if !ethcommon.IsHexAddress(req.WalletAddress) {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid wallet address"})
		return
	}

	// Check minimum amount
	if req.Amount < r.settlementMinAmount {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": fmt.Sprintf("amount below minimum: %.2f (minimum: %.2f)", req.Amount, r.settlementMinAmount),
		})
		return
	}

	// Check balance
	account, err := r.storage.GetBalance(c.Request.Context(), req.PeerID)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "account not found"})
		return
	}

	if account.Balance < req.Amount {
		c.JSON(http.StatusPaymentRequired, gin.H{
			"error": fmt.Sprintf("insufficient balance: have %.2f, need %.2f", account.Balance, req.Amount),
		})
		return
	}

	// Convert credits to wei
	ethAmount := req.Amount * r.settlementRate
	weiAmount := new(big.Float).Mul(big.NewFloat(ethAmount), big.NewFloat(1e18))
	weiInt, _ := weiAmount.Int(nil)
	amountWei := weiInt.String()

	// Create withdrawal request
	wdr, err := r.storage.CreateWithdrawal(
		c.Request.Context(),
		req.PeerID,
		req.Amount,
		req.WalletAddress,
		req.Chain,
		amountWei,
	)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to create withdrawal: " + err.Error()})
		return
	}

	log.Printf("[Settlement] Created withdrawal request %s: %.2f credits -> %.8f ETH (peer: %s, wallet: %s)",
		wdr.ID, wdr.Amount, ethAmount, req.PeerID[:12], req.WalletAddress)

	c.JSON(http.StatusOK, gin.H{
		"success": true,
		"request": wdr,
	})
}

// getWithdrawal handles GET /api/v1/credits/withdraw/:requestId
func (r *RegistryNode) getWithdrawal(c *gin.Context) {
	if !r.settlementEnabled {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "settlement not enabled"})
		return
	}

	if r.storage == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "settlement requires Redis storage"})
		return
	}

	requestID := c.Param("requestId")
	if requestID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "requestId is required"})
		return
	}

	wdr, err := r.storage.GetWithdrawal(c.Request.Context(), requestID)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "withdrawal not found"})
		return
	}

	c.JSON(http.StatusOK, wdr)
}

// getWithdrawalHistory handles GET /api/v1/credits/withdrawals/:peerId
func (r *RegistryNode) getWithdrawalHistory(c *gin.Context) {
	if !r.settlementEnabled {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "settlement not enabled"})
		return
	}

	if r.storage == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "settlement requires Redis storage"})
		return
	}

	peerID := c.Param("peerId")
	if peerID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "peerId is required"})
		return
	}

	limitStr := c.DefaultQuery("limit", "50")
	limit, err := strconv.Atoi(limitStr)
	if err != nil || limit <= 0 {
		limit = 50
	}

	withdrawals, err := r.storage.GetWithdrawals(c.Request.Context(), peerID, limit)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	// Calculate total withdrawn
	totalWithdrawn := 0.0
	for _, w := range withdrawals {
		if w.Status == "completed" {
			totalWithdrawn += w.Amount
		}
	}

	c.JSON(http.StatusOK, gin.H{
		"peer_id":         peerID,
		"withdrawals":     withdrawals,
		"count":           len(withdrawals),
		"total_withdrawn": totalWithdrawn,
	})
}

// withdrawalProcessor processes pending withdrawal requests in background
func (r *RegistryNode) withdrawalProcessor() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	log.Println("[Settlement] Withdrawal processor started")

	for range ticker.C {
		r.processPendingWithdrawals()
	}
}

// processPendingWithdrawals processes all pending withdrawals
func (r *RegistryNode) processPendingWithdrawals() {
	ctx := context.Background()

	pending, err := r.storage.GetPendingWithdrawals(ctx)
	if err != nil {
		log.Printf("[Settlement] Error getting pending withdrawals: %v", err)
		return
	}

	if len(pending) == 0 {
		return
	}

	log.Printf("[Settlement] Processing %d pending withdrawals", len(pending))

	for _, wdr := range pending {
		if err := r.processWithdrawal(ctx, &wdr); err != nil {
			log.Printf("[Settlement] Error processing withdrawal %s: %v", wdr.ID, err)
		}
	}
}

// processWithdrawal processes a single withdrawal request
func (r *RegistryNode) processWithdrawal(ctx context.Context, wdr *common.WithdrawalRequest) error {
	// Check balance again (might have changed)
	account, err := r.storage.GetBalance(ctx, wdr.PeerID)
	if err != nil || account.Balance < wdr.Amount {
		errMsg := "insufficient balance"
		if err != nil {
			errMsg = err.Error()
		}
		r.storage.UpdateWithdrawalStatus(ctx, wdr.ID, "failed", "", errMsg)
		return fmt.Errorf("%s", errMsg)
	}

	// Update status to processing
	if err := r.storage.UpdateWithdrawalStatus(ctx, wdr.ID, "processing", "", ""); err != nil {
		return err
	}

	log.Printf("[Settlement] Processing withdrawal %s: %.2f credits -> %s ETH to %s",
		wdr.ID, wdr.Amount, wdr.AmountWei, wdr.WalletAddress)

	// Send ETH transaction
	txHash, err := r.ethClient.SendETH(wdr.WalletAddress, wdr.AmountWei)
	if err != nil {
		r.storage.UpdateWithdrawalStatus(ctx, wdr.ID, "failed", "", err.Error())
		return fmt.Errorf("failed to send ETH: %w", err)
	}

	log.Printf("[Settlement] Transaction sent: %s (withdrawal: %s)", txHash, wdr.ID)

	// Update with tx hash
	if err := r.storage.UpdateWithdrawalStatus(ctx, wdr.ID, "processing", txHash, ""); err != nil {
		return err
	}

	// Wait for confirmation
	if err := r.ethClient.WaitForConfirmation(txHash, r.settlementConfirms); err != nil {
		r.storage.UpdateWithdrawalStatus(ctx, wdr.ID, "failed", txHash, err.Error())
		return fmt.Errorf("transaction failed: %w", err)
	}

	log.Printf("[Settlement] Transaction confirmed: %s (%d confirmations)", txHash, r.settlementConfirms)

	// Deduct balance
	newBalance := account.Balance - wdr.Amount
	if err := r.storage.SetBalance(ctx, wdr.PeerID, newBalance); err != nil {
		log.Printf("[Settlement] WARNING: Transaction sent but failed to update balance: %v", err)
		// Don't fail - transaction already confirmed
	}

	// Mark as completed
	if err := r.storage.UpdateWithdrawalStatus(ctx, wdr.ID, "completed", txHash, ""); err != nil {
		return err
	}

	log.Printf("[Settlement] ✅ Withdrawal completed: %s (%.2f credits withdrawn, new balance: %.2f)",
		wdr.ID, wdr.Amount, newBalance)

	return nil
}

// --- Blockchain Deposit (ETH -> credits) ---

// getDepositInfo returns information for depositing ETH
// GET /api/v1/deposit/info
func (r *RegistryNode) getDepositInfo(c *gin.Context) {
	if !r.depositEnabled || r.depositContract == "" {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "blockchain deposits not enabled"})
		return
	}

	// Calculate min deposit in ETH and wei
	minCredits := 1.0 // 1 credit minimum
	minETH := minCredits * r.settlementRate
	minWei := new(big.Float).Mul(big.NewFloat(minETH), big.NewFloat(1e18))
	minWeiInt, _ := minWei.Int(nil)

	info := common.DepositInfo{
		ContractAddress: r.depositContract,
		Chain:           r.settlementChain,
		Rate:            r.settlementRate,
		MinDeposit:      fmt.Sprintf("%.6f ETH", minETH),
		MinDepositWei:   minWeiInt.String(),
		Instructions:    fmt.Sprintf("Call deposit(peerId) on contract %s with ETH. Rate: %.6f ETH = 1 credit", r.depositContract, r.settlementRate),
	}

	c.JSON(http.StatusOK, info)
}

// getDepositHistory returns deposit history for a peer
// GET /api/v1/deposit/history/:peerId
func (r *RegistryNode) getDepositHistory(c *gin.Context) {
	if !r.depositEnabled {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "blockchain deposits not enabled"})
		return
	}

	if r.storage == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "deposits require Redis storage"})
		return
	}

	peerID := c.Param("peerId")
	if peerID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "peerId is required"})
		return
	}

	limitStr := c.DefaultQuery("limit", "50")
	limit, err := strconv.Atoi(limitStr)
	if err != nil || limit <= 0 {
		limit = 50
	}

	deposits, err := r.storage.GetDeposits(c.Request.Context(), peerID, limit)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	// Calculate total deposited
	totalDeposited := 0.0
	for _, d := range deposits {
		totalDeposited += d.AmountCredits
	}

	c.JSON(http.StatusOK, gin.H{
		"peer_id":         peerID,
		"deposits":        deposits,
		"count":           len(deposits),
		"total_deposited": totalDeposited,
	})
}

// depositWatcher monitors the deposit contract for new deposits
func (r *RegistryNode) depositWatcher() {
	ticker := time.NewTicker(15 * time.Second)
	defer ticker.Stop()

	log.Println("[Deposit] Watcher started")

	// Get initial block
	ctx := context.Background()
	lastBlock, err := r.storage.GetLastProcessedBlock(ctx)
	if err != nil {
		log.Printf("[Deposit] Warning: Failed to get last processed block: %v", err)
	}

	if lastBlock == 0 {
		// Start from current block - 100 (to catch recent deposits)
		currentBlock, err := r.ethClient.GetCurrentBlock(ctx)
		if err != nil {
			log.Printf("[Deposit] Warning: Failed to get current block: %v", err)
			lastBlock = 0
		} else if currentBlock > 100 {
			lastBlock = currentBlock - 100
		}
	}

	log.Printf("[Deposit] Starting from block %d", lastBlock)

	for range ticker.C {
		r.processDeposits(lastBlock, &lastBlock)
	}
}

// processDeposits fetches and processes new deposit events
func (r *RegistryNode) processDeposits(fromBlock uint64, lastBlock *uint64) {
	ctx := context.Background()

	// Get current block
	currentBlock, err := r.ethClient.GetCurrentBlock(ctx)
	if err != nil {
		log.Printf("[Deposit] Error getting current block: %v", err)
		return
	}

	// Don't process if no new blocks
	if currentBlock <= *lastBlock {
		return
	}

	// Process in chunks of 1000 blocks
	toBlock := *lastBlock + 1000
	if toBlock > currentBlock {
		toBlock = currentBlock
	}

	// Fetch deposit events
	events, err := r.ethClient.GetDepositEvents(ctx, r.depositContract, *lastBlock+1, toBlock)
	if err != nil {
		log.Printf("[Deposit] Error fetching events: %v", err)
		return
	}

	// Process each deposit
	for _, event := range events {
		// Check if already processed
		processed, err := r.storage.IsDepositProcessed(ctx, event.TxHash)
		if err != nil {
			log.Printf("[Deposit] Error checking tx %s: %v", event.TxHash, err)
			continue
		}
		if processed {
			continue
		}

		// Calculate credits: amountWei / (rate * 1e18)
		amountWeiFloat := new(big.Float).SetInt(event.AmountWei)
		rateWei := new(big.Float).Mul(big.NewFloat(r.settlementRate), big.NewFloat(1e18))
		creditsFloat := new(big.Float).Quo(amountWeiFloat, rateWei)
		credits, _ := creditsFloat.Float64()

		// Record deposit
		deposit, err := r.storage.RecordDeposit(
			ctx,
			event.TxHash,
			event.FromAddress,
			event.PeerID,
			event.AmountWei.String(),
			credits,
			event.BlockNumber,
		)
		if err != nil {
			log.Printf("[Deposit] Error recording deposit: %v", err)
			continue
		}

		log.Printf("[Deposit] Processed: %s -> %.2f credits for %s (tx: %s)",
			event.FromAddress[:12], credits, event.PeerID[:12], event.TxHash[:12])

		_ = deposit // silence unused warning
	}

	// Update last processed block
	*lastBlock = toBlock
	if err := r.storage.SetLastProcessedBlock(ctx, toBlock); err != nil {
		log.Printf("[Deposit] Warning: Failed to save last processed block: %v", err)
	}
}

// --- Embedding client (OpenAI-compatible) ---

type EmbeddingClient struct {
	APIKey  string
	Model   string
	BaseURL string
	HTTP    *http.Client
	Dim     int
}

type embeddingRequest struct {
	Input string `json:"input"`
	Model string `json:"model"`
}

type embeddingResponse struct {
	Data []struct {
		Embedding []float64 `json:"embedding"`
	} `json:"data"`
}

func NewEmbeddingClient(apiKey, model, baseURL string, dim int) *EmbeddingClient {
	return &EmbeddingClient{
		APIKey:  apiKey,
		Model:   model,
		BaseURL: strings.TrimRight(baseURL, "/"),
		HTTP:    &http.Client{Timeout: 10 * time.Second},
		Dim:     dim,
	}
}

func (ec *EmbeddingClient) EmbedText(ctx context.Context, text string) ([]float32, error) {
	if ec == nil {
		return nil, fmt.Errorf("embedding client not configured")
	}
	if strings.TrimSpace(text) == "" {
		return nil, fmt.Errorf("cannot embed empty text")
	}

	body := embeddingRequest{Input: text, Model: ec.Model}
	b, _ := json.Marshal(body)
	url := fmt.Sprintf("%s/embeddings", ec.BaseURL)

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(b))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+ec.APIKey)
	req.Header.Set("Content-Type", "application/json")

	resp, err := ec.HTTP.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		bodyBytes, _ := ioutil.ReadAll(resp.Body)
		return nil, fmt.Errorf("embedding request failed: status=%d body=%s", resp.StatusCode, string(bodyBytes))
	}

	var er embeddingResponse
	if err := json.NewDecoder(resp.Body).Decode(&er); err != nil {
		return nil, err
	}
	if len(er.Data) == 0 || len(er.Data[0].Embedding) == 0 {
		return nil, fmt.Errorf("embedding response empty")
	}

	vec64 := er.Data[0].Embedding

	if ec.Dim > 0 && len(vec64) != ec.Dim {
		return nil, fmt.Errorf("embedding length mismatch: got %d want %d", len(vec64), ec.Dim)
	}

	vec := make([]float32, len(vec64))
	for i, v := range vec64 {
		vec[i] = float32(v)
	}
	return vec, nil
}

// --- Minimal Qdrant HTTP client (for demo use only) ---

type QdrantClient struct {
	BaseURL    string
	Collection string
	HTTP       *http.Client
	VectorSize int
}

func NewQdrantClient(baseURL, collection string) *QdrantClient {
	return &QdrantClient{
		BaseURL:    strings.TrimRight(baseURL, "/"),
		Collection: collection,
		HTTP:       &http.Client{Timeout: 5 * time.Second},
	}
}

// ensureCollection creates the collection if it does not exist yet.
func (qc *QdrantClient) ensureCollection(dim int) error {
	if qc == nil {
		return nil
	}
	if qc.VectorSize != 0 {
		return nil
	}

	body := map[string]interface{}{
		"vectors": map[string]interface{}{
			"size":     dim,
			"distance": "Cosine",
		},
	}

	b, _ := json.Marshal(body)
	url := fmt.Sprintf("%s/collections/%s", qc.BaseURL, qc.Collection)

	req, err := http.NewRequest(http.MethodPut, url, bytes.NewReader(b))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := qc.HTTP.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	// 200 OK or 201 Created or 409 Already exists are fine
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusConflict {
		bodyBytes, _ := ioutil.ReadAll(resp.Body)
		return fmt.Errorf("qdrant create collection failed: status=%d body=%s", resp.StatusCode, string(bodyBytes))
	}

	qc.VectorSize = dim
	return nil
}

// UpsertService stores or updates a single service vector in Qdrant.
func (qc *QdrantClient) UpsertService(id string, vector []float32, payload map[string]interface{}) error {
	if qc == nil {
		return nil
	}
	if len(vector) == 0 {
		return nil
	}
	if err := qc.ensureCollection(len(vector)); err != nil {
		return err
	}

	// Qdrant in this config expects numeric or UUID IDs.
	// We hash the provided string into a uint64 for demo purposes.
	numID := hashToUint64(id)

	body := map[string]interface{}{
		"points": []map[string]interface{}{
			{
				"id":      numID,
				"vector":  vector,
				"payload": payload,
			},
		},
	}

	b, _ := json.Marshal(body)
	url := fmt.Sprintf("%s/collections/%s/points", qc.BaseURL, qc.Collection)

	req, err := http.NewRequest(http.MethodPut, url, bytes.NewReader(b))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := qc.HTTP.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusAccepted {
		bodyBytes, _ := ioutil.ReadAll(resp.Body)
		return fmt.Errorf("qdrant upsert failed: status=%d body=%s", resp.StatusCode, string(bodyBytes))
	}

	return nil
}

// hashToUint64 deterministically maps a string to a uint64.
func hashToUint64(s string) uint64 {
	sum := sha256.Sum256([]byte(s))
	var v uint64
	for i := 0; i < 8; i++ {
		v = (v << 8) | uint64(sum[i])
	}
	return v
}

type qdrantSearchResult struct {
	ID      interface{}            `json:"id"`
	Score   float64                `json:"score"`
	Payload map[string]interface{} `json:"payload"`
}

type qdrantSearchResponse struct {
	Result []qdrantSearchResult `json:"result"`
}

// RemoveService deletes a service vector from Qdrant.
func (qc *QdrantClient) RemoveService(id string) error {
	if qc == nil {
		return nil
	}

	// Hash the ID to match what was used in UpsertService
	numID := hashToUint64(id)

	body := map[string]interface{}{
		"points": []uint64{numID},
	}

	b, _ := json.Marshal(body)
	url := fmt.Sprintf("%s/collections/%s/points/delete", qc.BaseURL, qc.Collection)

	req, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(b))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := qc.HTTP.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusAccepted {
		bodyBytes, _ := ioutil.ReadAll(resp.Body)
		return fmt.Errorf("qdrant delete failed: status=%d body=%s", resp.StatusCode, string(bodyBytes))
	}

	return nil
}

// Search performs a vector similarity search in Qdrant.
func (qc *QdrantClient) Search(vector []float32, limit int) ([]qdrantSearchResult, error) {
	if qc == nil {
		return nil, fmt.Errorf("qdrant client not configured")
	}
	if len(vector) == 0 {
		return nil, fmt.Errorf("empty query vector")
	}
	if limit <= 0 {
		limit = 5
	}
	if err := qc.ensureCollection(len(vector)); err != nil {
		return nil, err
	}

	body := map[string]interface{}{
		"vector":       vector,
		"limit":        limit,
		"with_payload": true,
	}

	b, _ := json.Marshal(body)
	url := fmt.Sprintf("%s/collections/%s/points/search", qc.BaseURL, qc.Collection)

	req, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(b))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := qc.HTTP.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		bodyBytes, _ := ioutil.ReadAll(resp.Body)
		return nil, fmt.Errorf("qdrant search failed: status=%d body=%s", resp.StatusCode, string(bodyBytes))
	}

	var sr qdrantSearchResponse
	if err := json.NewDecoder(resp.Body).Decode(&sr); err != nil {
		return nil, err
	}
	return sr.Result, nil
}
