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
	"prxs/storage"
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

// RegistryNode holds the state of the marketplace
type RegistryNode struct {
	Host host.Host

	// Core state: PeerID -> active session
	Registrations map[peer.ID]*RegistrationRecord
	// Lookup index: ServiceName -> PeerIDs
	ServiceIndex map[string][]peer.ID

	mu sync.Mutex

	minStake          float64
	seenStakeNonces   map[string]bool            // Replay protection for stake nonces
	peerStakes        map[peer.ID][]string       // PeerID -> list of stake IDs
	freezedPeerStakes map[peer.ID][]freezedStake // PeerID -> list of frozen stakes
	freezedStakes     []freezedStake             // All frozen stakes for periodic cleanup
	stakeMu           sync.Mutex

	qdrant       *QdrantClient
	storage      *storage.RedisStorage
	embeddingDim int
	embedder     *EmbeddingClient
}

func main() {
	// Structured timestamps for all logs
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)

	port := flag.Int("port", 4001, "port")
	apiPort := flag.Int("api-port", 8080, "REST API port (default: 8080, avoid restricted ports like 6000)")
	bootstrap := flag.String("bootstrap", "", "bootstrap multiaddr")
	keyFile := flag.String("key", "", "path to key file (e.g. registry.key)")
	devMode := flag.Bool("dev", true, "Enable LAN/Dev mode")
	minStake := flag.Float64("min-stake", 10.0, "minimum stake required to register")
	qdrantEnabled := flag.Bool("qdrant-enabled", false, "enable Qdrant semantic index")
	qdrantURL := flag.String("qdrant-url", "http://localhost:6333", "Qdrant base URL")
	qdrantCollection := flag.String("qdrant-collection", "prxs_services", "Qdrant collection name")
	redisAddr := flag.String("redis", "", "Redis address (e.g., localhost:6379) - if set, registrations are stored in both memory and Redis")
	embeddingDim := flag.Int("embedding-dim", 1536, "Embedding dimension (e.g., 1536 for text-embedding-3-small)")
	embeddingModel := flag.String("embedding-model", "text-embedding-3-small", "Embedding model name (used for query embeddings)")
	embeddingBaseURL := flag.String("embedding-base-url", "https://api.openai.com/v1", "Embedding API base URL")
	embeddingAPIKey := flag.String("embedding-api-key", "", "Embedding API key (default: OPENAI_API_KEY env)")
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

	startRegistry(*port, *apiPort, *bootstrap, *devMode, *minStake, privKey, *qdrantURL, *qdrantCollection, *qdrantEnabled, *redisAddr, *embeddingDim, *embeddingModel, baseURL, key)
}

func startRegistry(port int, apiPort int, bootstrapAddr string, devMode bool, minStake float64, privKey crypto.PrivKey, qdrantURL, qdrantCollection string, qdrantEnabled bool, redisAddr string, embeddingDim int, embeddingModel, embeddingBaseURL, embeddingAPIKey string) {
	ctx := context.Background()

	h, err := libp2p.New(common.CommonLibp2pOptions(port, privKey)...)
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

	// Initialize Redis storage if address is provided
	redisStorage, err := storage.NewRedisStorage(redisAddr, 120*time.Second)
	if err != nil {
		log.Printf("[Reg] Warning: Failed to initialize Redis storage: %v", err)
		log.Printf("[Reg] Continuing without Redis persistence (in-memory only)")
		redisStorage = nil
	}

	reg := &RegistryNode{
		Host:              h,
		Registrations:     make(map[peer.ID]*RegistrationRecord),
		ServiceIndex:      make(map[string][]peer.ID),
		minStake:          minStake,
		seenStakeNonces:   make(map[string]bool),
		peerStakes:        make(map[peer.ID][]string),
		freezedPeerStakes: make(map[peer.ID][]freezedStake),
		freezedStakes:     make([]freezedStake, 0),
		qdrant:            qdrant,
		storage:           redisStorage,
		embeddingDim:      embeddingDim,
		embedder:          embedder,
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
				log.Printf("[Reg] Pruning dead provider: %s (last seen %s)\n", pid.ShortString(), record.LastSeen.Format(time.RFC3339))
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
				log.Printf("[Reg] Unfreezing stake %s for peer %s (frozen at %s)\n",
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
			log.Printf("[Reg] Unfroze %d stake(s)\n", unfrozenCount)

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
		log.Printf("[Reg] Restored %d active registrations", len(storageRecords))
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
		log.Printf("[Reg] Restored stakes for %d peers", len(peerStakes))
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
		log.Printf("[Reg] Restored freezed stakes for %d peers", len(convertedFreezedPeerStakes))
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
			log.Printf("[Reg] Restored %d global freezed stakes", len(fsList))
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
		return fmt.Errorf("stake proof required (min %.2f)", r.minStake)
	}

	if proof.Amount < r.minStake {
		return fmt.Errorf("stake too low: have %.2f need %.2f", proof.Amount, r.minStake)
	}

	if proof.Staker != "" && proof.Staker != remote.String() {
		return fmt.Errorf("stake staker mismatch (expected %s got %s)", remote.ShortString(), proof.Staker)
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
		// Decide whether this is a new registration or a heartbeat
		r.mu.Lock()
		existing, isRegistered := r.Registrations[remotePeer]
		r.mu.Unlock()

		isHeartbeat := false
		if isRegistered && existing.StakeProof != nil && req.StakeProof != nil &&
			existing.StakeProof.TxHash == req.StakeProof.TxHash {
			isHeartbeat = true
		}

		if isHeartbeat {
			// Heartbeat: update LastSeen and optionally AddrInfo
			r.mu.Lock()
			if entry, ok := r.Registrations[remotePeer]; ok {
				entry.LastSeen = time.Now()
				if req.ProviderInfo != nil {
					entry.AddrInfo = *req.ProviderInfo
				}
				log.Printf("[Reg] Heartbeat received: %s\n", remotePeer.ShortString())
				resp.Success = true

				// Save to Redis if enabled
				storageRecord := r.convertToStorageRecord(entry)
				if err := r.storage.SaveRegistration(context.Background(), remotePeer, storageRecord); err != nil {
					log.Printf("[Reg] Warning: Failed to save heartbeat to Redis: %v", err)
				}
			}
			r.mu.Unlock()
		} else {
			// New registration or stake changed
			if err := r.checkStakeValidity(remotePeer, req.StakeProof); err != nil {
				resp.Error = err.Error()
				log.Printf("[Reg] Stake Invalid: %v\n", err)
				break
			}

			// Replay protection: check if this stake is already used by this peer
			key := fmt.Sprintf("%s|%d", req.StakeProof.TxHash, req.StakeProof.Nonce)
			r.stakeMu.Lock()
			stakes := r.peerStakes[remotePeer]
			alreadyUsed := false
			for _, existingKey := range stakes {
				if existingKey == key {
					alreadyUsed = true
					break
				}
			}
			if alreadyUsed {
				r.stakeMu.Unlock()
				resp.Error = "stake proof already used (replay detected)"
				log.Printf("[Reg] Replay Attack: %s\n", resp.Error)
				break
			}
			r.peerStakes[remotePeer] = append(stakes, key)

			// Persist to Redis
			if err := r.storage.SavePeerStakes(context.Background(), remotePeer, r.peerStakes[remotePeer]); err != nil {
				log.Printf("[Reg] Warning: Failed to save peer stakes to Redis: %v", err)
			}

			r.stakeMu.Unlock()

			var embedding []float32
			if r.qdrant != nil {
				if err := r.validateEmbedding(req.Card.Embedding); err != nil {
					resp.Error = fmt.Sprintf("invalid embedding: %v", err)
					log.Printf("[Reg] Embedding invalid: %v\n", err)
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

			log.Printf("[Reg] New Registration: %s (Service: %s)\n", remotePeer.ShortString(), req.Card.Name)
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
		if req.StakeProof == nil {
			resp.Error = "stake proof required for unregister"
			log.Printf("[Reg] Unregister failed: no stake proof provided\n")
			break
		}

		// Get the stake key from the provided StakeProof
		stakeKey := fmt.Sprintf("%s|%d", req.StakeProof.TxHash, req.StakeProof.Nonce)

		r.stakeMu.Lock()

		// Find and remove the stake from peerStakes
		stakes, exists := r.peerStakes[remotePeer]
		if !exists {
			r.stakeMu.Unlock()
			resp.Error = "no stakes found for peer"
			log.Printf("[Reg] Unregister failed: no stakes for %s\n", remotePeer.ShortString())
			break
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
			log.Printf("[Reg] Unregister failed: stake %s not found for %s\n", stakeKey, remotePeer.ShortString())
			break
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

			log.Printf("[Reg] Removed service %s for peer %s\n", serviceName, remotePeer.ShortString())
		}
		r.mu.Unlock()

		resp.Success = true
		log.Printf("[Reg] Unregistered stake %s for %s: frozen until %s\n",
			stakeKey, remotePeer.ShortString(), time.Unix(now+UNFREEZE_DELAY, 0).Format(time.RFC3339))

	default:
		resp.Error = "Unknown method"
	}

	_ = json.NewEncoder(rw).Encode(resp)
	_ = rw.Flush()
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
