package main

import (
	"bufio"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/json"
	"flag"
	"fmt"
	"html/template"
	"log"
	"net/http"
	"os"
	"os/exec"
	"sync"
	"time"

	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/p2p/discovery/routing"

	"prxs/common"
	"prxs/mcp"
)

func init() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
}

// --- Agent Management ---

type ProviderDaemon struct {
	agentCmd     *exec.Cmd
	agentEncoder *json.Encoder
	agentDecoder *json.Decoder
	mu           sync.Mutex
	Card         common.ServiceCard
}

func buildStakeProof(priv crypto.PrivKey, amount float64, chainID string) (*common.StakeProof, error) {
	nonce := time.Now().UnixNano()
	txHash := fmt.Sprintf("mock-tx-%x", nonce)
	timestamp := time.Now().Unix()

	pid, err := peer.IDFromPrivateKey(priv)
	if err != nil {
		return nil, fmt.Errorf("failed to derive peer ID: %v", err)
	}

	payload := fmt.Sprintf("%s|%f|%d|%d|%s", txHash, amount, nonce, timestamp, chainID)
	digest := sha256.Sum256([]byte(payload))

	signature, err := priv.Sign(digest[:])
	if err != nil {
		return nil, fmt.Errorf("failed to sign stake proof: %v", err)
	}

	return &common.StakeProof{
		TxHash:    txHash,
		Staker:    pid.String(),
		Amount:    amount,
		Nonce:     nonce,
		Timestamp: timestamp,
		ChainID:   chainID,
		Signature: signature,
	}, nil
}

func loadStakeProofFromFile(path string, priv crypto.PrivKey, chainID string) (*common.StakeProof, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}

	var proof common.StakeProof
	if err := json.Unmarshal(data, &proof); err != nil {
		return nil, fmt.Errorf("failed to parse stake proof: %v", err)
	}

	if chainID != "" && proof.ChainID != "" && chainID != proof.ChainID {
		return nil, fmt.Errorf("stake proof chain mismatch (got %s want %s)", proof.ChainID, chainID)
	}

	pub := priv.GetPublic()
	payload := fmt.Sprintf("%s|%f|%d|%d|%s", proof.TxHash, proof.Amount, proof.Nonce, proof.Timestamp, proof.ChainID)
	digest := sha256.Sum256([]byte(payload))
	ok, err := pub.Verify(digest[:], proof.Signature)
	if err != nil || !ok {
		return nil, fmt.Errorf("stake proof signature invalid")
	}
	return &proof, nil
}

func openBrowser(url string) {
	cmd := exec.Command("open", url)
	if err := cmd.Start(); err == nil {
		return
	}
	cmd = exec.Command("xdg-open", url)
	_ = cmd.Start()
}

func runStakingHelper(ctx context.Context, proofPath string, amount float64, chainID string, address string, webPort int, priv crypto.PrivKey) (*common.StakeProof, error) {
	fmt.Printf("[Prov] Staking required. Visit http://127.0.0.1:%d/stake to stake %.2f tokens (chain=%s)\n", webPort, amount, chainID)

	type pageData struct {
		Address   string
		Amount    float64
		ChainID   string
		ProofPath string
	}

	tmpl := template.Must(template.New("stake").Parse(`
<!doctype html>
<html>
<head><title>Stake to Register Service</title></head>
<body style="font-family: sans-serif; max-width: 480px; margin: 2rem auto;">
  <h2>Stake required</h2>
  <p>Send <strong>{{printf "%.2f" .Amount}}</strong> tokens on <strong>{{.ChainID}}</strong> to:</p>
  <pre style="background:#f4f4f5;padding:12px;border-radius:8px;word-break:break-all;">{{.Address}}</pre>
  <div id="qrcode" style="margin:12px 0;"></div>
  <form method="POST" action="/stake">
    <button type="submit" style="padding:10px 16px;border:none;border-radius:8px;background:#2563eb;color:white;">I sent the stake</button>
  </form>
  <p>Stake proof will be saved to <code>{{.ProofPath}}</code> and the node will register automatically.</p>
  <script src="https://cdnjs.cloudflare.com/ajax/libs/qrcodejs/1.0.0/qrcode.min.js"></script>
  <script>
    new QRCode(document.getElementById("qrcode"), { text: "{{.Address}}", width: 200, height: 200 });
  </script>
</body>
</html>
`))

	proofCh := make(chan *common.StakeProof, 1)
	srv := &http.Server{Addr: fmt.Sprintf(":%d", webPort)}

	mux := http.NewServeMux()
	mux.HandleFunc("/stake", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			_ = tmpl.Execute(w, pageData{Address: address, Amount: amount, ChainID: chainID, ProofPath: proofPath})
		case http.MethodPost:
			proof, err := buildStakeProof(priv, amount, chainID)
			if err != nil {
				http.Error(w, "failed to build stake proof", http.StatusInternalServerError)
				return
			}
			b, _ := json.MarshalIndent(proof, "", "  ")
	if err := os.WriteFile(proofPath, b, 0600); err != nil {
		http.Error(w, "failed to save stake proof", http.StatusInternalServerError)
		return
	}
	select {
	case proofCh <- proof:
	default:
	}
	fmt.Fprintf(w, "<p>Stake proof saved. You can close this tab.</p>")
	go srv.Shutdown(context.Background())
		default:
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		}
	})

	srv.Handler = mux

	go func() {
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			fmt.Printf("[Prov] Staking helper server error: %v\n", err)
		}
	}()
	go func() {
		<-ctx.Done()
		_ = srv.Shutdown(context.Background())
	}()

	openBrowser(fmt.Sprintf("http://127.0.0.1:%d/stake", webPort))

	select {
	case proof := <-proofCh:
		return proof, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func NewProviderDaemon(agentPath string) (*ProviderDaemon, error) {
	log.Printf("[Daemon] Launching agent script: %s\n", agentPath)

	// Try python first (Windows), then python3
	pythonCmd := "python"
	if _, err := exec.LookPath("python"); err != nil {
		// Python not found, try python3 (Unix systems)
		if _, err := exec.LookPath("python3"); err != nil {
			return nil, fmt.Errorf("neither 'python' nor 'python3' found in PATH")
		}
		pythonCmd = "python3"
	}
	log.Printf("[Daemon] Using Python command: %s", pythonCmd)
	cmd := exec.Command(pythonCmd, "-u", agentPath)

	stdin, err := cmd.StdinPipe()
	if err != nil {
		return nil, err
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, err
	}

	cmd.Stderr = os.Stderr

	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("failed to start python process: %v", err)
	}

	pd := &ProviderDaemon{
		agentCmd:     cmd,
		agentEncoder: json.NewEncoder(stdin),
		agentDecoder: json.NewDecoder(stdout),
	}

	// --- HANDSHAKE ---
	log.Println("[Daemon] Sending 'initialize' handshake...")

	initReq := common.JSONRPCRequest{Method: "initialize", ID: 0}
	if err := pd.agentEncoder.Encode(initReq); err != nil {
		return nil, fmt.Errorf("failed to write to agent stdin: %v", err)
	}

	var resp common.JSONRPCResponse
	if err := pd.agentDecoder.Decode(&resp); err != nil {
		return nil, fmt.Errorf("agent handshake failed (did the script crash?): %v", err)
	}

	cardBytes, _ := json.Marshal(resp.Result)
	json.Unmarshal(cardBytes, &pd.Card)

	log.Printf("[Daemon] Handshake complete. Service: %s\n", pd.Card.Name)

	return pd, nil
}

func (pd *ProviderDaemon) HandleExecutionStream(stream network.Stream) {
	defer stream.Close()
	rw := bufio.NewReadWriter(bufio.NewReader(stream), bufio.NewWriter(stream))
	var req common.JSONRPCRequest
	if err := json.NewDecoder(rw).Decode(&req); err != nil {
		return
	}

	fmt.Printf("[Daemon] Executing %s...\n", req.Method)

	pd.mu.Lock()
	pd.agentEncoder.Encode(req)
	var resp common.JSONRPCResponse
	pd.agentDecoder.Decode(&resp)
	pd.mu.Unlock()

	json.NewEncoder(rw).Encode(resp)
	rw.Flush()
}

// --- Provider Logic ---

func startProvider(port int, agentPath string, bootstrapAddr string, devMode bool, stakeAmount float64, stakeChain string, stakeProofPath string, stakeWebPort int, stakeAddress string, privKey crypto.PrivKey) {
	ctx := context.Background()

	h, err := libp2p.New(common.CommonLibp2pOptions(port, privKey)...)
	if err != nil {
		log.Fatal(err)
	}
	defer h.Close()

	daemon, err := NewProviderDaemon(agentPath)
	if err != nil {
		log.Fatalf("Failed to start agent: %v", err)
	}
	defer daemon.agentCmd.Process.Kill()

	// Ensure stake proof exists (load or guide user)
	stakeProof, err := loadStakeProofFromFile(stakeProofPath, privKey, stakeChain)
	if err != nil {
		log.Fatalf("Failed to load stake proof: %v", err)
	}
	if stakeProof == nil {
		stakeProof, err = runStakingHelper(ctx, stakeProofPath, stakeAmount, stakeChain, stakeAddress, stakeWebPort, privKey)
		if err != nil {
			log.Fatalf("Staking helper failed: %v", err)
		}
		fmt.Printf("[Prov] Stake proof saved to %s\n", stakeProofPath)
	}
	fmt.Printf("[Prov] Using stake proof tx=%s amount=%.2f chain=%s\n", stakeProof.TxHash, stakeProof.Amount, stakeProof.ChainID)

	log.Printf("PROVIDER ONLINE: %s (ID: %s)\n", daemon.Card.Name, h.ID().ShortString())

	h.SetStreamHandler(common.ProtocolID, daemon.HandleExecutionStream)

	kademliaDHT, _ := common.SetupDHT(ctx, h, []string{bootstrapAddr}, devMode)

	// Registration Loop
	go func() {
		rd := routing.NewRoutingDiscovery(kademliaDHT)

		for {
			fmt.Println("[Prov] ----------------------------------------")
			fmt.Println("[Prov] Attempting Registration...")

			candidatePeers := make(map[peer.ID]peer.AddrInfo)

			for _, pid := range h.Network().Peers() {
				candidatePeers[pid] = h.Peerstore().PeerInfo(pid)
			}

			ctxSearch, cancel := context.WithTimeout(ctx, 5*time.Second)
			dhtPeers, _ := rd.FindPeers(ctxSearch, common.RegistryRendezvous)
			for p := range dhtPeers {
				candidatePeers[p.ID] = p
			}
			cancel()

			if len(candidatePeers) == 0 {
				log.Println("[Prov] Warning: No peers connected or found. Check bootstrap address.")
			}

			registered := false
			for _, p := range candidatePeers {
				if p.ID == h.ID() {
					continue
				}

				s, err := h.NewStream(ctx, p.ID, common.RegistryProtocolID)
				if err != nil {
					continue
				}

				log.Printf("[Prov] Found Registry Candidate: %s\n", p.ID.ShortString())

				myself := peer.AddrInfo{
					ID:    h.ID(),
					Addrs: h.Addrs(),
				}

				req := common.RegistryRequest{
					Method:       "register",
					Card:         daemon.Card,
					ProviderInfo: &myself,
					StakeProof:   stakeProof,
				}

				rw := bufio.NewReadWriter(bufio.NewReader(s), bufio.NewWriter(s))
				if err := json.NewEncoder(rw).Encode(req); err != nil {
					s.Close()
					continue
				}
				rw.Flush()

				var resp common.RegistryResponse
				if err := json.NewDecoder(rw).Decode(&resp); err != nil {
					s.Close()
					continue
				}
				s.Close()

				if resp.Success {
					log.Printf("[Prov] ✅ SUCCESS: Registered with %s\n", p.ID.ShortString())
					registered = true
					break
				}
			}

			if !registered {
				log.Println("[Prov] ❌ Failed to register. Retrying in 10s...")
			} else {
				log.Println("[Prov] Registration checks pass. Sleeping 30s...")
			}

			time.Sleep(30 * time.Second)
		}
	}()

	select {}
}

// --- Client Logic ---

func startClient(bootstrapAddr string, query string, args string, devMode bool, privKey crypto.PrivKey) {
	ctx := context.Background()
	h, _ := libp2p.New(common.CommonLibp2pOptions(0, privKey)...)
	defer h.Close()

	kademliaDHT, _ := common.SetupDHT(ctx, h, []string{bootstrapAddr}, devMode)
	rd := routing.NewRoutingDiscovery(kademliaDHT)

	fmt.Println("CLIENT ONLINE.")
	fmt.Printf("1. Searching for Registry (Bootstrap: %s)...\n", bootstrapAddr)

	var registryPeer peer.ID
	found := false

	// Check connected peers first
	currentPeers := h.Network().Peers()
	fmt.Printf("   [Debug] Currently connected to %d peers. Checking them...\n", len(currentPeers))

	for _, pid := range currentPeers {
		s, err := h.NewStream(ctx, pid, common.RegistryProtocolID)
		if err == nil {
			fmt.Printf("   [FastPath] Found Registry via direct connection: %s\n", pid.ShortString())
			registryPeer = pid
			found = true
			s.Close()
			break
		}
	}

	// If not found locally, search the DHT
	if !found {
		fmt.Println("   [DHT] Asking network for Registry providers...")
		ctxSearch, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()

		registryChan, _ := rd.FindPeers(ctxSearch, common.RegistryRendezvous)

		for p := range registryChan {
			if p.ID == h.ID() {
				continue
			}

			if h.Network().Connectedness(p.ID) != network.Connected {
				if err := h.Connect(ctx, p); err != nil {
					continue
				}
			}

			s, err := h.NewStream(ctx, p.ID, common.RegistryProtocolID)
			if err == nil {
				fmt.Printf("   [DHT] Found Registry: %s\n", p.ID.ShortString())
				registryPeer = p.ID
				found = true
				s.Close()
				break
			}
		}
	}

	if !found {
		log.Fatal("Could not find a Registry Node. (Ensure Registry is running and Bootstrap address is correct)")
	}

	// Query Registry
	log.Printf("2. Asking Registry for service: '%s'...\n", query)

	stream, err := h.NewStream(ctx, registryPeer, common.RegistryProtocolID)
	if err != nil {
		log.Fatal(err)
	}

	req := common.RegistryRequest{Method: "find", Query: query}
	rw := bufio.NewReadWriter(bufio.NewReader(stream), bufio.NewWriter(stream))
	json.NewEncoder(rw).Encode(req)
	rw.Flush()

	var resp common.RegistryResponse
	json.NewDecoder(rw).Decode(&resp)
	stream.Close()

	if len(resp.Providers) == 0 {
		log.Fatalf("Registry returned 0 providers for '%s'", query)
	}

	target := resp.Providers[0]
	log.Printf(" > Registry suggested Provider: %s\n", target.ID.ShortString())

	// Connect to Provider
	log.Println("3. Connecting to Provider...")

	h.Peerstore().AddAddrs(target.ID, target.Addrs, time.Hour)

	if err := h.Connect(ctx, target); err != nil {
		log.Fatalf("Failed to connect to provider: %v", err)
	}

	// Execute RPC
	s, err := h.NewStream(ctx, target.ID, common.ProtocolID)
	if err != nil {
		log.Fatal(err)
	}

	log.Println("4. Sending Computation Request...")
	var payload interface{} = args
	if err := json.Unmarshal([]byte(args), &payload); err == nil {
		// parsed successfully
	}

	execReq := common.JSONRPCRequest{Method: "compute", Params: payload, ID: 1}

	rwExec := bufio.NewReadWriter(bufio.NewReader(s), bufio.NewWriter(s))
	json.NewEncoder(rwExec).Encode(execReq)
	rwExec.Flush()

	var execResp common.JSONRPCResponse
	if err := json.NewDecoder(rwExec).Decode(&execResp); err != nil {
		log.Fatalf("Failed to decode response: %v", err)
	}

	fmt.Printf("\n--- RESULT ---\n%v\n--------------\n", execResp.Result)
}

// --- MCP Server Logic ---

func startMCPServer(configPath string, bootstrapAddr string, devMode bool, privKey crypto.PrivKey) {
	ctx := context.Background()

	// Load MCP configuration
	config, err := mcp.LoadConfig(configPath)
	if err != nil {
		log.Fatalf("[MCP] Failed to load config: %v", err)
	}

	// Create libp2p host (as client)
	h, err := libp2p.New(common.CommonLibp2pOptions(0, privKey)...)
	if err != nil {
		log.Fatal(err)
	}
	defer h.Close()

	// Setup DHT
	kademliaDHT, err := common.SetupDHT(ctx, h, []string{bootstrapAddr}, devMode)
	if err != nil {
		log.Fatal(err)
	}

	log.Printf("[MCP] Node ID: %s", h.ID().ShortString())

	// Create and start MCP server
	mcpServer := mcp.NewServer(config, h, kademliaDHT)
	if err := mcpServer.Start(ctx); err != nil {
		log.Fatalf("[MCP] Server error: %v", err)
	}
}

// --- Main Entry Point ---

func main() {
	mode := flag.String("mode", "provider", "provider, client, or mcp-server")
	port := flag.Int("port", 4001, "port")
	bootstrap := flag.String("bootstrap", "", "bootstrap multiaddr")
	agent := flag.String("agent", "./calc.py", "agent binary")
	query := flag.String("query", "math", "service query (client only)")
	args := flag.String("args", "16", "rpc arguments (client only)")
	keyFile := flag.String("key", "", "path to key file (e.g. node.key)")
	devMode := flag.Bool("dev", true, "Enable LAN/Dev mode")
	stakeAmount := flag.Float64("stake-amount", 10.0, "mock stake amount (provider only)")
	stakeChain := flag.String("stake-chain", "mock-l2", "mock chain id for staking (provider only)")
	stakeProofPath := flag.String("stake-proof", "stake_proof.json", "path to stake proof file (provider only)")
	stakeWebPort := flag.Int("stake-web-port", 8090, "port for local staking helper UI (provider only)")
	stakeAddress := flag.String("stake-address", "0xDEADBEEF00000000000000000000000000DEMO", "display address for staking UI (provider only)")
	mcpConfig := flag.String("mcp-config", "mcp_config.yaml", "path to MCP config file (mcp-server only)")
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

	switch *mode {
	case "provider":
		if *bootstrap == "" {
			log.Fatal("Need -bootstrap")
		}
		startProvider(*port, *agent, *bootstrap, *devMode, *stakeAmount, *stakeChain, *stakeProofPath, *stakeWebPort, *stakeAddress, privKey)
	case "client":
		if *bootstrap == "" {
			log.Fatal("Need -bootstrap")
		}
		startClient(*bootstrap, *query, *args, *devMode, privKey)
	case "mcp-server":
		if *bootstrap == "" {
			log.Fatal("Need -bootstrap")
		}
		startMCPServer(*mcpConfig, *bootstrap, *devMode, privKey)
	default:
		log.Fatalf("Invalid mode: %s. Use 'provider', 'client', or 'mcp-server'", *mode)
	}
}
