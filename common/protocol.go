package common

import (
	"fmt"
	"strings"

	"github.com/libp2p/go-libp2p/core/peer"
)

const (
	// ProtocolID is the p2p protocol used for Client <-> Provider execution
	ProtocolID = "/prxs/rpc/1.0"

	// RegistryProtocolID is the p2p protocol used for Node <-> Registry interactions
	RegistryProtocolID = "/prxs/registry-rpc/1.0"

	// RegistryRendezvous is the DHT Key used ONLY to find the Registry Node.
	// Nodes do NOT advertise services here. They only look for the Registry.
	RegistryRendezvous = "prxs.infra.registry"
)

// --- Data Models ---

type ServiceCard struct {
	Name        string    `json:"name"`
	Description string    `json:"description"`
	Inputs      []string  `json:"inputs"`      // e.g. ["prompt", "style"]
	CostPerOp   float64   `json:"cost_per_op"` // Fake tokens
	Version     string    `json:"version"`
	Tags        []string  `json:"tags,omitempty"`      // Categories / labels
	Embedding   []float32 `json:"embedding,omitempty"` // Optional vector for semantic search
}

// PaymentTicket is an off-chain receipt signed by the client to pay a provider.
type PaymentTicket struct {
	ClientID     string  `json:"client_id"`
	ProviderID   string  `json:"provider_id"`
	Amount       float64 `json:"amount"`
	Nonce        int64   `json:"nonce"`
	Signature    []byte  `json:"signature"`
	ClientPubKey []byte  `json:"client_pubkey"`
}

// --- Credits System (Agent-to-Agent Payments) ---

// CreditAccount represents an agent's credit balance
type CreditAccount struct {
	PeerID    string  `json:"peer_id"`
	Balance   float64 `json:"balance"`
	UpdatedAt int64   `json:"updated_at"`
}

// CreditTransaction represents a payment between agents
type CreditTransaction struct {
	ID        string  `json:"id"`
	From      string  `json:"from"`    // payer peer ID
	To        string  `json:"to"`      // receiver peer ID
	Amount    float64 `json:"amount"`
	Service   string  `json:"service"` // service name
	Timestamp int64   `json:"timestamp"`
}

// CreditDepositRequest is used to deposit credits via REST API
type CreditDepositRequest struct {
	PeerID string  `json:"peer_id"`
	Amount float64 `json:"amount"`
}

// CreditChargeRequest is used by providers to charge clients
type CreditChargeRequest struct {
	FromPeer string  `json:"from_peer"` // client peer ID
	ToPeer   string  `json:"to_peer"`   // provider peer ID
	Amount   float64 `json:"amount"`
	Service  string  `json:"service"`
}

// --- Settlement System (Withdraw Credits to Blockchain) ---

// WithdrawalRequest represents a request to withdraw credits to blockchain
type WithdrawalRequest struct {
	ID            string  `json:"id"`              // wdr-timestamp-shortPeerID
	PeerID        string  `json:"peer_id"`         // who is withdrawing
	Amount        float64 `json:"amount"`          // credits to withdraw
	AmountWei     string  `json:"amount_wei"`      // ETH amount in wei (string for precision)
	WalletAddress string  `json:"wallet_address"`  // destination address
	Chain         string  `json:"chain"`           // blockchain (e.g., "sepolia")
	Status        string  `json:"status"`          // pending/processing/completed/failed
	TxHash        string  `json:"tx_hash"`         // blockchain transaction hash
	CreatedAt     int64   `json:"created_at"`
	UpdatedAt     int64   `json:"updated_at"`
	ErrorMessage  string  `json:"error_message,omitempty"`
}

// WithdrawRequest is the API request to initiate withdrawal
type WithdrawRequest struct {
	PeerID        string  `json:"peer_id"`
	Amount        float64 `json:"amount"`
	WalletAddress string  `json:"wallet_address"`
	Chain         string  `json:"chain"` // must be "sepolia"
}

// --- Blockchain Deposit System ---

// DepositRecord represents a processed deposit from blockchain
type DepositRecord struct {
	ID            string  `json:"id"`             // dep-timestamp-txHash[:10]
	TxHash        string  `json:"tx_hash"`        // blockchain transaction hash
	FromAddress   string  `json:"from_address"`   // wallet that sent ETH
	PeerID        string  `json:"peer_id"`        // peer to credit
	AmountWei     string  `json:"amount_wei"`     // ETH amount in wei
	AmountCredits float64 `json:"amount_credits"` // credits awarded
	BlockNumber   uint64  `json:"block_number"`
	Timestamp     int64   `json:"timestamp"`
}

// DepositInfo is returned by GET /api/v1/deposit/info
type DepositInfo struct {
	ContractAddress string  `json:"contract_address"`
	Chain           string  `json:"chain"`
	Rate            float64 `json:"rate"`         // ETH per credit
	MinDeposit      string  `json:"min_deposit"`  // minimum in ETH
	MinDepositWei   string  `json:"min_deposit_wei"`
	Instructions    string  `json:"instructions"`
}

type StakeProofMode string

const (
	StakeProofModeMock StakeProofMode = "mock"
	StakeProofModeEVM  StakeProofMode = "evm"
)

type StakeProof struct {
	Mode StakeProofMode `json:"mode,omitempty"`

	TxHash    string  `json:"tx_hash,omitempty"`
	Staker    string  `json:"staker"`
	Amount    float64 `json:"amount,omitempty"`
	Nonce     int64   `json:"nonce,omitempty"`
	Timestamp int64   `json:"timestamp,omitempty"`
	ChainID   string  `json:"chain_id,omitempty"`
	Signature []byte  `json:"signature,omitempty"`

	WalletAddress   string `json:"wallet_address,omitempty"`
	WalletSignature []byte `json:"wallet_signature,omitempty"`
	WalletNonce     int64  `json:"wallet_nonce,omitempty"`
	WalletIssuedAt  int64  `json:"wallet_issued_at,omitempty"`
	WalletExpiresAt int64  `json:"wallet_expires_at,omitempty"`
	EvmChainID      int64  `json:"evm_chain_id,omitempty"`
	StakingContract string `json:"staking_contract,omitempty"`
}

const WalletBindingMessageTemplate = "PRXS Peer Binding\nPeer ID: {{peer_id}}\nChain ID: {{chain_id}}\nContract: {{contract}}\nNonce: {{nonce}}\nIssued: {{issued}}\nExpires: {{expires}}"

func (sp *StakeProof) IsMockMode() bool {
	return sp.Mode == "" || sp.Mode == StakeProofModeMock
}

func (sp *StakeProof) IsEVMMode() bool {
	return sp.Mode == StakeProofModeEVM
}

func BuildWalletBindingMessage(peerID string, chainID int64, contract string, nonce int64, issuedAt int64, expiresAt int64) string {
	expiresStr := "never"
	if expiresAt > 0 {
		expiresStr = fmt.Sprintf("%d", expiresAt)
	}

	message := WalletBindingMessageTemplate
	message = strings.ReplaceAll(message, "{{peer_id}}", peerID)
	message = strings.ReplaceAll(message, "{{chain_id}}", fmt.Sprintf("%d", chainID))
	message = strings.ReplaceAll(message, "{{contract}}", contract)
	message = strings.ReplaceAll(message, "{{nonce}}", fmt.Sprintf("%d", nonce))
	message = strings.ReplaceAll(message, "{{issued}}", fmt.Sprintf("%d", issuedAt))
	message = strings.ReplaceAll(message, "{{expires}}", expiresStr)
	return message
}

func (sp *StakeProof) GetWalletBindingMessage() string {
	return BuildWalletBindingMessage(
		sp.Staker,
		sp.EvmChainID,
		sp.StakingContract,
		sp.WalletNonce,
		sp.WalletIssuedAt,
		sp.WalletExpiresAt,
	)
}

// --- Registry RPC (Node <-> Registry) ---

type RegistryRequest struct {
	Method     string      `json:"method"` // "register" or "find"
	Card       ServiceCard `json:"card,omitempty"`
	Query      string      `json:"query,omitempty"`
	StakeProof *StakeProof `json:"stake_proof,omitempty"`
	// Providers send their own address info so the Registry can tell Clients how to connect
	ProviderInfo *peer.AddrInfo `json:"provider_info,omitempty"`
}

type RegistryResponse struct {
	Success   bool            `json:"success"`
	Providers []peer.AddrInfo `json:"providers,omitempty"`
	Error     string          `json:"error,omitempty"`
}

// --- Execution RPC (Client <-> Provider) ---

type JSONRPCRequest struct {
	Method string      `json:"method"`
	Params interface{} `json:"params"`
	ID     int         `json:"id"`
}

type JSONRPCResponse struct {
	Result interface{} `json:"result,omitempty"`
	Error  string      `json:"error,omitempty"`
	ID     int         `json:"id"`
}
