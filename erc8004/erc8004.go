package erc8004

import (
	"context"
	"crypto/ecdsa"
	"fmt"
	"math/big"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/ethclient"
)

// Config contains ERC-8004 configuration
type Config struct {
	RPCEndpoint            string
	IdentityRegistryAddr   common.Address
	ReputationRegistryAddr common.Address
	ValidationRegistryAddr common.Address
	PrivateKey             *ecdsa.PrivateKey
	ChainID                *big.Int
}

// Client wraps Ethereum client and ERC-8004 contracts
type Client struct {
	ethClient              *ethclient.Client
	identityRegistryAddr   common.Address
	reputationRegistryAddr common.Address
	validationRegistryAddr common.Address
	identityABI            abi.ABI
	reputationABI          abi.ABI
	validationABI          abi.ABI
	privateKey             *ecdsa.PrivateKey
	chainID                *big.Int
}

// NewClient creates a new ERC-8004 client
func NewClient(cfg Config) (*Client, error) {
	// Connect to Ethereum node
	ethClient, err := ethclient.Dial(cfg.RPCEndpoint)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to Ethereum node: %w", err)
	}

	// Load ABIs
	identityABI, err := loadIdentityABI()
	if err != nil {
		return nil, fmt.Errorf("failed to load identity ABI: %w", err)
	}

	reputationABI, err := loadReputationABI()
	if err != nil {
		return nil, fmt.Errorf("failed to load reputation ABI: %w", err)
	}

	validationABI, err := loadValidationABI()
	if err != nil {
		return nil, fmt.Errorf("failed to load validation ABI: %w", err)
	}

	return &Client{
		ethClient:              ethClient,
		identityRegistryAddr:   cfg.IdentityRegistryAddr,
		reputationRegistryAddr: cfg.ReputationRegistryAddr,
		validationRegistryAddr: cfg.ValidationRegistryAddr,
		identityABI:            identityABI,
		reputationABI:          reputationABI,
		validationABI:          validationABI,
		privateKey:             cfg.PrivateKey,
		chainID:                cfg.ChainID,
	}, nil
}

// RegisterProvider registers a PRXS provider on-chain
func (c *Client) RegisterProvider(ctx context.Context, peerID, agentURI string, paymentWallet common.Address) (*big.Int, *types.Transaction, error) {
	// Pack function call
	data, err := c.identityABI.Pack("registerProvider", peerID, agentURI, paymentWallet)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to pack registerProvider: %w", err)
	}

	// Create transaction
	tx, err := c.sendTransaction(ctx, c.identityRegistryAddr, data)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to send transaction: %w", err)
	}

	// Wait for transaction receipt
	receipt, err := c.waitForReceipt(ctx, tx.Hash())
	if err != nil {
		return nil, tx, fmt.Errorf("transaction failed: %w", err)
	}

	// Parse logs to get agentId
	// Event signature: AgentRegistered(uint256 indexed agentId, address indexed owner, string agentURI)
	// topics[0] = event signature hash
	// topics[1] = agentId (indexed)
	// topics[2] = owner (indexed)
	// data = agentURI (non-indexed)
	agentRegisteredSig := c.identityABI.Events["AgentRegistered"].ID
	for _, log := range receipt.Logs {
		if log.Address != c.identityRegistryAddr {
			continue
		}

		// Check if this is AgentRegistered event (at least 3 topics: signature + 2 indexed params)
		if len(log.Topics) >= 3 && log.Topics[0] == agentRegisteredSig {
			// agentId is in topics[1] (first indexed parameter)
			agentId := new(big.Int).SetBytes(log.Topics[1].Bytes())
			return agentId, tx, nil
		}
	}

	return nil, tx, fmt.Errorf("agentId not found in receipt")
}

// GetAgentIdByPeerID retrieves agentId by peerID
func (c *Client) GetAgentIdByPeerID(ctx context.Context, peerID string) (*big.Int, error) {
	// Pack function call
	data, err := c.identityABI.Pack("getAgentIdByPeerID", peerID)
	if err != nil {
		return nil, fmt.Errorf("failed to pack getAgentIdByPeerID: %w", err)
	}

	// Call contract
	result, err := c.ethClient.CallContract(ctx, ethereum.CallMsg{
		To:   &c.identityRegistryAddr,
		Data: data,
	}, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to call contract: %w", err)
	}

	// Unpack result
	agentId, err := unpackAgentIDResult(c.identityABI, result)
	if err != nil {
		return nil, fmt.Errorf("failed to unpack result: %w", err)
	}
	return agentId, nil
}

// OwnerOf returns the current owner of agentId (ERC-721 tokenId).
func (c *Client) OwnerOf(ctx context.Context, agentId *big.Int) (common.Address, error) {
	data, err := c.identityABI.Pack("ownerOf", agentId)
	if err != nil {
		return common.Address{}, fmt.Errorf("failed to pack ownerOf: %w", err)
	}

	result, err := c.ethClient.CallContract(ctx, ethereum.CallMsg{
		To:   &c.identityRegistryAddr,
		Data: data,
	}, nil)
	if err != nil {
		return common.Address{}, fmt.Errorf("failed to call contract: %w", err)
	}

	out, err := c.identityABI.Unpack("ownerOf", result)
	if err != nil {
		return common.Address{}, fmt.Errorf("failed to unpack result: %w", err)
	}
	if len(out) != 1 {
		return common.Address{}, fmt.Errorf("unexpected result arity: %d", len(out))
	}
	owner, ok := out[0].(common.Address)
	if !ok {
		return common.Address{}, fmt.Errorf("unexpected owner type")
	}

	return owner, nil
}

// GiveBatchFeedback submits batch feedback for multiple agents
func (c *Client) GiveBatchFeedback(ctx context.Context, feedback []BatchFeedback) (*types.Transaction, error) {
	// Prepare arrays
	agentIds := make([]*big.Int, len(feedback))
	values := make([]*big.Int, len(feedback)) // Use *big.Int for int128
	decimals := make([]uint8, len(feedback))
	tags1 := make([]string, len(feedback))
	tags2 := make([]string, len(feedback))

	for i, f := range feedback {
		agentIds[i] = f.AgentId
		values[i] = big.NewInt(f.Value) // Convert int64 to *big.Int
		decimals[i] = f.ValueDecimals
		tags1[i] = f.Tag1
		tags2[i] = f.Tag2
	}

	// Pack function call
	data, err := c.reputationABI.Pack("giveBatchFeedback", agentIds, values, decimals, tags1, tags2)
	if err != nil {
		return nil, fmt.Errorf("failed to pack giveBatchFeedback: %w", err)
	}

	// Send transaction
	tx, err := c.sendTransaction(ctx, c.reputationRegistryAddr, data)
	if err != nil {
		return nil, fmt.Errorf("failed to send transaction: %w", err)
	}

	// Wait for the transaction to be mined and confirm it didn't revert.
	if _, err := c.waitForReceipt(ctx, tx.Hash()); err != nil {
		return tx, fmt.Errorf("batch feedback transaction reverted: %w", err)
	}

	return tx, nil
}

// BatchFeedback represents feedback entry for batch submission
type BatchFeedback struct {
	AgentId       *big.Int
	Value         int64
	ValueDecimals uint8
	Tag1          string
	Tag2          string
}

// ReputationSummary represents reputation data
type ReputationSummary struct {
	Count    *big.Int
	Value    *big.Int
	Decimals uint8
}

// GetReputation retrieves reputation summary for an agent
func (c *Client) GetReputation(ctx context.Context, agentId *big.Int, tag1, tag2 string) (*ReputationSummary, error) {
	// Registry address (sender of feedback)
	registryAddr := crypto.PubkeyToAddress(c.privateKey.PublicKey)
	clientAddresses := []common.Address{registryAddr}

	// Pack getSummary call
	data, err := c.reputationABI.Pack("getSummary", agentId, clientAddresses, tag1, tag2)
	if err != nil {
		return nil, fmt.Errorf("failed to pack getSummary: %w", err)
	}

	// Call contract
	result, err := c.ethClient.CallContract(ctx, ethereum.CallMsg{
		To:   &c.reputationRegistryAddr,
		Data: data,
	}, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to call contract: %w", err)
	}

	out, err := c.reputationABI.Unpack("getSummary", result)
	if err != nil {
		return nil, fmt.Errorf("failed to unpack result: %w", err)
	}
	if len(out) != 3 {
		return nil, fmt.Errorf("unexpected result arity: %d", len(out))
	}

	count, _ := out[0].(*big.Int)
	summaryValue, _ := out[1].(*big.Int)
	decimals, _ := out[2].(uint8)

	if count == nil || summaryValue == nil {
		return nil, fmt.Errorf("unexpected result types")
	}

	return &ReputationSummary{
		Count:    count,
		Value:    summaryValue,
		Decimals: decimals,
	}, nil
}

// IsTrustedAggregator checks whether addr is a trusted aggregator on the ReputationRegistry.
func (c *Client) IsTrustedAggregator(ctx context.Context, addr common.Address) (bool, error) {
	data, err := c.reputationABI.Pack("trustedAggregators", addr)
	if err != nil {
		return false, fmt.Errorf("failed to pack trustedAggregators: %w", err)
	}

	result, err := c.ethClient.CallContract(ctx, ethereum.CallMsg{
		To:   &c.reputationRegistryAddr,
		Data: data,
	}, nil)
	if err != nil {
		return false, fmt.Errorf("failed to call contract: %w", err)
	}

	out, err := c.reputationABI.Unpack("trustedAggregators", result)
	if err != nil {
		return false, fmt.Errorf("failed to unpack result: %w", err)
	}
	if len(out) != 1 {
		return false, fmt.Errorf("unexpected result arity: %d", len(out))
	}
	trusted, ok := out[0].(bool)
	if !ok {
		return false, fmt.Errorf("unexpected type for trustedAggregators result")
	}
	return trusted, nil
}

// EnsureTrustedAggregator makes the registry wallet a trusted aggregator on the
// ReputationRegistry contract. It is a no-op if the wallet is already trusted.
// The registry wallet must be the contract owner for this call to succeed.
func (c *Client) EnsureTrustedAggregator(ctx context.Context) error {
	selfAddr := crypto.PubkeyToAddress(c.privateKey.PublicKey)

	trusted, err := c.IsTrustedAggregator(ctx, selfAddr)
	if err != nil {
		return fmt.Errorf("failed to check trusted aggregator status: %w", err)
	}
	if trusted {
		return nil // Already trusted; nothing to do.
	}

	data, err := c.reputationABI.Pack("setTrustedAggregator", selfAddr, true)
	if err != nil {
		return fmt.Errorf("failed to pack setTrustedAggregator: %w", err)
	}

	tx, err := c.sendTransaction(ctx, c.reputationRegistryAddr, data)
	if err != nil {
		return fmt.Errorf("failed to send setTrustedAggregator tx: %w", err)
	}

	if _, err := c.waitForReceipt(ctx, tx.Hash()); err != nil {
		return fmt.Errorf("setTrustedAggregator tx failed: %w", err)
	}

	return nil
}

// VerifyPeerIDBinding checks that the on-chain peerIdToAgentId mapping in the
// IdentityRegistry matches the agentId the provider claims. Providers must call
// registerProvider(peerID, agentURI, paymentWallet) on the contract before
// registering with the PRXS registry.
func (c *Client) VerifyPeerIDBinding(ctx context.Context, peerID string, agentId *big.Int) error {
	onChainAgentId, err := c.GetAgentIdByPeerID(ctx, peerID)
	if err != nil {
		return fmt.Errorf("failed to query peerID binding: %w", err)
	}
	if onChainAgentId.Sign() == 0 {
		return fmt.Errorf("peerID %s has no on-chain agentId binding: provider must call registerProvider() on PRXSIdentityRegistry first", peerID)
	}
	if onChainAgentId.Cmp(agentId) != 0 {
		return fmt.Errorf("on-chain agentId mismatch: peerID %s maps to %s on-chain, but provider claims %s", peerID, onChainAgentId.String(), agentId.String())
	}
	return nil
}

// sendTransaction sends a transaction to the blockchain
func (c *Client) sendTransaction(ctx context.Context, to common.Address, data []byte) (*types.Transaction, error) {
	// Get nonce
	from := crypto.PubkeyToAddress(c.privateKey.PublicKey)
	nonce, err := c.ethClient.PendingNonceAt(ctx, from)
	if err != nil {
		return nil, fmt.Errorf("failed to get nonce: %w", err)
	}

	// Get gas price
	gasPrice, err := c.ethClient.SuggestGasPrice(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get gas price: %w", err)
	}

	// Estimate gas
	gasLimit, err := c.ethClient.EstimateGas(ctx, ethereum.CallMsg{
		From: from,
		To:   &to,
		Data: data,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to estimate gas: %w", err)
	}

	// Create transaction
	tx := types.NewTransaction(nonce, to, big.NewInt(0), gasLimit, gasPrice, data)

	// Sign transaction
	signedTx, err := types.SignTx(tx, types.NewEIP155Signer(c.chainID), c.privateKey)
	if err != nil {
		return nil, fmt.Errorf("failed to sign transaction: %w", err)
	}

	// Send transaction
	err = c.ethClient.SendTransaction(ctx, signedTx)
	if err != nil {
		return nil, fmt.Errorf("failed to send transaction: %w", err)
	}

	return signedTx, nil
}

// waitForReceipt waits for transaction receipt
func (c *Client) waitForReceipt(ctx context.Context, txHash common.Hash) (*types.Receipt, error) {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	timeout := time.After(2 * time.Minute)

	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-timeout:
			return nil, fmt.Errorf("timeout waiting for receipt")
		case <-ticker.C:
			receipt, err := c.ethClient.TransactionReceipt(ctx, txHash)
			if err == nil {
				if receipt.Status == types.ReceiptStatusSuccessful {
					return receipt, nil
				}
				return receipt, fmt.Errorf("transaction reverted: tx=%s status=%d", txHash.Hex(), receipt.Status)
			}
		}
	}
}

// loadIdentityABI loads the Identity Registry ABI
func loadIdentityABI() (abi.ABI, error) {
	// This will be replaced with actual ABI JSON
	abiJSON := `[
		{
			"anonymous":false,
			"inputs":[
				{"indexed":true,"internalType":"uint256","name":"agentId","type":"uint256"},
				{"indexed":true,"internalType":"address","name":"owner","type":"address"},
				{"indexed":false,"internalType":"string","name":"agentURI","type":"string"}
			],
			"name":"AgentRegistered",
			"type":"event"
		},
		{
			"inputs":[
				{"internalType":"string","name":"peerID","type":"string"},
				{"internalType":"string","name":"agentURI","type":"string"},
				{"internalType":"address","name":"paymentWallet","type":"address"}
			],
			"name":"registerProvider",
			"outputs":[{"internalType":"uint256","name":"agentId","type":"uint256"}],
			"stateMutability":"nonpayable",
			"type":"function"
		},
		{
			"inputs":[{"internalType":"string","name":"peerID","type":"string"}],
			"name":"getAgentIdByPeerID",
			"outputs":[{"internalType":"uint256","name":"","type":"uint256"}],
			"stateMutability":"view",
			"type":"function"
		},
		{
			"inputs":[{"internalType":"uint256","name":"tokenId","type":"uint256"}],
			"name":"ownerOf",
			"outputs":[{"internalType":"address","name":"","type":"address"}],
			"stateMutability":"view",
			"type":"function"
		}
	]`

	return abi.JSON(strings.NewReader(abiJSON))
}

// loadReputationABI loads the Reputation Registry ABI
func loadReputationABI() (abi.ABI, error) {
	// Reputation Registry ABI with getSummary, giveBatchFeedback, and aggregator management.
	abiJSON := `[
		{
			"inputs":[
				{"internalType":"uint256[]","name":"agentIds","type":"uint256[]"},
				{"internalType":"int128[]","name":"values","type":"int128[]"},
				{"internalType":"uint8[]","name":"valueDecimals","type":"uint8[]"},
				{"internalType":"string[]","name":"tags1","type":"string[]"},
				{"internalType":"string[]","name":"tags2","type":"string[]"}
			],
			"name":"giveBatchFeedback",
			"outputs":[],
			"stateMutability":"nonpayable",
			"type":"function"
		},
		{
			"inputs":[
				{"internalType":"uint256","name":"agentId","type":"uint256"},
				{"internalType":"address[]","name":"clientAddresses","type":"address[]"},
				{"internalType":"string","name":"tag1","type":"string"},
				{"internalType":"string","name":"tag2","type":"string"}
			],
			"name":"getSummary",
			"outputs":[
				{"internalType":"uint256","name":"count","type":"uint256"},
				{"internalType":"int128","name":"summaryValue","type":"int128"},
				{"internalType":"uint8","name":"summaryValueDecimals","type":"uint8"}
			],
			"stateMutability":"view",
			"type":"function"
		},
		{
			"inputs":[{"internalType":"address","name":"","type":"address"}],
			"name":"trustedAggregators",
			"outputs":[{"internalType":"bool","name":"","type":"bool"}],
			"stateMutability":"view",
			"type":"function"
		},
		{
			"inputs":[
				{"internalType":"address","name":"aggregator","type":"address"},
				{"internalType":"bool","name":"trusted","type":"bool"}
			],
			"name":"setTrustedAggregator",
			"outputs":[],
			"stateMutability":"nonpayable",
			"type":"function"
		}
	]`

	return abi.JSON(strings.NewReader(abiJSON))
}

// loadValidationABI loads the Validation Registry ABI
func loadValidationABI() (abi.ABI, error) {
	// Placeholder for now
	abiJSON := `[]`

	return abi.JSON(strings.NewReader(abiJSON))
}

// Close closes the Ethereum client connection
func (c *Client) Close() {
	if c.ethClient != nil {
		c.ethClient.Close()
	}
}

func unpackAgentIDResult(identityABI abi.ABI, result []byte) (*big.Int, error) {
	values, err := identityABI.Unpack("getAgentIdByPeerID", result)
	if err != nil {
		return nil, err
	}
	if len(values) != 1 {
		return nil, fmt.Errorf("unexpected output count: %d", len(values))
	}

	switch v := values[0].(type) {
	case *big.Int:
		return v, nil
	case big.Int:
		return new(big.Int).Set(&v), nil
	default:
		return nil, fmt.Errorf("unexpected output type: %T", values[0])
	}
}
