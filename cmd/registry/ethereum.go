package main

import (
	"context"
	"crypto/ecdsa"
	"fmt"
	"math/big"
	"time"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/ethclient"
)

// EthereumClient handles interactions with Ethereum blockchain
type EthereumClient struct {
	client     *ethclient.Client
	privateKey *ecdsa.PrivateKey
	chainID    *big.Int
	rpcURL     string
}

// NewEthereumClient creates a new Ethereum client
func NewEthereumClient(rpcURL string, privateKeyHex string) (*EthereumClient, error) {
	// Connect to Ethereum node
	client, err := ethclient.Dial(rpcURL)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to Ethereum: %w", err)
	}

	// Parse private key
	privateKey, err := crypto.HexToECDSA(privateKeyHex)
	if err != nil {
		return nil, fmt.Errorf("invalid private key: %w", err)
	}

	// Get chain ID
	chainID, err := client.ChainID(context.Background())
	if err != nil {
		return nil, fmt.Errorf("failed to get chain ID: %w", err)
	}

	return &EthereumClient{
		client:     client,
		privateKey: privateKey,
		chainID:    chainID,
		rpcURL:     rpcURL,
	}, nil
}

// GetAddress returns the address of the Ethereum wallet
func (ec *EthereumClient) GetAddress() string {
	return crypto.PubkeyToAddress(ec.privateKey.PublicKey).Hex()
}

// GetBalance returns the ETH balance of the wallet in wei
func (ec *EthereumClient) GetBalance(ctx context.Context) (*big.Int, error) {
	address := crypto.PubkeyToAddress(ec.privateKey.PublicKey)
	balance, err := ec.client.BalanceAt(ctx, address, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to get balance: %w", err)
	}
	return balance, nil
}

// SendETH sends ETH to a specified address
func (ec *EthereumClient) SendETH(toAddress string, amountWei string) (string, error) {
	ctx := context.Background()

	// Parse amount
	amount := new(big.Int)
	amount, ok := amount.SetString(amountWei, 10)
	if !ok {
		return "", fmt.Errorf("invalid amount: %s", amountWei)
	}

	// Validate recipient address
	if !common.IsHexAddress(toAddress) {
		return "", fmt.Errorf("invalid recipient address: %s", toAddress)
	}

	// Get sender address
	fromAddress := crypto.PubkeyToAddress(ec.privateKey.PublicKey)

	// Check balance
	balance, err := ec.client.BalanceAt(ctx, fromAddress, nil)
	if err != nil {
		return "", fmt.Errorf("failed to get balance: %w", err)
	}

	// Estimate gas cost
	gasPrice, err := ec.client.SuggestGasPrice(ctx)
	if err != nil {
		return "", fmt.Errorf("failed to get gas price: %w", err)
	}

	gasLimit := uint64(21000) // Standard ETH transfer gas limit
	gasCost := new(big.Int).Mul(gasPrice, new(big.Int).SetUint64(gasLimit))

	// Check if we have enough balance (amount + gas)
	totalCost := new(big.Int).Add(amount, gasCost)
	if balance.Cmp(totalCost) < 0 {
		return "", fmt.Errorf("insufficient balance: have %s wei, need %s wei (amount + gas)", balance.String(), totalCost.String())
	}

	// Get nonce
	nonce, err := ec.client.PendingNonceAt(ctx, fromAddress)
	if err != nil {
		return "", fmt.Errorf("failed to get nonce: %w", err)
	}

	// Create transaction
	to := common.HexToAddress(toAddress)
	tx := types.NewTransaction(
		nonce,
		to,
		amount,
		gasLimit,
		gasPrice,
		nil,
	)

	// Sign transaction
	signedTx, err := types.SignTx(tx, types.NewEIP155Signer(ec.chainID), ec.privateKey)
	if err != nil {
		return "", fmt.Errorf("failed to sign transaction: %w", err)
	}

	// Send transaction
	if err := ec.client.SendTransaction(ctx, signedTx); err != nil {
		return "", fmt.Errorf("failed to send transaction: %w", err)
	}

	txHash := signedTx.Hash().Hex()
	return txHash, nil
}

// WaitForConfirmation waits for a transaction to be confirmed
func (ec *EthereumClient) WaitForConfirmation(txHash string, confirmations int) error {
	ctx := context.Background()
	hash := common.HexToHash(txHash)

	// Wait for transaction to be mined (max 5 minutes)
	for i := 0; i < 60; i++ {
		receipt, err := ec.client.TransactionReceipt(ctx, hash)
		if err == nil {
			// Transaction is mined
			if receipt.Status == 0 {
				return fmt.Errorf("transaction failed (status: 0)")
			}

			// Check confirmations
			currentBlock, err := ec.client.BlockNumber(ctx)
			if err != nil {
				return fmt.Errorf("failed to get current block: %w", err)
			}

			confirmationsCount := currentBlock - receipt.BlockNumber.Uint64()
			if confirmationsCount >= uint64(confirmations) {
				return nil // Success!
			}

			// Wait for more confirmations
			time.Sleep(5 * time.Second)
			continue
		}

		// Transaction not mined yet, wait
		time.Sleep(5 * time.Second)
	}

	return fmt.Errorf("timeout waiting for confirmation (5 minutes)")
}

// GetCurrentBlock returns the current block number
func (ec *EthereumClient) GetCurrentBlock(ctx context.Context) (uint64, error) {
	return ec.client.BlockNumber(ctx)
}

// DepositEvent represents a parsed Deposit event from the contract
type DepositEvent struct {
	TxHash      string
	FromAddress string
	PeerID      string
	AmountWei   *big.Int
	BlockNumber uint64
}

// GetDepositEvents fetches Deposit events from the contract
func (ec *EthereumClient) GetDepositEvents(ctx context.Context, contractAddress string, fromBlock uint64, toBlock uint64) ([]DepositEvent, error) {
	if !common.IsHexAddress(contractAddress) {
		return nil, fmt.Errorf("invalid contract address: %s", contractAddress)
	}

	contract := common.HexToAddress(contractAddress)

	// Deposit event signature: Deposit(address indexed from, string peerId, uint256 amount)
	// keccak256("Deposit(address,string,uint256)") = 0x2d4b597935f3cd67fb2eebf1db4debc934cee5c7baa7153f980fdbeb2e74084e
	depositEventSig := common.HexToHash("0x2d4b597935f3cd67fb2eebf1db4debc934cee5c7baa7153f980fdbeb2e74084e")

	query := ethereum.FilterQuery{
		FromBlock: new(big.Int).SetUint64(fromBlock),
		ToBlock:   new(big.Int).SetUint64(toBlock),
		Addresses: []common.Address{contract},
		Topics:    [][]common.Hash{{depositEventSig}},
	}

	logs, err := ec.client.FilterLogs(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to filter logs: %w", err)
	}

	events := make([]DepositEvent, 0, len(logs))
	for _, vLog := range logs {
		event, err := parseDepositEvent(vLog)
		if err != nil {
			continue // Skip malformed events
		}
		events = append(events, event)
	}

	return events, nil
}

// parseDepositEvent parses a raw log into a DepositEvent
func parseDepositEvent(vLog types.Log) (DepositEvent, error) {
	event := DepositEvent{
		TxHash:      vLog.TxHash.Hex(),
		BlockNumber: vLog.BlockNumber,
	}

	// Topic[0] is event signature
	// Topic[1] is indexed 'from' address (padded to 32 bytes)
	if len(vLog.Topics) < 2 {
		return event, fmt.Errorf("missing topics")
	}

	// Extract 'from' address from topic[1]
	event.FromAddress = common.HexToAddress(vLog.Topics[1].Hex()).Hex()

	// Data contains: peerId (string) and amount (uint256)
	// ABI encoded: offset to string, amount, string length, string data
	if len(vLog.Data) < 96 {
		return event, fmt.Errorf("data too short")
	}

	// Parse amount (second 32 bytes after string offset)
	// String offset is first 32 bytes, then amount is NOT there for non-indexed
	// Actually for Solidity: string is dynamic, so data is:
	// [0:32] offset to string
	// [32:64] amount (uint256)
	// [64:96] string length
	// [96:...] string data

	// Wait, let me reconsider the ABI encoding for: (string peerId, uint256 amount)
	// Non-indexed params: string peerId, uint256 amount
	// string is dynamic, uint256 is static
	// Data layout:
	// [0:32] offset to peerId string (should be 64 = 0x40)
	// [32:64] amount (uint256)
	// [64:96] peerId length
	// [96:...] peerId data (padded to 32 bytes)

	amountBytes := vLog.Data[32:64]
	event.AmountWei = new(big.Int).SetBytes(amountBytes)

	// Parse peerId string
	if len(vLog.Data) >= 96 {
		strLenBytes := vLog.Data[64:96]
		strLen := new(big.Int).SetBytes(strLenBytes).Uint64()

		if uint64(len(vLog.Data)) >= 96+strLen {
			event.PeerID = string(vLog.Data[96 : 96+strLen])
		}
	}

	return event, nil
}

// Close closes the Ethereum client connection
func (ec *EthereumClient) Close() {
	if ec.client != nil {
		ec.client.Close()
	}
}
