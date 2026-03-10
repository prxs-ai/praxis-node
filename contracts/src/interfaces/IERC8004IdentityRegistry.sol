// SPDX-License-Identifier: MIT
pragma solidity ^0.8.24;

import { IERC721 } from "@openzeppelin/contracts/token/ERC721/IERC721.sol";

/**
 * @title IERC8004IdentityRegistry
 * @notice Interface for ERC-8004 Identity Registry
 * @dev Agent identity registry built on ERC-721 for portable, transferable identities
 */
interface IERC8004IdentityRegistry is IERC721 {
    /// @notice Metadata entry structure for on-chain key-value storage
    struct MetadataEntry {
        string key;
        bytes value;
    }

    /// @notice Emitted when a new agent is registered
    event AgentRegistered(uint256 indexed agentId, address indexed owner, string agentURI);

    /// @notice Emitted when agent URI is updated
    event AgentURIUpdated(uint256 indexed agentId, string newURI);

    /// @notice Emitted when agent metadata is updated
    event AgentMetadataUpdated(uint256 indexed agentId, string key, bytes value);

    /// @notice Emitted when agent wallet is updated
    event AgentWalletUpdated(uint256 indexed agentId, address indexed newWallet);

    /// @notice Emitted when operator is set for an agent
    event OperatorSet(uint256 indexed agentId, address indexed operator, bool approved);

    /**
     * @notice Register a new agent and mint identity NFT
     * @param agentURI URI pointing to agent registration file (JSON metadata)
     * @param metadata Optional on-chain metadata entries
     * @return agentId Unique identifier (NFT tokenId) for the agent
     */
    function register(
        string calldata agentURI,
        MetadataEntry[] calldata metadata
    ) external returns (uint256 agentId);

    /**
     * @notice Update agent registration URI
     * @param agentId Agent identifier
     * @param newURI New URI for agent metadata
     */
    function setAgentURI(uint256 agentId, string calldata newURI) external;

    /**
     * @notice Set on-chain metadata for an agent
     * @param agentId Agent identifier
     * @param key Metadata key
     * @param value Metadata value (bytes for flexibility)
     */
    function setMetadata(uint256 agentId, string calldata key, bytes calldata value) external;

    /**
     * @notice Get on-chain metadata for an agent
     * @param agentId Agent identifier
     * @param key Metadata key
     * @return value Metadata value
     */
    function getMetadata(uint256 agentId, string calldata key) external view returns (bytes memory value);

    /**
     * @notice Set agent wallet address (requires signature proof)
     * @param agentId Agent identifier
     * @param newWallet New wallet address
     * @param deadline Signature expiration timestamp
     * @param signature EIP-712 or ERC-1271 signature from newWallet
     */
    function setAgentWallet(
        uint256 agentId,
        address newWallet,
        uint256 deadline,
        bytes calldata signature
    ) external;

    /**
     * @notice Get agent's payment wallet address
     * @param agentId Agent identifier
     * @return wallet Agent's wallet address
     */
    function getAgentWallet(uint256 agentId) external view returns (address wallet);

    /**
     * @notice Set or revoke operator status for an agent
     * @param agentId Agent identifier
     * @param operator Address to grant/revoke operator rights
     * @param approved True to approve, false to revoke
     */
    function setOperator(uint256 agentId, address operator, bool approved) external;

    /**
     * @notice Check if address is operator for an agent
     * @param agentId Agent identifier
     * @param operator Address to check
     * @return True if operator is approved
     */
    function isOperator(uint256 agentId, address operator) external view returns (bool);
}
