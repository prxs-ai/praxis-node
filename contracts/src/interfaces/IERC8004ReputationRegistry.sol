// SPDX-License-Identifier: MIT
pragma solidity ^0.8.24;

/**
 * @title IERC8004ReputationRegistry
 * @notice Interface for ERC-8004 Reputation Registry
 * @dev Feedback system for posting and reading trust signals about agents
 */
interface IERC8004ReputationRegistry {
    /// @notice Feedback entry structure
    struct Feedback {
        int128 value;           // Signed fixed-point value
        uint8 valueDecimals;    // Number of decimal places (0-18)
        string tag1;            // Primary tag for categorization
        string tag2;            // Secondary tag for filtering
        string endpoint;        // Service endpoint evaluated
        string feedbackURI;     // Off-chain evidence URI
        bytes32 feedbackHash;   // KECCAK-256 hash of feedbackURI content
        uint256 timestamp;      // When feedback was given
        bool isRevoked;         // Whether feedback was revoked
    }

    /// @notice Emitted when feedback is given
    event FeedbackGiven(
        uint256 indexed agentId,
        address indexed client,
        uint64 indexed feedbackIndex,
        int128 value,
        uint8 valueDecimals,
        string tag1,
        string tag2
    );

    /// @notice Emitted when feedback is revoked
    event FeedbackRevoked(
        uint256 indexed agentId,
        address indexed client,
        uint64 indexed feedbackIndex
    );

    /// @notice Emitted when agent responds to feedback
    event ResponseAppended(
        uint256 indexed agentId,
        address indexed client,
        uint64 indexed feedbackIndex,
        string responseURI,
        bytes32 responseHash
    );

    /**
     * @notice Give feedback for an agent
     * @param agentId Agent identifier from Identity Registry
     * @param value Numerical rating (fixed-point)
     * @param valueDecimals Decimal places for value (0-18)
     * @param tag1 Primary categorization tag (e.g., "starred", "latency")
     * @param tag2 Secondary tag (e.g., "24h", "ethereum")
     * @param endpoint Service endpoint being rated
     * @param feedbackURI Off-chain detailed evidence
     * @param feedbackHash KECCAK-256 hash of feedbackURI content
     */
    function giveFeedback(
        uint256 agentId,
        int128 value,
        uint8 valueDecimals,
        string calldata tag1,
        string calldata tag2,
        string calldata endpoint,
        string calldata feedbackURI,
        bytes32 feedbackHash
    ) external;

    /**
     * @notice Revoke previously given feedback
     * @param agentId Agent identifier
     * @param feedbackIndex Index of feedback to revoke
     */
    function revokeFeedback(uint256 agentId, uint64 feedbackIndex) external;

    /**
     * @notice Agent appends response to client feedback
     * @param agentId Agent identifier
     * @param clientAddress Client who gave feedback
     * @param feedbackIndex Index of feedback
     * @param responseURI URI with agent's response
     * @param responseHash KECCAK-256 hash of responseURI content
     */
    function appendResponse(
        uint256 agentId,
        address clientAddress,
        uint64 feedbackIndex,
        string calldata responseURI,
        bytes32 responseHash
    ) external;

    /**
     * @notice Read specific feedback entry
     * @param agentId Agent identifier
     * @param clientAddress Client who gave feedback
     * @param feedbackIndex Index of feedback
     * @return value Feedback value
     * @return valueDecimals Decimal places
     * @return tag1 Primary tag
     * @return tag2 Secondary tag
     * @return isRevoked Whether feedback was revoked
     */
    function readFeedback(
        uint256 agentId,
        address clientAddress,
        uint64 feedbackIndex
    ) external view returns (
        int128 value,
        uint8 valueDecimals,
        string memory tag1,
        string memory tag2,
        bool isRevoked
    );

    /**
     * @notice Get aggregated summary of feedback
     * @param agentId Agent identifier
     * @param clientAddresses Array of clients to filter (empty = all)
     * @param tag1 Primary tag filter (empty = all)
     * @param tag2 Secondary tag filter (empty = all)
     * @return count Number of feedback entries
     * @return summaryValue Aggregated value (sum)
     * @return summaryValueDecimals Decimals for summary
     */
    function getSummary(
        uint256 agentId,
        address[] calldata clientAddresses,
        string calldata tag1,
        string calldata tag2
    ) external view returns (
        uint256 count,
        int128 summaryValue,
        uint8 summaryValueDecimals
    );

    /**
     * @notice Read all feedback entries with filtering
     * @param agentId Agent identifier
     * @param clientAddresses Array of clients to filter (empty = all)
     * @param tag1 Primary tag filter (empty = all)
     * @param tag2 Secondary tag filter (empty = all)
     * @param includeRevoked Whether to include revoked feedback
     * @return clients Array of client addresses
     * @return indexes Array of feedback indexes
     * @return values Array of feedback values
     * @return decimals Array of value decimals
     * @return tags1 Array of primary tags
     * @return tags2 Array of secondary tags
     * @return revoked Array of revocation status
     */
    function readAllFeedback(
        uint256 agentId,
        address[] calldata clientAddresses,
        string calldata tag1,
        string calldata tag2,
        bool includeRevoked
    ) external view returns (
        address[] memory clients,
        uint64[] memory indexes,
        int128[] memory values,
        uint8[] memory decimals,
        string[] memory tags1,
        string[] memory tags2,
        bool[] memory revoked
    );

    /**
     * @notice Get feedback count for an agent
     * @param agentId Agent identifier
     * @param client Client address (address(0) for all clients)
     * @return count Number of feedback entries
     */
    function getFeedbackCount(uint256 agentId, address client) external view returns (uint256 count);
}
