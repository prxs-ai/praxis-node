// SPDX-License-Identifier: MIT
pragma solidity ^0.8.24;

import { IERC8004ReputationRegistry } from "./interfaces/IERC8004ReputationRegistry.sol";
import { IERC8004IdentityRegistry } from "./interfaces/IERC8004IdentityRegistry.sol";

/**
 * @title PRXSReputationRegistry
 * @notice ERC-8004 compliant Reputation Registry for PRXS AI agents
 * @dev Stores feedback signals and aggregates reputation metrics
 */
contract PRXSReputationRegistry is IERC8004ReputationRegistry {
    // ============ State Variables ============

    IERC8004IdentityRegistry public immutable identityRegistry;

    /// @notice Mapping: agentId => client => feedback index => Feedback
    mapping(uint256 => mapping(address => mapping(uint64 => Feedback))) private _feedbacks;

    /// @notice Mapping: agentId => client => feedback count
    mapping(uint256 => mapping(address => uint64)) private _feedbackCounts;

    /// @notice Mapping: agentId => total feedback count (all clients)
    mapping(uint256 => uint256) private _totalFeedbackCounts;

    /// @notice Trusted aggregator addresses (can submit batch feedback)
    mapping(address => bool) public trustedAggregators;

    /// @notice Contract owner
    address public owner;

    // ============ Constructor ============

    constructor(address _identityRegistry) {
        require(_identityRegistry != address(0), "Invalid identity registry");
        identityRegistry = IERC8004IdentityRegistry(_identityRegistry);
        owner = msg.sender;
        trustedAggregators[msg.sender] = true; // Owner is trusted by default
    }

    // ============ Modifiers ============

    modifier onlyOwner() {
        require(msg.sender == owner, "Not owner");
        _;
    }

    // ============ External Functions ============

    /**
     * @inheritdoc IERC8004ReputationRegistry
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
    ) external {
        // Verify agent exists
        require(identityRegistry.ownerOf(agentId) != address(0), "Agent not found");

        // Prevent self-feedback
        address agentOwner = identityRegistry.ownerOf(agentId);
        require(msg.sender != agentOwner, "Cannot give self-feedback");

        // Prevent operator feedback
        require(!identityRegistry.isOperator(agentId, msg.sender), "Operators cannot give feedback");

        // Validate decimals
        require(valueDecimals <= 18, "Decimals too high");

        uint64 feedbackIndex = _feedbackCounts[agentId][msg.sender]++;
        _totalFeedbackCounts[agentId]++;

        _feedbacks[agentId][msg.sender][feedbackIndex] = Feedback({
            value: value,
            valueDecimals: valueDecimals,
            tag1: tag1,
            tag2: tag2,
            endpoint: endpoint,
            feedbackURI: feedbackURI,
            feedbackHash: feedbackHash,
            timestamp: block.timestamp,
            isRevoked: false
        });

        emit FeedbackGiven(agentId, msg.sender, feedbackIndex, value, valueDecimals, tag1, tag2);
    }

    /**
     * @notice Submit batch feedback (trusted aggregators only)
     * @dev Used by Registry to submit aggregated metrics periodically
     */
    function giveBatchFeedback(
        uint256[] calldata agentIds,
        int128[] calldata values,
        uint8[] calldata valueDecimals,
        string[] calldata tags1,
        string[] calldata tags2
    ) external {
        require(trustedAggregators[msg.sender], "Not trusted aggregator");
        require(
            agentIds.length == values.length &&
            agentIds.length == valueDecimals.length &&
            agentIds.length == tags1.length &&
            agentIds.length == tags2.length,
            "Array length mismatch"
        );

        for (uint256 i = 0; i < agentIds.length; i++) {
            uint256 agentId = agentIds[i];

            // Verify agent exists
            require(identityRegistry.ownerOf(agentId) != address(0), "Agent not found");

            uint64 feedbackIndex = _feedbackCounts[agentId][msg.sender]++;
            _totalFeedbackCounts[agentId]++;

            _feedbacks[agentId][msg.sender][feedbackIndex] = Feedback({
                value: values[i],
                valueDecimals: valueDecimals[i],
                tag1: tags1[i],
                tag2: tags2[i],
                endpoint: "",
                feedbackURI: "",
                feedbackHash: bytes32(0),
                timestamp: block.timestamp,
                isRevoked: false
            });

            emit FeedbackGiven(
                agentId,
                msg.sender,
                feedbackIndex,
                values[i],
                valueDecimals[i],
                tags1[i],
                tags2[i]
            );
        }
    }

    /**
     * @inheritdoc IERC8004ReputationRegistry
     */
    function revokeFeedback(uint256 agentId, uint64 feedbackIndex) external {
        Feedback storage feedback = _feedbacks[agentId][msg.sender][feedbackIndex];
        require(feedback.timestamp > 0, "Feedback not found");
        require(!feedback.isRevoked, "Already revoked");

        feedback.isRevoked = true;
        emit FeedbackRevoked(agentId, msg.sender, feedbackIndex);
    }

    /**
     * @inheritdoc IERC8004ReputationRegistry
     */
    function appendResponse(
        uint256 agentId,
        address clientAddress,
        uint64 feedbackIndex,
        string calldata responseURI,
        bytes32 responseHash
    ) external {
        // Only agent owner or operator can respond
        address agentOwner = identityRegistry.ownerOf(agentId);
        require(
            msg.sender == agentOwner || identityRegistry.isOperator(agentId, msg.sender),
            "Not authorized"
        );

        Feedback storage feedback = _feedbacks[agentId][clientAddress][feedbackIndex];
        require(feedback.timestamp > 0, "Feedback not found");

        emit ResponseAppended(agentId, clientAddress, feedbackIndex, responseURI, responseHash);
    }

    /**
     * @inheritdoc IERC8004ReputationRegistry
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
    ) {
        Feedback storage feedback = _feedbacks[agentId][clientAddress][feedbackIndex];
        return (
            feedback.value,
            feedback.valueDecimals,
            feedback.tag1,
            feedback.tag2,
            feedback.isRevoked
        );
    }

    /**
     * @inheritdoc IERC8004ReputationRegistry
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
    ) {
        count = 0;
        int256 totalValue = 0;
        summaryValueDecimals = 2; // Default to 2 decimal places

        // If no clients specified, iterate all feedback (expensive!)
        // In production, consider limiting or using off-chain aggregation
        if (clientAddresses.length == 0) {
            // For now, return zero as full iteration is gas-prohibitive
            return (0, 0, 0);
        }

        // Filter by specified clients
        for (uint256 i = 0; i < clientAddresses.length; i++) {
            address client = clientAddresses[i];
            uint64 feedbackCount = _feedbackCounts[agentId][client];

            for (uint64 j = 0; j < feedbackCount; j++) {
                Feedback storage feedback = _feedbacks[agentId][client][j];

                if (feedback.isRevoked) continue;

                // Filter by tags if specified
                bool tag1Match = bytes(tag1).length == 0 ||
                    keccak256(bytes(feedback.tag1)) == keccak256(bytes(tag1));
                bool tag2Match = bytes(tag2).length == 0 ||
                    keccak256(bytes(feedback.tag2)) == keccak256(bytes(tag2));

                if (tag1Match && tag2Match) {
                    totalValue += int256(feedback.value);
                    count++;
                }
            }
        }

        // Calculate average
        if (count > 0) {
            summaryValue = int128(totalValue / int256(count));
        }

        return (count, summaryValue, summaryValueDecimals);
    }

    /**
     * @inheritdoc IERC8004ReputationRegistry
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
    ) {
        // Count matching feedback first
        uint256 matchCount = 0;
        for (uint256 i = 0; i < clientAddresses.length; i++) {
            address client = clientAddresses[i];
            uint64 feedbackCount = _feedbackCounts[agentId][client];

            for (uint64 j = 0; j < feedbackCount; j++) {
                Feedback storage feedback = _feedbacks[agentId][client][j];

                if (!includeRevoked && feedback.isRevoked) continue;

                bool tag1Match = bytes(tag1).length == 0 ||
                    keccak256(bytes(feedback.tag1)) == keccak256(bytes(tag1));
                bool tag2Match = bytes(tag2).length == 0 ||
                    keccak256(bytes(feedback.tag2)) == keccak256(bytes(tag2));

                if (tag1Match && tag2Match) {
                    matchCount++;
                }
            }
        }

        // Allocate arrays
        clients = new address[](matchCount);
        indexes = new uint64[](matchCount);
        values = new int128[](matchCount);
        decimals = new uint8[](matchCount);
        tags1 = new string[](matchCount);
        tags2 = new string[](matchCount);
        revoked = new bool[](matchCount);

        // Populate arrays
        uint256 idx = 0;
        for (uint256 i = 0; i < clientAddresses.length; i++) {
            address client = clientAddresses[i];
            uint64 feedbackCount = _feedbackCounts[agentId][client];

            for (uint64 j = 0; j < feedbackCount; j++) {
                Feedback storage feedback = _feedbacks[agentId][client][j];

                if (!includeRevoked && feedback.isRevoked) continue;

                bool tag1Match = bytes(tag1).length == 0 ||
                    keccak256(bytes(feedback.tag1)) == keccak256(bytes(tag1));
                bool tag2Match = bytes(tag2).length == 0 ||
                    keccak256(bytes(feedback.tag2)) == keccak256(bytes(tag2));

                if (tag1Match && tag2Match) {
                    clients[idx] = client;
                    indexes[idx] = j;
                    values[idx] = feedback.value;
                    decimals[idx] = feedback.valueDecimals;
                    tags1[idx] = feedback.tag1;
                    tags2[idx] = feedback.tag2;
                    revoked[idx] = feedback.isRevoked;
                    idx++;
                }
            }
        }

        return (clients, indexes, values, decimals, tags1, tags2, revoked);
    }

    /**
     * @inheritdoc IERC8004ReputationRegistry
     */
    function getFeedbackCount(uint256 agentId, address client)
        external
        view
        returns (uint256 count)
    {
        if (client == address(0)) {
            return _totalFeedbackCounts[agentId];
        }
        return _feedbackCounts[agentId][client];
    }

    // ============ Admin Functions ============

    /**
     * @notice Set trusted aggregator status
     * @param aggregator Address to set status for
     * @param trusted True to trust, false to revoke
     */
    function setTrustedAggregator(address aggregator, bool trusted) external onlyOwner {
        trustedAggregators[aggregator] = trusted;
    }

    /**
     * @notice Transfer ownership
     * @param newOwner New owner address
     */
    function transferOwnership(address newOwner) external onlyOwner {
        require(newOwner != address(0), "Invalid new owner");
        owner = newOwner;
    }
}
