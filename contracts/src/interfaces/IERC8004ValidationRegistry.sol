// SPDX-License-Identifier: MIT
pragma solidity ^0.8.24;

/**
 * @title IERC8004ValidationRegistry
 * @notice Interface for ERC-8004 Validation Registry
 * @dev Enables cryptographic verification of agent work through validators
 */
interface IERC8004ValidationRegistry {
    /// @notice Validation status structure
    struct ValidationStatus {
        address validatorAddress;   // Address of validator
        uint256 agentId;            // Agent being validated
        uint8 response;             // Validation result (0-100 scale)
        bytes32 responseHash;       // KECCAK-256 hash of responseURI
        string tag;                 // Categorization tag
        bool hasResponse;           // True after validator submitted response
        uint256 lastUpdate;         // Timestamp of last update
    }

    /// @notice Emitted when validation is requested
    event ValidationRequested(
        bytes32 indexed requestHash,
        address indexed validatorAddress,
        uint256 indexed agentId,
        string requestURI,
        bytes32 requestHashValue
    );

    /// @notice Emitted when validator provides response
    event ValidationResponse(
        bytes32 indexed requestHash,
        address indexed validatorAddress,
        uint8 response,
        string responseURI,
        bytes32 responseHash,
        string tag
    );

    /**
     * @notice Request validation for agent work
     * @dev Must be called by agent owner or operator
     * @param validatorAddress Address of validator to perform validation
     * @param agentId Agent identifier from Identity Registry
     * @param requestURI URI containing validation inputs/outputs
     * @param requestHash KECCAK-256 hash of requestURI content
     * @return requestHashValue Unique identifier for this validation request
     */
    function validationRequest(
        address validatorAddress,
        uint256 agentId,
        string calldata requestURI,
        bytes32 requestHash
    ) external returns (bytes32 requestHashValue);

    /**
     * @notice Validator submits validation result
     * @dev Must be called by designated validator address
     * @param requestHash Unique identifier from validationRequest
     * @param response Validation result (0-100): 0=failed, 100=passed, or spectrum
     * @param responseURI URI with validation proof/evidence
     * @param responseHash KECCAK-256 hash of responseURI content
     * @param tag Optional categorization (e.g., "zkproof", "tee", "reexecution")
     */
    function validationResponse(
        bytes32 requestHash,
        uint8 response,
        string calldata responseURI,
        bytes32 responseHash,
        string calldata tag
    ) external;

    /**
     * @notice Get validation status for a request
     * @param requestHash Unique identifier from validationRequest
     * @return status Current validation status
     */
    function getValidationStatus(bytes32 requestHash)
        external
        view
        returns (ValidationStatus memory status);

    /**
     * @notice Get aggregated validation summary for an agent
     * @param agentId Agent identifier
     * @param validatorAddresses Array of validators to filter (empty = all)
     * @param tag Tag filter (empty = all)
     * @return count Number of validations
     * @return averageResponse Average validation score (0-100)
     */
    function getSummary(
        uint256 agentId,
        address[] calldata validatorAddresses,
        string calldata tag
    ) external view returns (uint256 count, uint256 averageResponse);

    /**
     * @notice Get aggregated validation summary for a bounded request window
     * @param agentId Agent identifier
     * @param validatorAddresses Array of validators to filter (empty = all)
     * @param tag Tag filter (empty = all)
     * @param offset Starting index in the agent's request list
     * @param limit Max number of requests to scan
     * @return count Number of answered validations in scanned window
     * @return averageResponse Average validation score (0-100) in scanned window
     * @return scanned Number of requests scanned
     */
    function getSummaryPaginated(
        uint256 agentId,
        address[] calldata validatorAddresses,
        string calldata tag,
        uint256 offset,
        uint256 limit
    ) external view returns (uint256 count, uint256 averageResponse, uint256 scanned);

    /**
     * @notice Get all validation request hashes for an agent
     * @param agentId Agent identifier
     * @return requestHashes Array of request hashes
     */
    function getAgentValidations(uint256 agentId)
        external
        view
        returns (bytes32[] memory requestHashes);

    /**
     * @notice Get all validation requests for a validator
     * @param validatorAddress Validator address
     * @return requestHashes Array of request hashes
     */
    function getValidatorRequests(address validatorAddress)
        external
        view
        returns (bytes32[] memory requestHashes);
}
