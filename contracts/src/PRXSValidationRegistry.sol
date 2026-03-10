// SPDX-License-Identifier: MIT
pragma solidity ^0.8.24;

import { IERC8004ValidationRegistry } from "./interfaces/IERC8004ValidationRegistry.sol";
import { IERC8004IdentityRegistry } from "./interfaces/IERC8004IdentityRegistry.sol";

/**
 * @title PRXSValidationRegistry
 * @notice ERC-8004 compliant Validation Registry for PRXS AI agents
 * @dev Enables cryptographic verification of service execution through validators
 */
contract PRXSValidationRegistry is IERC8004ValidationRegistry {
    // ============ State Variables ============
    uint256 private constant MAX_SUMMARY_SCAN = 1000;

    IERC8004IdentityRegistry public immutable identityRegistry;

    /// @notice Mapping: requestHash => ValidationStatus
    mapping(bytes32 => ValidationStatus) private _validations;

    /// @notice Mapping: agentId => array of request hashes
    mapping(uint256 => bytes32[]) private _agentValidations;

    /// @notice Mapping: validatorAddress => array of request hashes
    mapping(address => bytes32[]) private _validatorRequests;

    /// @notice Mapping: requestHash => exists
    mapping(bytes32 => bool) private _requestExists;

    // ============ Constructor ============

    constructor(address _identityRegistry) {
        require(_identityRegistry != address(0), "Invalid identity registry");
        identityRegistry = IERC8004IdentityRegistry(_identityRegistry);
    }

    // ============ External Functions ============

    /**
     * @inheritdoc IERC8004ValidationRegistry
     */
    function validationRequest(
        address validatorAddress,
        uint256 agentId,
        string calldata requestURI,
        bytes32 requestHash
    ) external returns (bytes32 requestHashValue) {
        // Verify caller is agent owner or operator
        address agentOwner = identityRegistry.ownerOf(agentId);
        require(
            msg.sender == agentOwner || identityRegistry.isOperator(agentId, msg.sender),
            "Not authorized"
        );

        require(validatorAddress != address(0), "Invalid validator");
        require(!_requestExists[requestHash], "Request already exists");

        // Create validation entry
        _validations[requestHash] = ValidationStatus({
            validatorAddress: validatorAddress,
            agentId: agentId,
            response: 0,
            responseHash: bytes32(0),
            tag: "",
            hasResponse: false,
            lastUpdate: 0
        });

        _requestExists[requestHash] = true;
        _agentValidations[agentId].push(requestHash);
        _validatorRequests[validatorAddress].push(requestHash);

        emit ValidationRequested(requestHash, validatorAddress, agentId, requestURI, requestHash);

        return requestHash;
    }

    /**
     * @notice Request compute validation (PRXS specific)
     * @param agentId Agent identifier
     * @param validator Validator address
     * @param serviceName Name of service executed
     * @param params Input parameters
     * @param result Claimed result
     * @return requestHash Unique identifier for validation
     */
    function requestComputeValidation(
        uint256 agentId,
        address validator,
        string calldata serviceName,
        string calldata params,
        string calldata result
    ) external returns (bytes32 requestHash) {
        // Verify caller is agent owner or operator
        address agentOwner = identityRegistry.ownerOf(agentId);
        require(
            msg.sender == agentOwner || identityRegistry.isOperator(agentId, msg.sender),
            "Not authorized"
        );

        require(validator != address(0), "Invalid validator");

        // Build requestURI from parameters
        string memory requestURI = string(
            abi.encodePacked(
                '{"service":"', serviceName,
                '","params":"', params,
                '","result":"', result,
                '"}'
            )
        );

        // Generate request hash
        requestHash = keccak256(abi.encodePacked(agentId, validator, requestURI, block.timestamp));
        require(!_requestExists[requestHash], "Request already exists");

        // Create validation entry
        _validations[requestHash] = ValidationStatus({
            validatorAddress: validator,
            agentId: agentId,
            response: 0,
            responseHash: bytes32(0),
            tag: "compute_verification",
            hasResponse: false,
            lastUpdate: 0
        });

        _requestExists[requestHash] = true;
        _agentValidations[agentId].push(requestHash);
        _validatorRequests[validator].push(requestHash);

        emit ValidationRequested(requestHash, validator, agentId, requestURI, requestHash);

        return requestHash;
    }

    /**
     * @inheritdoc IERC8004ValidationRegistry
     */
    function validationResponse(
        bytes32 requestHash,
        uint8 response,
        string calldata responseURI,
        bytes32 responseHash,
        string calldata tag
    ) external {
        require(_requestExists[requestHash], "Request not found");

        ValidationStatus storage validation = _validations[requestHash];
        require(msg.sender == validation.validatorAddress, "Not designated validator");
        require(response <= 100, "Invalid response value");

        validation.response = response;
        validation.responseHash = responseHash;
        validation.tag = tag;
        validation.hasResponse = true;
        validation.lastUpdate = block.timestamp;

        emit ValidationResponse(requestHash, msg.sender, response, responseURI, responseHash, tag);
    }

    /**
     * @notice Submit validation result (simplified for compute verification)
     * @param requestHash Request identifier
     * @param passed Whether validation passed
     * @param proofURI URI with validation proof
     */
    function submitValidationResult(
        bytes32 requestHash,
        bool passed,
        string calldata proofURI
    ) external {
        require(_requestExists[requestHash], "Request not found");

        ValidationStatus storage validation = _validations[requestHash];
        require(msg.sender == validation.validatorAddress, "Not designated validator");

        uint8 response = passed ? 100 : 0;
        bytes32 proofHash = keccak256(bytes(proofURI));

        validation.response = response;
        validation.responseHash = proofHash;
        validation.hasResponse = true;
        validation.lastUpdate = block.timestamp;

        emit ValidationResponse(
            requestHash,
            msg.sender,
            response,
            proofURI,
            proofHash,
            validation.tag
        );
    }

    /**
     * @inheritdoc IERC8004ValidationRegistry
     */
    function getValidationStatus(bytes32 requestHash)
        external
        view
        returns (ValidationStatus memory status)
    {
        require(_requestExists[requestHash], "Request not found");
        return _validations[requestHash];
    }

    /**
     * @inheritdoc IERC8004ValidationRegistry
     */
    function getSummary(
        uint256 agentId,
        address[] calldata validatorAddresses,
        string calldata tag
    ) external view returns (uint256 count, uint256 averageResponse) {
        bytes32[] storage requests = _agentValidations[agentId];
        uint256 offset = 0;
        if (requests.length > MAX_SUMMARY_SCAN) {
            // Bound scan size to keep view calls usable as history grows.
            offset = requests.length - MAX_SUMMARY_SCAN;
        }
        (count, averageResponse,) = _getSummary(agentId, validatorAddresses, tag, offset, MAX_SUMMARY_SCAN);
    }

    /**
     * @inheritdoc IERC8004ValidationRegistry
     */
    function getSummaryPaginated(
        uint256 agentId,
        address[] calldata validatorAddresses,
        string calldata tag,
        uint256 offset,
        uint256 limit
    ) external view returns (uint256 count, uint256 averageResponse, uint256 scanned) {
        return _getSummary(agentId, validatorAddresses, tag, offset, limit);
    }

    /**
     * @inheritdoc IERC8004ValidationRegistry
     */
    function getAgentValidations(uint256 agentId)
        external
        view
        returns (bytes32[] memory requestHashes)
    {
        return _agentValidations[agentId];
    }

    /**
     * @inheritdoc IERC8004ValidationRegistry
     */
    function getValidatorRequests(address validatorAddress)
        external
        view
        returns (bytes32[] memory requestHashes)
    {
        return _validatorRequests[validatorAddress];
    }

    /**
     * @notice Check if request exists
     * @param requestHash Request identifier
     * @return exists True if request exists
     */
    function requestExists(bytes32 requestHash) external view returns (bool exists) {
        return _requestExists[requestHash];
    }

    function _getSummary(
        uint256 agentId,
        address[] calldata validatorAddresses,
        string calldata tag,
        uint256 offset,
        uint256 limit
    ) internal view returns (uint256 count, uint256 averageResponse, uint256 scanned) {
        if (limit == 0) {
            return (0, 0, 0);
        }

        bytes32[] storage requests = _agentValidations[agentId];
        if (offset >= requests.length) {
            return (0, 0, 0);
        }

        uint256 end = offset + limit;
        if (end > requests.length) {
            end = requests.length;
        }

        uint256 totalResponse = 0;
        for (uint256 i = offset; i < end; i++) {
            scanned++;
            ValidationStatus storage validation = _validations[requests[i]];

            if (validatorAddresses.length > 0) {
                bool matchValidator = false;
                for (uint256 j = 0; j < validatorAddresses.length; j++) {
                    if (validation.validatorAddress == validatorAddresses[j]) {
                        matchValidator = true;
                        break;
                    }
                }
                if (!matchValidator) {
                    continue;
                }
            }

            if (bytes(tag).length > 0 && keccak256(bytes(validation.tag)) != keccak256(bytes(tag))) {
                continue;
            }

            // Count only answered validations.
            if (validation.hasResponse) {
                totalResponse += validation.response;
                count++;
            }
        }

        if (count > 0) {
            averageResponse = totalResponse / count;
        }
    }
}
