// SPDX-License-Identifier: MIT
pragma solidity ^0.8.24;

import { ERC721 } from "@openzeppelin/contracts/token/ERC721/ERC721.sol";
import { ERC721URIStorage } from "@openzeppelin/contracts/token/ERC721/extensions/ERC721URIStorage.sol";
import { IERC8004IdentityRegistry } from "./interfaces/IERC8004IdentityRegistry.sol";
import { ECDSA } from "@openzeppelin/contracts/utils/cryptography/ECDSA.sol";

/**
 * @title PRXSIdentityRegistry
 * @notice ERC-8004 compliant Identity Registry for PRXS AI agents
 * @dev Each PRXS provider is registered as an NFT (agent identity)
 */
contract PRXSIdentityRegistry is ERC721URIStorage, IERC8004IdentityRegistry {
    using ECDSA for bytes32;

    // ============ State Variables ============

    uint256 private _nextAgentId;

    /// @notice Mapping: agentId => metadata key => value
    mapping(uint256 => mapping(string => bytes)) private _metadata;

    /// @notice Mapping: agentId => agent wallet address
    mapping(uint256 => address) private _agentWallets;

    /// @notice Mapping: agentId => operatorEpoch => operator => approved
    /// @dev Operator epoch increments on transfer to invalidate stale operators.
    mapping(uint256 => mapping(uint256 => mapping(address => bool))) private _operators;

    /// @notice Mapping: agentId => current operator epoch
    mapping(uint256 => uint256) private _operatorEpoch;

    /// @notice Mapping: peerID => agentId (PRXS specific)
    mapping(string => uint256) public peerIdToAgentId;

    /// @notice EIP-712 domain separator
    bytes32 private immutable _DOMAIN_SEPARATOR;

    /// @notice EIP-712 typehash for wallet update
    bytes32 private constant _SET_WALLET_TYPEHASH =
        keccak256("SetAgentWallet(uint256 agentId,address newWallet,uint256 deadline)");

    // ============ Constructor ============

    constructor() ERC721("PRXS AI Agent", "PRXSAI") {
        _nextAgentId = 1; // Start from 1, 0 is invalid

        _DOMAIN_SEPARATOR = keccak256(
            abi.encode(
                keccak256("EIP712Domain(string name,string version,uint256 chainId,address verifyingContract)"),
                keccak256(bytes("PRXSIdentityRegistry")),
                keccak256(bytes("1")),
                block.chainid,
                address(this)
            )
        );
    }

    // ============ External Functions ============

    /**
     * @inheritdoc IERC8004IdentityRegistry
     */
    function register(
        string calldata agentURI,
        MetadataEntry[] calldata metadata
    ) external returns (uint256 agentId) {
        agentId = _nextAgentId++;

        _mint(msg.sender, agentId);
        _setTokenURI(agentId, agentURI);

        // Set optional metadata
        for (uint256 i = 0; i < metadata.length; i++) {
            _metadata[agentId][metadata[i].key] = metadata[i].value;
        }

        emit AgentRegistered(agentId, msg.sender, agentURI);

        return agentId;
    }

    /**
     * @notice Register PRXS provider with peerID mapping
     * @param peerID PRXS libp2p peer identifier
     * @param agentURI URI pointing to service metadata
     * @param paymentWallet Wallet address for payments
     * @return agentId Unique agent identifier
     */
    function registerProvider(
        string calldata peerID,
        string calldata agentURI,
        address paymentWallet
    ) external returns (uint256 agentId) {
        require(peerIdToAgentId[peerID] == 0, "PeerID already registered");
        // Note: paymentWallet can be zero address for non-EVM providers

        agentId = _nextAgentId++;

        _mint(msg.sender, agentId);
        _setTokenURI(agentId, agentURI);

        // Set payment wallet
        _agentWallets[agentId] = paymentWallet;

        // Map peerID to agentId
        peerIdToAgentId[peerID] = agentId;

        emit AgentRegistered(agentId, msg.sender, agentURI);
        emit AgentWalletUpdated(agentId, paymentWallet);

        return agentId;
    }

    /**
     * @inheritdoc IERC8004IdentityRegistry
     */
    function setAgentURI(uint256 agentId, string calldata newURI) external {
        require(_isApprovedOrOwner(msg.sender, agentId), "Not authorized");
        _setTokenURI(agentId, newURI);
        emit AgentURIUpdated(agentId, newURI);
    }

    /**
     * @inheritdoc IERC8004IdentityRegistry
     */
    function setMetadata(uint256 agentId, string calldata key, bytes calldata value) external {
        require(
            _isApprovedOrOwner(msg.sender, agentId) || _operators[agentId][_operatorEpoch[agentId]][msg.sender],
            "Not authorized"
        );

        _metadata[agentId][key] = value;
        emit AgentMetadataUpdated(agentId, key, value);
    }

    /**
     * @inheritdoc IERC8004IdentityRegistry
     */
    function getMetadata(uint256 agentId, string calldata key)
        external
        view
        returns (bytes memory value)
    {
        return _metadata[agentId][key];
    }

    /**
     * @inheritdoc IERC8004IdentityRegistry
     */
    function setAgentWallet(
        uint256 agentId,
        address newWallet,
        uint256 deadline,
        bytes calldata signature
    ) external {
        require(
            _isApprovedOrOwner(msg.sender, agentId) || _operators[agentId][_operatorEpoch[agentId]][msg.sender],
            "Not authorized"
        );
        require(block.timestamp <= deadline, "Signature expired");
        require(newWallet != address(0), "Invalid wallet");

        // Verify signature from newWallet
        bytes32 structHash = keccak256(
            abi.encode(_SET_WALLET_TYPEHASH, agentId, newWallet, deadline)
        );
        bytes32 digest = keccak256(
            abi.encodePacked("\x19\x01", _DOMAIN_SEPARATOR, structHash)
        );

        address signer = digest.recover(signature);
        require(signer == newWallet, "Invalid signature");

        _agentWallets[agentId] = newWallet;
        emit AgentWalletUpdated(agentId, newWallet);
    }

    /**
     * @notice Set agent wallet without signature (for owner/operator)
     * @param agentId Agent identifier
     * @param newWallet New wallet address
     */
    function setAgentWalletDirect(uint256 agentId, address newWallet) external {
        require(
            _isApprovedOrOwner(msg.sender, agentId) || _operators[agentId][_operatorEpoch[agentId]][msg.sender],
            "Not authorized"
        );
        require(newWallet != address(0), "Invalid wallet");

        _agentWallets[agentId] = newWallet;
        emit AgentWalletUpdated(agentId, newWallet);
    }

    /**
     * @inheritdoc IERC8004IdentityRegistry
     */
    function getAgentWallet(uint256 agentId) external view returns (address wallet) {
        return _agentWallets[agentId];
    }

    /**
     * @inheritdoc IERC8004IdentityRegistry
     */
    function setOperator(uint256 agentId, address operator, bool approved) external {
        require(_isApprovedOrOwner(msg.sender, agentId), "Not authorized");
        _operators[agentId][_operatorEpoch[agentId]][operator] = approved;
        emit OperatorSet(agentId, operator, approved);
    }

    /**
     * @inheritdoc IERC8004IdentityRegistry
     */
    function isOperator(uint256 agentId, address operator) external view returns (bool) {
        return _operators[agentId][_operatorEpoch[agentId]][operator];
    }

    /**
     * @notice Get agentId by PRXS peerID
     * @param peerID PRXS libp2p peer identifier
     * @return agentId Agent identifier (0 if not found)
     */
    function getAgentIdByPeerID(string calldata peerID) external view returns (uint256) {
        return peerIdToAgentId[peerID];
    }

    // ============ Internal Functions ============

    function _isApprovedOrOwner(address spender, uint256 tokenId) internal view returns (bool) {
        address owner = ownerOf(tokenId);
        return (spender == owner ||
                getApproved(tokenId) == spender ||
                isApprovedForAll(owner, spender));
    }

    /**
     * @dev Invalidate all prior operator approvals when token ownership changes.
     */
    function _update(address to, uint256 tokenId, address auth)
        internal
        override(ERC721)
        returns (address)
    {
        address previousOwner = super._update(to, tokenId, auth);
        if (previousOwner != address(0) && previousOwner != to) {
            unchecked {
                _operatorEpoch[tokenId]++;
            }
        }
        return previousOwner;
    }
}
