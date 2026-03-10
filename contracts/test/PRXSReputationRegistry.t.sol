// SPDX-License-Identifier: MIT
pragma solidity ^0.8.24;

import { Test } from "forge-std/Test.sol";
import { PRXSIdentityRegistry } from "../src/PRXSIdentityRegistry.sol";
import { PRXSReputationRegistry } from "../src/PRXSReputationRegistry.sol";

contract PRXSReputationRegistryTest is Test {
    PRXSIdentityRegistry internal identity;
    PRXSReputationRegistry internal reputation;

    uint256 internal agentId;
    address internal reviewer = address(0xABCD);

    function setUp() public {
        identity = new PRXSIdentityRegistry();
        reputation = new PRXSReputationRegistry(address(identity));
        agentId = identity.registerProvider("peer-1", "ipfs://agent-1", address(0));
    }

    function test_GiveFeedbackAndSummary() public {
        vm.prank(reviewer);
        reputation.giveFeedback(
            agentId,
            int128(int256(90)),
            0,
            "uptime",
            "prxs_registry",
            "/api/v1/run",
            "ipfs://feedback/1",
            keccak256(bytes("feedback-1"))
        );

        address[] memory clients = new address[](1);
        clients[0] = reviewer;

        (uint256 count, int128 summaryValue, uint8 summaryDecimals) = reputation.getSummary(
            agentId,
            clients,
            "uptime",
            "prxs_registry"
        );

        assertEq(count, 1);
        assertEq(summaryValue, 90);
        assertEq(summaryDecimals, 2);
    }

    function test_SelfFeedbackReverts() public {
        vm.expectRevert("Cannot give self-feedback");
        reputation.giveFeedback(
            agentId,
            int128(int256(80)),
            0,
            "uptime",
            "prxs_registry",
            "/api/v1/run",
            "ipfs://feedback/self",
            keccak256(bytes("self-feedback"))
        );
    }
}
