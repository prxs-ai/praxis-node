// SPDX-License-Identifier: MIT
pragma solidity ^0.8.24;

import { Test } from "forge-std/Test.sol";
import { PRXSIdentityRegistry } from "../src/PRXSIdentityRegistry.sol";

contract PRXSIdentityRegistryTest is Test {
    PRXSIdentityRegistry internal identity;

    address internal operator = address(0xBEEF);
    address internal newOwner = address(0xCAFE);

    function setUp() public {
        identity = new PRXSIdentityRegistry();
    }

    function test_OperatorRevokedOnTransfer() public {
        uint256 agentId = identity.registerProvider("peer-1", "ipfs://agent-1", address(0x1234));

        identity.setOperator(agentId, operator, true);
        assertTrue(identity.isOperator(agentId, operator));

        identity.transferFrom(address(this), newOwner, agentId);

        assertFalse(identity.isOperator(agentId, operator));

        vm.prank(operator);
        vm.expectRevert("Not authorized");
        identity.setMetadata(agentId, "region", bytes("eu"));
    }

    function test_NewOwnerCanSetOperatorAfterTransfer() public {
        uint256 agentId = identity.registerProvider("peer-2", "ipfs://agent-2", address(0x5678));

        identity.transferFrom(address(this), newOwner, agentId);

        vm.prank(newOwner);
        identity.setOperator(agentId, operator, true);

        assertTrue(identity.isOperator(agentId, operator));
    }
}

