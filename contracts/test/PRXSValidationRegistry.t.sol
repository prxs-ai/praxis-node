// SPDX-License-Identifier: MIT
pragma solidity ^0.8.24;

import { Test } from "forge-std/Test.sol";
import { PRXSIdentityRegistry } from "../src/PRXSIdentityRegistry.sol";
import { PRXSValidationRegistry } from "../src/PRXSValidationRegistry.sol";

contract PRXSValidationRegistryTest is Test {
    PRXSIdentityRegistry internal identity;
    PRXSValidationRegistry internal validation;

    uint256 internal agentId;
    address internal validator = address(0xD00D);

    function setUp() public {
        identity = new PRXSIdentityRegistry();
        validation = new PRXSValidationRegistry(address(identity));
        agentId = identity.registerProvider("peer-1", "ipfs://agent-1", address(0));
    }

    function test_PendingValidationNotCountedInSummary() public {
        bytes32 requestHash = keccak256(abi.encodePacked("pending", uint256(1)));
        validation.validationRequest(validator, agentId, "ipfs://req/1", requestHash);

        address[] memory validators = new address[](0);
        (uint256 count, uint256 average) = validation.getSummary(agentId, validators, "");

        assertEq(count, 0);
        assertEq(average, 0);
    }

    function test_AnsweredValidationCountedInSummary() public {
        bytes32 requestHash = keccak256(abi.encodePacked("answered", uint256(1)));
        validation.validationRequest(validator, agentId, "ipfs://req/2", requestHash);

        vm.prank(validator);
        validation.validationResponse(
            requestHash,
            80,
            "ipfs://resp/2",
            keccak256(bytes("ipfs://resp/2")),
            "compute_verification"
        );

        address[] memory validators = new address[](0);
        (uint256 count, uint256 average) = validation.getSummary(agentId, validators, "");

        assertEq(count, 1);
        assertEq(average, 80);
    }

    function test_SummaryIsBoundedForLargeHistory() public {
        for (uint256 i = 0; i < 1001; i++) {
            bytes32 requestHash = keccak256(abi.encodePacked("bounded", i));
            validation.validationRequest(validator, agentId, "ipfs://req", requestHash);

            uint8 score = i == 0 ? 0 : 100;
            vm.prank(validator);
            validation.validationResponse(
                requestHash,
                score,
                "ipfs://resp",
                keccak256(abi.encodePacked("proof", i)),
                "compute_verification"
            );
        }

        address[] memory validators = new address[](0);
        (uint256 count, uint256 average) = validation.getSummary(agentId, validators, "");

        // getSummary scans only latest 1000 requests
        assertEq(count, 1000);
        assertEq(average, 100);
    }

    function test_GetSummaryPaginated() public {
        bytes32 h1 = keccak256(abi.encodePacked("page", uint256(1)));
        bytes32 h2 = keccak256(abi.encodePacked("page", uint256(2)));
        bytes32 h3 = keccak256(abi.encodePacked("page", uint256(3)));

        validation.validationRequest(validator, agentId, "ipfs://req/1", h1);
        validation.validationRequest(validator, agentId, "ipfs://req/2", h2);
        validation.validationRequest(validator, agentId, "ipfs://req/3", h3);

        vm.prank(validator);
        validation.validationResponse(h1, 20, "ipfs://resp/1", keccak256(bytes("r1")), "compute");
        vm.prank(validator);
        validation.validationResponse(h2, 40, "ipfs://resp/2", keccak256(bytes("r2")), "compute");
        vm.prank(validator);
        validation.validationResponse(h3, 60, "ipfs://resp/3", keccak256(bytes("r3")), "compute");

        address[] memory validators = new address[](0);
        (uint256 count, uint256 average, uint256 scanned) = validation.getSummaryPaginated(
            agentId,
            validators,
            "",
            1,
            2
        );

        assertEq(scanned, 2);
        assertEq(count, 2);
        assertEq(average, 50);
    }
}
