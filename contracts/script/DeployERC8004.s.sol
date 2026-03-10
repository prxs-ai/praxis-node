// SPDX-License-Identifier: MIT
pragma solidity ^0.8.24;

import { Script } from "forge-std/Script.sol";
import { console } from "forge-std/console.sol";
import { PRXSIdentityRegistry } from "../src/PRXSIdentityRegistry.sol";
import { PRXSReputationRegistry } from "../src/PRXSReputationRegistry.sol";
import { PRXSValidationRegistry } from "../src/PRXSValidationRegistry.sol";

/**
 * @title DeployERC8004
 * @notice Deployment script for PRXS ERC-8004 registries
 * @dev Deploys Identity, Reputation, and Validation registries in correct order
 */
contract DeployERC8004 is Script {
    function run() external {
        // Read private key as string to support with/without 0x prefix
        string memory pkStr = vm.envString("PRIVATE_KEY");
        uint256 deployerPrivateKey;

        // Add 0x prefix if missing
        if (bytes(pkStr).length > 0 && bytes(pkStr)[0] == '0' && bytes(pkStr)[1] == 'x') {
            deployerPrivateKey = vm.parseUint(pkStr);
        } else {
            deployerPrivateKey = vm.parseUint(string.concat("0x", pkStr));
        }

        address deployer = vm.addr(deployerPrivateKey);

        console.log("Deploying ERC-8004 registries...");
        console.log("Deployer address:", deployer);
        console.log("Chain ID:", block.chainid);

        vm.startBroadcast(deployerPrivateKey);

        // 1. Deploy Identity Registry first (no dependencies)
        console.log("\n1. Deploying PRXSIdentityRegistry...");
        PRXSIdentityRegistry identityRegistry = new PRXSIdentityRegistry();
        console.log("PRXSIdentityRegistry deployed at:", address(identityRegistry));

        // 2. Deploy Reputation Registry (depends on Identity Registry)
        console.log("\n2. Deploying PRXSReputationRegistry...");
        PRXSReputationRegistry reputationRegistry = new PRXSReputationRegistry(
            address(identityRegistry)
        );
        console.log("PRXSReputationRegistry deployed at:", address(reputationRegistry));

        // 3. Deploy Validation Registry (depends on Identity Registry)
        console.log("\n3. Deploying PRXSValidationRegistry...");
        PRXSValidationRegistry validationRegistry = new PRXSValidationRegistry(
            address(identityRegistry)
        );
        console.log("PRXSValidationRegistry deployed at:", address(validationRegistry));

        vm.stopBroadcast();

        // Print summary
        console.log("\n===========================================");
        console.log("DEPLOYMENT SUMMARY");
        console.log("===========================================");
        console.log("Identity Registry:   ", address(identityRegistry));
        console.log("Reputation Registry: ", address(reputationRegistry));
        console.log("Validation Registry: ", address(validationRegistry));
        console.log("===========================================");

        // Save to .env format
        console.log("\nAdd these to your .env file:");
        console.log("IDENTITY_REGISTRY=", address(identityRegistry));
        console.log("REPUTATION_REGISTRY=", address(reputationRegistry));
        console.log("VALIDATION_REGISTRY=", address(validationRegistry));
    }
}
