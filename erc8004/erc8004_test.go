package erc8004

import (
	"math/big"
	"testing"
)

func TestUnpackAgentIDResult(t *testing.T) {
	identityABI, err := loadIdentityABI()
	if err != nil {
		t.Fatalf("failed to load identity ABI: %v", err)
	}

	method, ok := identityABI.Methods["getAgentIdByPeerID"]
	if !ok {
		t.Fatal("method getAgentIdByPeerID not found in ABI")
	}

	encoded, err := method.Outputs.Pack(big.NewInt(42))
	if err != nil {
		t.Fatalf("failed to encode output: %v", err)
	}

	agentID, err := unpackAgentIDResult(identityABI, encoded)
	if err != nil {
		t.Fatalf("failed to unpack agent id: %v", err)
	}
	if agentID.Cmp(big.NewInt(42)) != 0 {
		t.Fatalf("unexpected agent id: got %s want 42", agentID.String())
	}
}

func TestUnpackAgentIDResult_InvalidData(t *testing.T) {
	identityABI, err := loadIdentityABI()
	if err != nil {
		t.Fatalf("failed to load identity ABI: %v", err)
	}

	_, err = unpackAgentIDResult(identityABI, []byte{0x01, 0x02, 0x03})
	if err == nil {
		t.Fatal("expected unpack error for invalid data")
	}
}
