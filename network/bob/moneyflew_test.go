package bob

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/qubic/go-node-connector/types"
	"github.com/stretchr/testify/require"
)

// testIdentity is a known Qubic identity for testing.
const testIdentityA = "BAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAARMID"
const testIdentityB = "CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAACNKL"

func pubKeyForIdentity(t *testing.T, identity string, lowercase bool) [32]byte {
	t.Helper()
	id := types.Identity(identity)
	pk, err := id.ToPubKey(lowercase)
	require.NoError(t, err)
	return pk
}

func makeTx(t *testing.T, src, dst string, amount int64, tick uint32) types.Transaction {
	t.Helper()
	return types.Transaction{
		SourcePublicKey:      pubKeyForIdentity(t, src, false),
		DestinationPublicKey: pubKeyForIdentity(t, dst, false),
		Amount:               amount,
		Tick:                 tick,
		InputType:            0,
		InputSize:            0,
		Signature:            [64]byte{},
	}
}

func TestSetMoneyFlewBit(t *testing.T) {
	var bits [128]byte
	setMoneyFlewBit(&bits, 0)
	require.Equal(t, byte(1), bits[0])

	setMoneyFlewBit(&bits, 7)
	require.Equal(t, byte(0x81), bits[0])

	setMoneyFlewBit(&bits, 8)
	require.Equal(t, byte(1), bits[1])
}

func TestFetchFirstQUTransfers(t *testing.T) {
	logs := []bobLogEvent{
		{OK: true, Type: 0, Tick: 100, TxHash: "txhash1", Body: bobLogBody{From: "SRC1", To: "DST1", Amount: 500}},
		{OK: true, Type: 0, Tick: 100, TxHash: "txhash1", Body: bobLogBody{From: "SRC1", To: "DST1", Amount: 200}}, // second event, should be ignored
		{OK: true, Type: 1, Tick: 100, TxHash: "txhash2", Body: bobLogBody{}},                                       // not QU_TRANSFER
		{OK: true, Type: 0, Tick: 100, TxHash: "txhash3", Body: bobLogBody{From: "SRC3", To: "DST3", Amount: 1000}},
		{OK: false, Type: 0, Tick: 100, TxHash: "txhash4", Body: bobLogBody{From: "SRC4", To: "DST4", Amount: 100}}, // not OK
	}

	logsJSON, err := json.Marshal(logs)
	require.NoError(t, err)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write(logsJSON)
	}))
	defer server.Close()

	client := NewClient(server.URL)
	result, err := fetchFirstQUTransfers(context.Background(), client, 150, 1, 5)
	require.NoError(t, err)

	// txhash1: first event kept (amount 500), second ignored
	require.Contains(t, result, "txhash1")
	require.Equal(t, int64(500), result["txhash1"].Amount)
	require.Equal(t, "SRC1", result["txhash1"].Source)
	require.Equal(t, "DST1", result["txhash1"].Destination)

	// txhash2: not QU_TRANSFER, not included
	require.NotContains(t, result, "txhash2")

	// txhash3: included
	require.Contains(t, result, "txhash3")
	require.Equal(t, int64(1000), result["txhash3"].Amount)

	// txhash4: not OK, not included
	require.NotContains(t, result, "txhash4")
}

func TestComputeMoneyFlew_MatchingTransfer(t *testing.T) {
	tx := makeTx(t, testIdentityA, testIdentityB, 1000, 100)
	txID, err := tx.ID()
	require.NoError(t, err)

	// The tx digest as qubic hash (lowercase)
	txDigest, err := tx.Digest()
	require.NoError(t, err)
	var digestID types.Identity
	digestID, err = digestID.FromPubKey(txDigest, true)
	require.NoError(t, err)
	digestHash := digestID.String()

	logs := []bobLogEvent{
		{
			OK:     true,
			Type:   0,
			Tick:   100,
			TxHash: txID,
			Body: bobLogBody{
				From:   testIdentityA,
				To:     testIdentityB,
				Amount: 1000,
			},
		},
	}
	logsJSON, _ := json.Marshal(logs)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write(logsJSON)
	}))
	defer server.Close()

	client := NewClient(server.URL)
	tickResp := &bobTickResponse{
		Tick: 100,
		TickData: bobTickData{
			Epoch:              150,
			Tick:               100,
			LogIdStart:         1,
			LogIdEnd:           1,
			TransactionDigests: []string{digestHash},
		},
	}

	status, err := computeMoneyFlew(context.Background(), client, tickResp, types.Transactions{tx}, 100)
	require.NoError(t, err)
	require.Equal(t, uint32(1), status.TxCount)
	require.Len(t, status.TransactionDigests, 1)
	// Bit 0 should be set (moneyFlew = true)
	require.Equal(t, byte(1), status.MoneyFlew[0]&1)
}

func TestComputeMoneyFlew_ZeroAmount(t *testing.T) {
	tx := makeTx(t, testIdentityA, testIdentityB, 0, 100)

	txDigest, err := tx.Digest()
	require.NoError(t, err)
	var digestID types.Identity
	digestID, err = digestID.FromPubKey(txDigest, true)
	require.NoError(t, err)
	digestHash := digestID.String()

	// Even with a matching log, amount=0 should give moneyFlew=false
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte("[]"))
	}))
	defer server.Close()

	client := NewClient(server.URL)
	tickResp := &bobTickResponse{
		Tick: 100,
		TickData: bobTickData{
			Epoch:              150,
			Tick:               100,
			LogIdStart:         1,
			LogIdEnd:           1,
			TransactionDigests: []string{digestHash},
		},
	}

	status, err := computeMoneyFlew(context.Background(), client, tickResp, types.Transactions{tx}, 100)
	require.NoError(t, err)
	require.Equal(t, byte(0), status.MoneyFlew[0]&1, "moneyFlew should be false for zero-amount tx")
}

func TestComputeMoneyFlew_MismatchedAmount(t *testing.T) {
	tx := makeTx(t, testIdentityA, testIdentityB, 1000, 100)
	txID, err := tx.ID()
	require.NoError(t, err)

	txDigest, err := tx.Digest()
	require.NoError(t, err)
	var digestID types.Identity
	digestID, err = digestID.FromPubKey(txDigest, true)
	require.NoError(t, err)
	digestHash := digestID.String()

	// Log has different amount -> moneyFlew = false
	logs := []bobLogEvent{
		{OK: true, Type: 0, Tick: 100, TxHash: txID, Body: bobLogBody{
			From: testIdentityA, To: testIdentityB, Amount: 999,
		}},
	}
	logsJSON, _ := json.Marshal(logs)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write(logsJSON)
	}))
	defer server.Close()

	client := NewClient(server.URL)
	tickResp := &bobTickResponse{
		Tick: 100,
		TickData: bobTickData{
			Epoch: 150, Tick: 100, LogIdStart: 1, LogIdEnd: 1,
			TransactionDigests: []string{digestHash},
		},
	}

	status, err := computeMoneyFlew(context.Background(), client, tickResp, types.Transactions{tx}, 100)
	require.NoError(t, err)
	require.Equal(t, byte(0), status.MoneyFlew[0]&1, "moneyFlew should be false when amount mismatches")
}

func TestComputeMoneyFlew_NoLogs(t *testing.T) {
	tx := makeTx(t, testIdentityA, testIdentityB, 1000, 100)

	txDigest, err := tx.Digest()
	require.NoError(t, err)
	var digestID types.Identity
	digestID, err = digestID.FromPubKey(txDigest, true)
	require.NoError(t, err)
	digestHash := digestID.String()

	// logIdStart < 0 means no logs for this tick
	tickResp := &bobTickResponse{
		Tick: 100,
		TickData: bobTickData{
			Epoch: 150, Tick: 100, LogIdStart: -1, LogIdEnd: -1,
			TransactionDigests: []string{digestHash},
		},
	}

	// No HTTP call should be made
	status, err := computeMoneyFlew(context.Background(), nil, tickResp, types.Transactions{tx}, 100)
	require.NoError(t, err)
	require.Equal(t, byte(0), status.MoneyFlew[0]&1, "moneyFlew should be false when no logs")
}

func TestComputeMoneyFlew_MismatchedDestination(t *testing.T) {
	tx := makeTx(t, testIdentityA, testIdentityB, 1000, 100)
	txID, err := tx.ID()
	require.NoError(t, err)

	txDigest, err := tx.Digest()
	require.NoError(t, err)
	var digestID types.Identity
	digestID, err = digestID.FromPubKey(txDigest, true)
	require.NoError(t, err)
	digestHash := digestID.String()

	// Log has correct src and amount but wrong destination
	logs := []bobLogEvent{
		{OK: true, Type: 0, Tick: 100, TxHash: txID, Body: bobLogBody{
			From: testIdentityA, To: testIdentityA, Amount: 1000, // wrong dst
		}},
	}
	logsJSON, _ := json.Marshal(logs)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, string(logsJSON))
	}))
	defer server.Close()

	client := NewClient(server.URL)
	tickResp := &bobTickResponse{
		Tick: 100,
		TickData: bobTickData{
			Epoch: 150, Tick: 100, LogIdStart: 1, LogIdEnd: 1,
			TransactionDigests: []string{digestHash},
		},
	}

	status, err := computeMoneyFlew(context.Background(), client, tickResp, types.Transactions{tx}, 100)
	require.NoError(t, err)
	require.Equal(t, byte(0), status.MoneyFlew[0]&1, "moneyFlew should be false when destination mismatches")
}

func TestComputeMoneyFlew_EmptyTick(t *testing.T) {
	tickResp := &bobTickResponse{
		Tick: 100,
		TickData: bobTickData{
			Epoch: 150, Tick: 100, LogIdStart: -1, LogIdEnd: -1,
			TransactionDigests: nil,
		},
	}

	status, err := computeMoneyFlew(context.Background(), nil, tickResp, nil, 100)
	require.NoError(t, err)
	require.Equal(t, uint32(0), status.TxCount)
	require.Empty(t, status.TransactionDigests)
	require.Equal(t, [128]byte{}, status.MoneyFlew)
}
