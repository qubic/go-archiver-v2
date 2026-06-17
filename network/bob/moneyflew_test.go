package bob

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/qubic/go-node-connector/v2/types"
	"github.com/stretchr/testify/require"
)

// makeTx returns a deterministic Transaction whose digest varies with `nonce`.
// The amount field is used so that each call produces a distinct digest.
func makeTx(t *testing.T, nonce int64) (types.Transaction, string) {
	t.Helper()
	tx := types.Transaction{
		Amount:    nonce,
		Tick:      100,
		InputType: 0,
		InputSize: 0,
	}
	digest, err := tx.Digest()
	require.NoError(t, err)
	var id types.Identity
	hashID, err := id.FromPubKey(digest, true)
	require.NoError(t, err)
	return tx, strings.ToLower(hashID.String())
}

// bobServer spins up an httptest.Server that responds to qubic_getTickByNumber
// with the provided transactions and returns a Client pointing at it.
func bobServer(t *testing.T, txs []bobRPCTransaction) (*Client, func()) {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req jsonRPCRequest
		require.NoError(t, json.NewDecoder(r.Body).Decode(&req))
		require.Equal(t, "qubic_getTickByNumber", req.Method)

		result, err := json.Marshal(bobRPCTickResponse{TickNumber: 100, Transactions: txs})
		require.NoError(t, err)

		require.NoError(t, json.NewEncoder(w).Encode(jsonRPCResponse{
			JSONRPC: "2.0",
			ID:      req.ID,
			Result:  result,
		}))
	}))
	return NewClient(srv.URL, nil), srv.Close
}

func ptrBool(b bool) *bool { return &b }

func TestGetMoneyFlew_AllExecuted(t *testing.T) {
	tx1, hash1 := makeTx(t, 1)
	tx2, hash2 := makeTx(t, 2)

	client, stop := bobServer(t, []bobRPCTransaction{
		{Hash: hash1, Executed: ptrBool(true)},
		{Hash: hash2, Executed: ptrBool(true)},
	})
	defer stop()

	status, err := GetMoneyFlew(context.Background(), client, 100, []types.Transaction{tx1, tx2})
	require.NoError(t, err)
	require.Equal(t, uint32(2), status.TxCount)
	require.Equal(t, uint32(100), status.Tick)
	require.True(t, status.MoneyFlew[0]&0b01 != 0, "bit 0 set")
	require.True(t, status.MoneyFlew[0]&0b10 != 0, "bit 1 set")
}

func TestGetMoneyFlew_NoneExecuted(t *testing.T) {
	tx1, hash1 := makeTx(t, 10)
	tx2, hash2 := makeTx(t, 11)

	client, stop := bobServer(t, []bobRPCTransaction{
		{Hash: hash1, Executed: ptrBool(false)},
		{Hash: hash2, Executed: ptrBool(false)},
	})
	defer stop()

	status, err := GetMoneyFlew(context.Background(), client, 100, []types.Transaction{tx1, tx2})
	require.NoError(t, err)
	for _, b := range status.MoneyFlew {
		require.Equal(t, byte(0), b)
	}
}

func TestGetMoneyFlew_Mixed(t *testing.T) {
	tx1, hash1 := makeTx(t, 100)
	tx2, hash2 := makeTx(t, 101)
	tx3, hash3 := makeTx(t, 102)

	client, stop := bobServer(t, []bobRPCTransaction{
		{Hash: hash1, Executed: ptrBool(true)},
		{Hash: hash2, Executed: ptrBool(false)},
		{Hash: hash3, Executed: ptrBool(true)},
	})
	defer stop()

	status, err := GetMoneyFlew(context.Background(), client, 100, []types.Transaction{tx1, tx2, tx3})
	require.NoError(t, err)
	require.Equal(t, byte(0b101), status.MoneyFlew[0])
}

func TestGetMoneyFlew_CountMismatch(t *testing.T) {
	tx1, hash1 := makeTx(t, 1)
	tx2, _ := makeTx(t, 2)

	client, stop := bobServer(t, []bobRPCTransaction{
		{Hash: hash1, Executed: ptrBool(true)},
	})
	defer stop()

	_, err := GetMoneyFlew(context.Background(), client, 100, []types.Transaction{tx1, tx2})
	require.Error(t, err)
	require.Contains(t, err.Error(), "count mismatch")
}

func TestGetMoneyFlew_DigestMissing(t *testing.T) {
	tx1, _ := makeTx(t, 1)
	_, hashOther := makeTx(t, 999)

	client, stop := bobServer(t, []bobRPCTransaction{
		{Hash: hashOther, Executed: ptrBool(true)},
	})
	defer stop()

	_, err := GetMoneyFlew(context.Background(), client, 100, []types.Transaction{tx1})
	require.Error(t, err)
	require.Contains(t, err.Error(), "missing from bob")
}

func TestGetMoneyFlew_PendingFails(t *testing.T) {
	tx1, hash1 := makeTx(t, 1)

	client, stop := bobServer(t, []bobRPCTransaction{
		{Hash: hash1, Executed: nil}, // pending
	})
	defer stop()

	_, err := GetMoneyFlew(context.Background(), client, 100, []types.Transaction{tx1})
	require.Error(t, err)
	require.Contains(t, err.Error(), "pending")
}

func TestGetMoneyFlew_EmptyTick_NoRPC(t *testing.T) {
	rpcCalled := false
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rpcCalled = true
		http.Error(w, "should not be called", http.StatusInternalServerError)
	}))
	defer srv.Close()

	status, err := GetMoneyFlew(context.Background(), NewClient(srv.URL, nil), 100, []types.Transaction{})
	require.NoError(t, err)
	require.Equal(t, uint32(0), status.TxCount)
	require.Equal(t, uint32(100), status.Tick)
	require.False(t, rpcCalled, "empty tick must not hit bob")
}

func TestGetMoneyFlew_HashCaseInsensitive(t *testing.T) {
	tx1, hash1 := makeTx(t, 7)

	client, stop := bobServer(t, []bobRPCTransaction{
		{Hash: strings.ToUpper(hash1), Executed: ptrBool(true)},
	})
	defer stop()

	status, err := GetMoneyFlew(context.Background(), client, 100, []types.Transaction{tx1})
	require.NoError(t, err)
	require.Equal(t, byte(1), status.MoneyFlew[0])
}

func TestGetMoneyFlew_RPCError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.NoError(t, json.NewEncoder(w).Encode(jsonRPCResponse{
			JSONRPC: "2.0",
			ID:      1,
			Error:   &jsonRPCError{Code: -32603, Message: "internal error"},
		}))
	}))
	defer srv.Close()

	tx1, _ := makeTx(t, 1)
	_, err := GetMoneyFlew(context.Background(), NewClient(srv.URL, nil), 100, []types.Transaction{tx1})
	require.Error(t, err)
	require.Contains(t, err.Error(), fmt.Sprintf("tick=%d", 100))
}
