package bob

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSetMoneyFlewBit(t *testing.T) {
	var bits [128]byte
	setMoneyFlewBit(&bits, 0)
	require.Equal(t, byte(1), bits[0])

	setMoneyFlewBit(&bits, 7)
	require.Equal(t, byte(0x81), bits[0])

	setMoneyFlewBit(&bits, 8)
	require.Equal(t, byte(1), bits[1])
}

func TestFetchTxExecuted_True(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `{"hash":"abc","executed":true}`)
	}))
	defer server.Close()

	client := NewClient(server.URL)
	executed, err := fetchTxExecuted(context.Background(), client, "abc")
	require.NoError(t, err)
	require.True(t, executed)
}

func TestFetchTxExecuted_False(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `{"hash":"abc","executed":false}`)
	}))
	defer server.Close()

	client := NewClient(server.URL)
	executed, err := fetchTxExecuted(context.Background(), client, "abc")
	require.NoError(t, err)
	require.False(t, executed)
}

func TestFetchTxExecuted_NotIndexed(t *testing.T) {
	// When hasIndexedInfo is false, "executed" field is absent
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `{"hash":"abc","amount":1000}`)
	}))
	defer server.Close()

	client := NewClient(server.URL)
	executed, err := fetchTxExecuted(context.Background(), client, "abc")
	require.NoError(t, err)
	require.False(t, executed)
}

func TestFetchTxExecuted_Error(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `{"error":"Transaction not found"}`)
	}))
	defer server.Close()

	client := NewClient(server.URL)
	executed, err := fetchTxExecuted(context.Background(), client, "abc")
	require.NoError(t, err)
	require.False(t, executed)
}

func TestComputeMoneyFlew_WithExecutedTx(t *testing.T) {
	// Use a known qubic hash as the tx digest
	txHash := "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaafxib"

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.URL.Path, "/tx/") {
			fmt.Fprint(w, `{"hash":"abc","executed":true}`)
		}
	}))
	defer server.Close()

	client := NewClient(server.URL)
	tickResp := &bobTickResponse{
		Tick: 100,
		TickData: bobTickData{
			Epoch:              150,
			Tick:               100,
			TransactionDigests: []string{txHash},
		},
	}

	status, err := computeMoneyFlew(context.Background(), client, tickResp, 100)
	require.NoError(t, err)
	require.Equal(t, uint32(1), status.TxCount)
	require.Len(t, status.TransactionDigests, 1)
	require.Equal(t, byte(1), status.MoneyFlew[0]&1, "moneyFlew should be true")
}

func TestComputeMoneyFlew_WithNotExecutedTx(t *testing.T) {
	txHash := "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaafxib"

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.URL.Path, "/tx/") {
			fmt.Fprint(w, `{"hash":"abc","executed":false}`)
		}
	}))
	defer server.Close()

	client := NewClient(server.URL)
	tickResp := &bobTickResponse{
		Tick: 100,
		TickData: bobTickData{
			Epoch:              150,
			Tick:               100,
			TransactionDigests: []string{txHash},
		},
	}

	status, err := computeMoneyFlew(context.Background(), client, tickResp, 100)
	require.NoError(t, err)
	require.Equal(t, byte(0), status.MoneyFlew[0]&1, "moneyFlew should be false")
}

func TestComputeMoneyFlew_EmptyTick(t *testing.T) {
	tickResp := &bobTickResponse{
		Tick: 100,
		TickData: bobTickData{
			Epoch:              150,
			Tick:               100,
			TransactionDigests: nil,
		},
	}

	status, err := computeMoneyFlew(context.Background(), nil, tickResp, 100)
	require.NoError(t, err)
	require.Equal(t, uint32(0), status.TxCount)
	require.Empty(t, status.TransactionDigests)
	require.Equal(t, [128]byte{}, status.MoneyFlew)
}

func TestComputeMoneyFlew_MultipleTxs_MixedStatus(t *testing.T) {
	txHash1 := "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaafxib"
	txHash2 := "baaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaarmid"

	callCount := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		callCount++
		if callCount == 1 {
			fmt.Fprint(w, `{"hash":"tx1","executed":true}`)
		} else {
			fmt.Fprint(w, `{"hash":"tx2","executed":false}`)
		}
	}))
	defer server.Close()

	client := NewClient(server.URL)
	tickResp := &bobTickResponse{
		Tick: 100,
		TickData: bobTickData{
			Epoch:              150,
			Tick:               100,
			TransactionDigests: []string{txHash1, txHash2},
		},
	}

	status, err := computeMoneyFlew(context.Background(), client, tickResp, 100)
	require.NoError(t, err)
	require.Equal(t, uint32(2), status.TxCount)
	require.Equal(t, byte(1), status.MoneyFlew[0]&1, "first tx should be executed")
	require.Equal(t, byte(0), status.MoneyFlew[0]&2, "second tx should not be executed")
}
