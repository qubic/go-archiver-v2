package bob

import (
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

func TestComputeMoneyFlew_Executed(t *testing.T) {
	txHash := "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaafxib"
	tickResp := &bobTickResponse{
		Tick: 100,
		TickData: bobTickData{
			Epoch:              150,
			Tick:               100,
			TransactionDigests: []string{txHash},
		},
	}

	executedMap := map[string]bool{txHash: true}
	status, err := computeMoneyFlew(tickResp, executedMap, 100)
	require.NoError(t, err)
	require.Equal(t, uint32(1), status.TxCount)
	require.Len(t, status.TransactionDigests, 1)
	require.Equal(t, byte(1), status.MoneyFlew[0]&1, "moneyFlew should be true")
}

func TestComputeMoneyFlew_NotExecuted(t *testing.T) {
	txHash := "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaafxib"
	tickResp := &bobTickResponse{
		Tick: 100,
		TickData: bobTickData{
			Epoch:              150,
			Tick:               100,
			TransactionDigests: []string{txHash},
		},
	}

	executedMap := map[string]bool{txHash: false}
	status, err := computeMoneyFlew(tickResp, executedMap, 100)
	require.NoError(t, err)
	require.Equal(t, byte(0), status.MoneyFlew[0]&1, "moneyFlew should be false")
}

func TestComputeMoneyFlew_Pending(t *testing.T) {
	txHash := "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaafxib"
	tickResp := &bobTickResponse{
		Tick: 100,
		TickData: bobTickData{
			Epoch:              150,
			Tick:               100,
			TransactionDigests: []string{txHash},
		},
	}

	// Pending (null executed) → not in map → treated as false
	executedMap := map[string]bool{}
	status, err := computeMoneyFlew(tickResp, executedMap, 100)
	require.NoError(t, err)
	require.Equal(t, byte(0), status.MoneyFlew[0]&1, "moneyFlew should be false for pending")
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

	status, err := computeMoneyFlew(tickResp, nil, 100)
	require.NoError(t, err)
	require.Equal(t, uint32(0), status.TxCount)
	require.Empty(t, status.TransactionDigests)
	require.Equal(t, [128]byte{}, status.MoneyFlew)
}

func TestComputeMoneyFlew_MultipleTxs_MixedStatus(t *testing.T) {
	txHash1 := "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaafxib"
	txHash2 := "baaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaarmid"

	tickResp := &bobTickResponse{
		Tick: 100,
		TickData: bobTickData{
			Epoch:              150,
			Tick:               100,
			TransactionDigests: []string{txHash1, txHash2},
		},
	}

	executedMap := map[string]bool{
		txHash1: true,
		txHash2: false,
	}

	status, err := computeMoneyFlew(tickResp, executedMap, 100)
	require.NoError(t, err)
	require.Equal(t, uint32(2), status.TxCount)
	require.Equal(t, byte(1), status.MoneyFlew[0]&1, "first tx should be executed")
	require.Equal(t, byte(0), status.MoneyFlew[0]&2, "second tx should not be executed")
}
