package bob

import (
	"context"
	"testing"

	"github.com/qubic/go-archiver-v2/validator/txstatus"
	"github.com/qubic/go-node-connector/v2/types"
	"github.com/stretchr/testify/require"
)

// TestComputeMoneyFlew_ToStoredStatus_Mixed is the end-to-end check the bit-array
// tests don't cover: it runs bob's executed flags through ComputeMoneyFlew and then
// txstatus.ValidateAndConvert (the same two steps the validator runs) and asserts
// the per-transaction MoneyFlew bool that actually gets stored. It guards the index
// alignment across the bob -> txstatus boundary for a mixed executed=[true,false,true].
func TestComputeMoneyFlew_ToStoredStatus_Mixed(t *testing.T) {
	tx0, hash0 := makeTx(t, 1)
	tx1, hash1 := makeTx(t, 2)
	tx2, hash2 := makeTx(t, 3)
	txs := types.Transactions{tx0, tx1, tx2}

	// bob's view: tx0 and tx2 executed, tx1 did not.
	bobTick := TickData{executed: map[string]bool{
		hash0: true,
		hash1: false,
		hash2: true,
	}}

	status, err := ComputeMoneyFlew(bobTick, 100, txs)
	require.NoError(t, err)

	// validate=true so the count + digest checks run, mirroring the live path.
	stored, err := txstatus.ValidateAndConvert(context.Background(), status, txs, true)
	require.NoError(t, err)
	require.Len(t, stored.Transactions, 3)

	id0, err := tx0.ID()
	require.NoError(t, err)
	id1, err := tx1.ID()
	require.NoError(t, err)
	id2, err := tx2.ID()
	require.NoError(t, err)

	got := make(map[string]bool, len(stored.Transactions))
	for _, ts := range stored.Transactions {
		got[ts.TxId] = ts.MoneyFlew
	}
	require.True(t, got[id0], "tx0 executed=true -> stored moneyFlew=true")
	require.False(t, got[id1], "tx1 executed=false -> stored moneyFlew=false")
	require.True(t, got[id2], "tx2 executed=true -> stored moneyFlew=true")
}
