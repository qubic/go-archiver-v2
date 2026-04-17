package txstatus

import (
	"context"
	"testing"

	"github.com/qubic/go-node-connector/types"
	"github.com/stretchr/testify/require"
)

// helper to build a minimal transaction with known source/dest public keys
func makeTransaction(srcPubKey, dstPubKey [32]byte, amount int64, tick uint32) types.Transaction {
	return types.Transaction{
		SourcePublicKey:      srcPubKey,
		DestinationPublicKey: dstPubKey,
		Amount:               amount,
		Tick:                 tick,
	}
}

func TestValidate_HappyPath(t *testing.T) {
	tx := makeTransaction([32]byte{1}, [32]byte{2}, 100, 1000)
	txs := types.Transactions{tx}

	digest, err := tx.Digest()
	require.NoError(t, err)

	txStatus := types.TransactionStatus{
		CurrentTickOfNode:  1000,
		Tick:               1000,
		TxCount:            1,
		MoneyFlew:          [128]byte{0b00000001}, // first tx moneyFlew = true
		TransactionDigests: [][32]byte{digest},
	}

	result, err := ValidateAndConvert(context.Background(), txStatus, txs, false)
	require.NoError(t, err)
	require.Len(t, result.Transactions, 1)
	require.True(t, result.Transactions[0].MoneyFlew)
}

func TestValidate_TxCountMismatch(t *testing.T) {
	tx := makeTransaction([32]byte{1}, [32]byte{2}, 100, 1000)
	txs := types.Transactions{tx}

	// node returns TxCount=0 but we have 1 transaction
	txStatus := types.TransactionStatus{
		CurrentTickOfNode:  1000,
		Tick:               1000,
		TxCount:            0,
		MoneyFlew:          [128]byte{},
		TransactionDigests: nil,
	}

	_, err := ValidateAndConvert(context.Background(), txStatus, txs, true)
	require.Error(t, err)
}

func TestValidate_DigestMismatch(t *testing.T) {
	tx := makeTransaction([32]byte{1}, [32]byte{2}, 100, 1000)
	txs := types.Transactions{tx}

	// TxCount matches but digest is wrong
	txStatus := types.TransactionStatus{
		CurrentTickOfNode:  1000,
		Tick:               1000,
		TxCount:            1,
		MoneyFlew:          [128]byte{0b00000001},
		TransactionDigests: [][32]byte{{99, 99, 99}}, // wrong digest
	}

	_, err := ValidateAndConvert(context.Background(), txStatus, txs, true)
	require.Error(t, err)
}

func TestValidate_EmptyTick(t *testing.T) {
	txs := types.Transactions{}

	txStatus := types.TransactionStatus{
		CurrentTickOfNode:  1000,
		Tick:               1000,
		TxCount:            0,
		MoneyFlew:          [128]byte{},
		TransactionDigests: nil,
	}

	result, err := ValidateAndConvert(context.Background(), txStatus, txs, false)
	require.NoError(t, err)
	require.Empty(t, result.Transactions)
}

func TestValidateAndConvert_NoValidation(t *testing.T) {
	tx := makeTransaction([32]byte{1}, [32]byte{2}, 100, 1000)
	txs := types.Transactions{tx}

	// data that would fail validation
	txStatus := types.TransactionStatus{
		CurrentTickOfNode:  12345,
		Tick:               12345,
		TxCount:            1,           // any count
		MoneyFlew:          [128]byte{}, // no tx status data
		TransactionDigests: nil,         // no tx status digests
	}

	// should pass because validate=false
	result, err := ValidateAndConvert(t.Context(), txStatus, txs, false)
	require.NoError(t, err)
	require.Empty(t, result.Transactions)
}
