package txstatus

import (
	"context"
	"testing"

	"github.com/qubic/go-node-connector/types"
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
	if err != nil {
		t.Fatalf("computing digest: %v", err)
	}

	txStatus := types.TransactionStatus{
		CurrentTickOfNode:  1000,
		Tick:               1000,
		TxCount:            1,
		MoneyFlew:          [128]byte{0b00000001}, // first tx moneyFlew = true
		TransactionDigests: [][32]byte{digest},
	}

	result, err := Validate(context.Background(), txStatus, txs)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(result.Transactions) != 1 {
		t.Fatalf("expected 1 transaction status, got %d", len(result.Transactions))
	}

	if !result.Transactions[0].MoneyFlew {
		t.Error("expected moneyFlew to be true")
	}
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

	_, err := Validate(context.Background(), txStatus, txs)
	if err == nil {
		t.Fatal("expected error for TxCount mismatch, got nil")
	}
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

	_, err := Validate(context.Background(), txStatus, txs)
	if err == nil {
		t.Fatal("expected error for digest mismatch, got nil")
	}
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

	result, err := Validate(context.Background(), txStatus, txs)
	if err != nil {
		t.Fatalf("unexpected error for empty tick: %v", err)
	}

	if len(result.Transactions) != 0 {
		t.Fatalf("expected 0 transaction statuses, got %d", len(result.Transactions))
	}
}
