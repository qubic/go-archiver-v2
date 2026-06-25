package txstatus

import (
	"testing"

	"github.com/qubic/go-node-connector/v2/types"
	"github.com/stretchr/testify/require"
)

func TestQubicToProto(t *testing.T) {
	tickTransactionStatus := types.TransactionStatus{
		CurrentTickOfNode: 100,
		Tick:              100,
		TxCount:           3,
		MoneyFlew:         [(types.NumberOfTransactionsPerTick + 7) / 8]byte{0b10001000, 0b00000001},
		TransactionDigests: [][32]byte{
			{209, 173, 239, 194, 151, 98, 29, 180, 83, 67, 142, 32, 4, 9, 167, 32, 159, 95, 116, 116, 214, 221, 171, 255, 13, 125, 86, 112, 5, 31, 191, 193},
			{230, 252, 58, 173, 75, 89, 77, 130, 191, 49, 3, 161, 16, 22, 216, 13, 232, 131, 222, 135, 59, 206, 196, 142, 144, 57, 98, 134, 80, 59, 38, 19},
			{230, 252, 58, 173, 75, 89, 77, 130, 191, 49, 3, 161, 16, 22, 216, 13, 232, 131, 222, 135, 59, 206, 196, 142, 144, 57, 98, 134, 80, 59, 38, 21},
			{230, 252, 58, 173, 75, 89, 77, 130, 191, 49, 3, 161, 16, 22, 216, 13, 232, 131, 222, 135, 59, 206, 196, 142, 144, 57, 98, 134, 80, 59, 38, 19},
			{209, 173, 239, 194, 151, 98, 29, 180, 83, 67, 142, 32, 4, 9, 167, 32, 159, 95, 116, 116, 214, 221, 171, 255, 13, 125, 86, 112, 5, 31, 191, 193},
			{230, 252, 58, 173, 75, 89, 77, 130, 191, 49, 3, 161, 16, 22, 216, 13, 232, 131, 222, 135, 59, 206, 196, 142, 144, 57, 98, 134, 80, 59, 38, 19},
			{230, 252, 58, 173, 75, 89, 77, 130, 191, 49, 3, 161, 16, 22, 216, 13, 232, 131, 222, 135, 59, 206, 196, 142, 144, 57, 98, 134, 80, 59, 38, 21},
			{230, 252, 58, 173, 75, 89, 77, 130, 191, 49, 3, 161, 16, 22, 216, 13, 232, 131, 222, 135, 59, 206, 196, 142, 144, 57, 98, 134, 80, 59, 38, 21},
			{230, 252, 58, 173, 75, 89, 77, 130, 191, 49, 3, 161, 16, 22, 216, 13, 232, 131, 222, 135, 59, 206, 196, 142, 144, 57, 98, 134, 80, 59, 38, 19},
		},
	}

	// No transactions are supplied, so every digest is filtered out (a tx must be
	// present in the tick to get a stored status). The result must be empty, not a
	// list of statuses with arbitrary moneyFlew bits.
	res, err := qubicToProto(types.Transactions{}, tickTransactionStatus)
	require.NoError(t, err)
	require.Empty(t, res.Transactions, "digests with no matching transaction must be filtered out")
}

func TestEqualSlices(t *testing.T) {
	slice1 := [][32]byte{
		{21, 22, 23, 24, 25, 26, 27, 28, 29, 30},
		{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
		{11, 12, 13, 14, 15, 16, 17, 18, 19, 20},
	}

	slice2 := [][32]byte{
		{11, 12, 13, 14, 15, 16, 17, 18, 19, 20},
		{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
		{21, 22, 23, 24, 25, 26, 27, 28, 29, 30},
	}

	if !equalDigests(slice1, slice2) {
		t.Fatalf("Slices are not equal")
	}

	// check if the original slices were not changed
	if slice1[0] != [32]byte{21, 22, 23, 24, 25, 26, 27, 28, 29, 30} {
		t.Fatalf("Original slice was changed")
	}
	if slice2[0] != [32]byte{11, 12, 13, 14, 15, 16, 17, 18, 19, 20} {
		t.Fatalf("Original slice was changed")
	}
	if slice1[1] != [32]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10} {
		t.Fatalf("Original slice was changed")
	}
	if slice2[1] != [32]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10} {
		t.Fatalf("Original slice was changed")
	}
	if slice1[2] != [32]byte{11, 12, 13, 14, 15, 16, 17, 18, 19, 20} {
		t.Fatalf("Original slice was changed")
	}
	if slice2[2] != [32]byte{21, 22, 23, 24, 25, 26, 27, 28, 29, 30} {
		t.Fatalf("Original slice was changed")
	}

}
