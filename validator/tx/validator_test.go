package tx

import (
	"context"
	"github.com/cockroachdb/pebble"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/qubic/go-archiver/db"
	"github.com/qubic/go-archiver/protobuf"
	"github.com/qubic/go-node-connector/types"
	"github.com/stretchr/testify/require"
	"os"
	"path/filepath"
	"testing"
)

func Test_CreateTransferTransactionsIdentityMap(t *testing.T) {
	txs := []*protobuf.Transaction{
		{
			SourceId: "QJRRSSKMJRDKUDTYVNYGAMQPULKAMILQQYOWBEXUDEUWQUMNGDHQYLOAJMEB",
			DestId:   "IXTSDANOXIVIWGNDCNZVWSAVAEPBGLGSQTLSVHHBWEGKSEKPRQGWIJJCTUZB",
		},
		{
			SourceId: "IXTSDANOXIVIWGNDCNZVWSAVAEPBGLGSQTLSVHHBWEGKSEKPRQGWIJJCTUZB",
			DestId:   "QJRRSSKMJRDKUDTYVNYGAMQPULKAMILQQYOWBEXUDEUWQUMNGDHQYLOAJMEB",
		},
		{
			SourceId: "AXTSDANOXIVIWGNDCNZVWSAVAEPBGLGSQTLSVHHBWEGKSEKPRQGWIJJCTUZB",
			DestId:   "BJRRSSKMJRDKUDTYVNYGAMQPULKAMILQQYOWBEXUDEUWQUMNGDHQYLOAJMEB",
		},
		{
			SourceId: "QJRRSSKMJRDKUDTYVNYGAMQPULKAMILQQYOWBEXUDEUWQUMNGDHQYLOAJMEB",
			DestId:   "ZXTSDANOXIVIWGNDCNZVWSAVAEPBGLGSQTLSVHHBWEGKSEKPRQGWIJJCTUZB",
		},
	}

	expected := map[string][]*protobuf.Transaction{
		"QJRRSSKMJRDKUDTYVNYGAMQPULKAMILQQYOWBEXUDEUWQUMNGDHQYLOAJMEB": {
			{
				SourceId: "QJRRSSKMJRDKUDTYVNYGAMQPULKAMILQQYOWBEXUDEUWQUMNGDHQYLOAJMEB",
				DestId:   "IXTSDANOXIVIWGNDCNZVWSAVAEPBGLGSQTLSVHHBWEGKSEKPRQGWIJJCTUZB",
			},
			{
				SourceId: "IXTSDANOXIVIWGNDCNZVWSAVAEPBGLGSQTLSVHHBWEGKSEKPRQGWIJJCTUZB",
				DestId:   "QJRRSSKMJRDKUDTYVNYGAMQPULKAMILQQYOWBEXUDEUWQUMNGDHQYLOAJMEB",
			},
			{
				SourceId: "QJRRSSKMJRDKUDTYVNYGAMQPULKAMILQQYOWBEXUDEUWQUMNGDHQYLOAJMEB",
				DestId:   "ZXTSDANOXIVIWGNDCNZVWSAVAEPBGLGSQTLSVHHBWEGKSEKPRQGWIJJCTUZB",
			},
		},
		"IXTSDANOXIVIWGNDCNZVWSAVAEPBGLGSQTLSVHHBWEGKSEKPRQGWIJJCTUZB": {
			{
				SourceId: "QJRRSSKMJRDKUDTYVNYGAMQPULKAMILQQYOWBEXUDEUWQUMNGDHQYLOAJMEB",
				DestId:   "IXTSDANOXIVIWGNDCNZVWSAVAEPBGLGSQTLSVHHBWEGKSEKPRQGWIJJCTUZB",
			},
			{
				SourceId: "IXTSDANOXIVIWGNDCNZVWSAVAEPBGLGSQTLSVHHBWEGKSEKPRQGWIJJCTUZB",
				DestId:   "QJRRSSKMJRDKUDTYVNYGAMQPULKAMILQQYOWBEXUDEUWQUMNGDHQYLOAJMEB",
			},
		},
		"AXTSDANOXIVIWGNDCNZVWSAVAEPBGLGSQTLSVHHBWEGKSEKPRQGWIJJCTUZB": {
			{
				SourceId: "AXTSDANOXIVIWGNDCNZVWSAVAEPBGLGSQTLSVHHBWEGKSEKPRQGWIJJCTUZB",
				DestId:   "BJRRSSKMJRDKUDTYVNYGAMQPULKAMILQQYOWBEXUDEUWQUMNGDHQYLOAJMEB",
			},
		},
		"BJRRSSKMJRDKUDTYVNYGAMQPULKAMILQQYOWBEXUDEUWQUMNGDHQYLOAJMEB": {
			{
				SourceId: "AXTSDANOXIVIWGNDCNZVWSAVAEPBGLGSQTLSVHHBWEGKSEKPRQGWIJJCTUZB",
				DestId:   "BJRRSSKMJRDKUDTYVNYGAMQPULKAMILQQYOWBEXUDEUWQUMNGDHQYLOAJMEB",
			},
		},
		"ZXTSDANOXIVIWGNDCNZVWSAVAEPBGLGSQTLSVHHBWEGKSEKPRQGWIJJCTUZB": {
			{
				SourceId: "QJRRSSKMJRDKUDTYVNYGAMQPULKAMILQQYOWBEXUDEUWQUMNGDHQYLOAJMEB",
				DestId:   "ZXTSDANOXIVIWGNDCNZVWSAVAEPBGLGSQTLSVHHBWEGKSEKPRQGWIJJCTUZB",
			},
		},
	}

	got, err := createTransferTransactionsIdentityMap(context.Background(), txs)
	require.NoError(t, err)
	diff := cmp.Diff(got, expected, cmpopts.IgnoreUnexported(protobuf.Transaction{}))
	require.Empty(t, diff)
}

func TestStore(t *testing.T) {
	ctx := context.Background()

	// Setup test environment
	dbDir, err := os.MkdirTemp("", "pebble_test")
	require.NoError(t, err)
	defer func(path string) {
		_ = os.RemoveAll(path)
	}(dbDir)

	database, err := pebble.Open(filepath.Join(dbDir, "testdb"), &pebble.Options{})
	require.NoError(t, err)
	defer func(database *pebble.DB) {
		_ = database.Close()
	}(database)

	s := db.NewPebbleStore(database)

	firstTick := []types.Transaction{
		{
			SourcePublicKey:      identityToPubkeyNoError("QJRRSSKMJRDKUDTYVNYGAMQPULKAMILQQYOWBEXUDEUWQUMNGDHQYLOAJMEB"),
			DestinationPublicKey: identityToPubkeyNoError("IXTSDANOXIVIWGNDCNZVWSAVAEPBGLGSQTLSVHHBWEGKSEKPRQGWIJJCTUZB"),
			Amount:               15,
		},
		{
			SourcePublicKey:      identityToPubkeyNoError("IXTSDANOXIVIWGNDCNZVWSAVAEPBGLGSQTLSVHHBWEGKSEKPRQGWIJJCTUZB"),
			DestinationPublicKey: identityToPubkeyNoError("QJRRSSKMJRDKUDTYVNYGAMQPULKAMILQQYOWBEXUDEUWQUMNGDHQYLOAJMEB"),
			Amount:               20,
		},
	}

	secondTick := []types.Transaction{
		{
			SourcePublicKey:      identityToPubkeyNoError("QJRRSSKMJRDKUDTYVNYGAMQPULKAMILQQYOWBEXUDEUWQUMNGDHQYLOAJMEB"),
			DestinationPublicKey: identityToPubkeyNoError("IXTSDANOXIVIWGNDCNZVWSAVAEPBGLGSQTLSVHHBWEGKSEKPRQGWIJJCTUZB"),
			Amount:               25,
		},
		{
			SourcePublicKey:      identityToPubkeyNoError("IXTSDANOXIVIWGNDCNZVWSAVAEPBGLGSQTLSVHHBWEGKSEKPRQGWIJJCTUZB"),
			DestinationPublicKey: identityToPubkeyNoError("QJRRSSKMJRDKUDTYVNYGAMQPULKAMILQQYOWBEXUDEUWQUMNGDHQYLOAJMEB"),
			Amount:               30,
		},

		{
			SourcePublicKey:      identityToPubkeyNoError("IXTSDANOXIVIWGNDCNZVWSAVAEPBGLGSQTLSVHHBWEGKSEKPRQGWIJJCTUZB"),
			DestinationPublicKey: identityToPubkeyNoError("QJRRSSKMJRDKUDTYVNYGAMQPULKAMILQQYOWBEXUDEUWQUMNGDHQYLOAJMEB"),
			Amount:               0,
		},
		{
			SourcePublicKey:      identityToPubkeyNoError("QJRRSSKMJRDKUDTYVNYGAMQPULKAMILQQYOWBEXUDEUWQUMNGDHQYLOAJMEB"),
			DestinationPublicKey: identityToPubkeyNoError("IXTSDANOXIVIWGNDCNZVWSAVAEPBGLGSQTLSVHHBWEGKSEKPRQGWIJJCTUZB"),
			Amount:               0,
		},
	}

	err = Store(ctx, s, 1, firstTick)
	require.NoError(t, err)

	expectedFirstTickFirstID := &protobuf.TransferTransactionsPerTick{
		TickNumber: 1,
		Identity:   "QJRRSSKMJRDKUDTYVNYGAMQPULKAMILQQYOWBEXUDEUWQUMNGDHQYLOAJMEB",
		Transactions: []*protobuf.Transaction{
			{
				SourceId:     "QJRRSSKMJRDKUDTYVNYGAMQPULKAMILQQYOWBEXUDEUWQUMNGDHQYLOAJMEB",
				DestId:       "IXTSDANOXIVIWGNDCNZVWSAVAEPBGLGSQTLSVHHBWEGKSEKPRQGWIJJCTUZB",
				Amount:       15,
				SignatureHex: "00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000",
			},
			{
				SourceId:     "IXTSDANOXIVIWGNDCNZVWSAVAEPBGLGSQTLSVHHBWEGKSEKPRQGWIJJCTUZB",
				DestId:       "QJRRSSKMJRDKUDTYVNYGAMQPULKAMILQQYOWBEXUDEUWQUMNGDHQYLOAJMEB",
				Amount:       20,
				SignatureHex: "00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000",
			},
		},
	}

	expectedFirstTickSecondID := &protobuf.TransferTransactionsPerTick{
		TickNumber: 1,
		Identity:   "IXTSDANOXIVIWGNDCNZVWSAVAEPBGLGSQTLSVHHBWEGKSEKPRQGWIJJCTUZB",
		Transactions: []*protobuf.Transaction{
			{
				SourceId:     "QJRRSSKMJRDKUDTYVNYGAMQPULKAMILQQYOWBEXUDEUWQUMNGDHQYLOAJMEB",
				DestId:       "IXTSDANOXIVIWGNDCNZVWSAVAEPBGLGSQTLSVHHBWEGKSEKPRQGWIJJCTUZB",
				Amount:       15,
				SignatureHex: "00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000",
			},
			{
				SourceId:     "IXTSDANOXIVIWGNDCNZVWSAVAEPBGLGSQTLSVHHBWEGKSEKPRQGWIJJCTUZB",
				DestId:       "QJRRSSKMJRDKUDTYVNYGAMQPULKAMILQQYOWBEXUDEUWQUMNGDHQYLOAJMEB",
				Amount:       20,
				SignatureHex: "00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000",
			},
		},
	}

	// TODO the following will fail after removing the transfer transactions per identity data

	got, err := s.GetTransactionsForEntity(ctx, "QJRRSSKMJRDKUDTYVNYGAMQPULKAMILQQYOWBEXUDEUWQUMNGDHQYLOAJMEB", 1, 1)
	require.NoError(t, err)
	diff := cmp.Diff(got, []*protobuf.TransferTransactionsPerTick{expectedFirstTickFirstID}, cmpopts.IgnoreFields(protobuf.Transaction{}, "TxId"), cmpopts.IgnoreUnexported(protobuf.TransferTransactionsPerTick{}, protobuf.Transaction{}))
	require.Empty(t, diff)

	got, err = s.GetTransactionsForEntity(ctx, "IXTSDANOXIVIWGNDCNZVWSAVAEPBGLGSQTLSVHHBWEGKSEKPRQGWIJJCTUZB", 1, 1)
	require.NoError(t, err)
	diff = cmp.Diff(got, []*protobuf.TransferTransactionsPerTick{expectedFirstTickSecondID}, cmpopts.IgnoreFields(protobuf.Transaction{}, "TxId"), cmpopts.IgnoreUnexported(protobuf.TransferTransactionsPerTick{}, protobuf.Transaction{}))
	require.Empty(t, diff)

	err = Store(ctx, s, 2, secondTick)
	require.NoError(t, err)

	expectedSecondTickFirstID := &protobuf.TransferTransactionsPerTick{
		TickNumber: 2,
		Identity:   "QJRRSSKMJRDKUDTYVNYGAMQPULKAMILQQYOWBEXUDEUWQUMNGDHQYLOAJMEB",
		Transactions: []*protobuf.Transaction{
			{
				SourceId:     "QJRRSSKMJRDKUDTYVNYGAMQPULKAMILQQYOWBEXUDEUWQUMNGDHQYLOAJMEB",
				DestId:       "IXTSDANOXIVIWGNDCNZVWSAVAEPBGLGSQTLSVHHBWEGKSEKPRQGWIJJCTUZB",
				Amount:       25,
				SignatureHex: "00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000",
			},
			{
				SourceId:     "IXTSDANOXIVIWGNDCNZVWSAVAEPBGLGSQTLSVHHBWEGKSEKPRQGWIJJCTUZB",
				DestId:       "QJRRSSKMJRDKUDTYVNYGAMQPULKAMILQQYOWBEXUDEUWQUMNGDHQYLOAJMEB",
				Amount:       30,
				SignatureHex: "00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000",
			},
		},
	}

	expectedSecondTickSecondID := &protobuf.TransferTransactionsPerTick{
		TickNumber: 2,
		Identity:   "IXTSDANOXIVIWGNDCNZVWSAVAEPBGLGSQTLSVHHBWEGKSEKPRQGWIJJCTUZB",
		Transactions: []*protobuf.Transaction{
			{
				SourceId:     "QJRRSSKMJRDKUDTYVNYGAMQPULKAMILQQYOWBEXUDEUWQUMNGDHQYLOAJMEB",
				DestId:       "IXTSDANOXIVIWGNDCNZVWSAVAEPBGLGSQTLSVHHBWEGKSEKPRQGWIJJCTUZB",
				Amount:       25,
				SignatureHex: "00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000",
			},
			{
				SourceId:     "IXTSDANOXIVIWGNDCNZVWSAVAEPBGLGSQTLSVHHBWEGKSEKPRQGWIJJCTUZB",
				DestId:       "QJRRSSKMJRDKUDTYVNYGAMQPULKAMILQQYOWBEXUDEUWQUMNGDHQYLOAJMEB",
				Amount:       30,
				SignatureHex: "00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000",
			},
		},
	}

	got, err = s.GetTransactionsForEntity(ctx, "QJRRSSKMJRDKUDTYVNYGAMQPULKAMILQQYOWBEXUDEUWQUMNGDHQYLOAJMEB", 2, 2)
	require.NoError(t, err)
	diff = cmp.Diff(got, []*protobuf.TransferTransactionsPerTick{expectedSecondTickFirstID}, cmpopts.IgnoreFields(protobuf.Transaction{}, "TxId"), cmpopts.IgnoreUnexported(protobuf.TransferTransactionsPerTick{}, protobuf.Transaction{}))
	require.Empty(t, diff)

	got, err = s.GetTransactionsForEntity(ctx, "IXTSDANOXIVIWGNDCNZVWSAVAEPBGLGSQTLSVHHBWEGKSEKPRQGWIJJCTUZB", 2, 2)
	require.NoError(t, err)
	diff = cmp.Diff(got, []*protobuf.TransferTransactionsPerTick{expectedSecondTickSecondID}, cmpopts.IgnoreFields(protobuf.Transaction{}, "TxId"), cmpopts.IgnoreUnexported(protobuf.TransferTransactionsPerTick{}, protobuf.Transaction{}))
	require.Empty(t, diff)

	expectedCombined := []*protobuf.TransferTransactionsPerTick{expectedFirstTickFirstID, expectedSecondTickFirstID}
	gotCombined, err := s.GetTransactionsForEntity(ctx, "QJRRSSKMJRDKUDTYVNYGAMQPULKAMILQQYOWBEXUDEUWQUMNGDHQYLOAJMEB", 1, 2)
	require.NoError(t, err)
	diff = cmp.Diff(gotCombined, expectedCombined, cmpopts.IgnoreFields(protobuf.Transaction{}, "TxId"), cmpopts.IgnoreUnexported(protobuf.TransferTransactionsPerTick{}, protobuf.Transaction{}))
	require.Empty(t, diff)

	expectedCombined = []*protobuf.TransferTransactionsPerTick{expectedFirstTickSecondID, expectedSecondTickSecondID}
	gotCombined, err = s.GetTransactionsForEntity(ctx, "IXTSDANOXIVIWGNDCNZVWSAVAEPBGLGSQTLSVHHBWEGKSEKPRQGWIJJCTUZB", 1, 2)
	require.NoError(t, err)
	diff = cmp.Diff(gotCombined, expectedCombined, cmpopts.IgnoreFields(protobuf.Transaction{}, "TxId"), cmpopts.IgnoreUnexported(protobuf.TransferTransactionsPerTick{}, protobuf.Transaction{}))
	require.Empty(t, diff)
}

func identityToPubkeyNoError(id string) [32]byte {
	identity := types.Identity(id)
	pubKey, _ := identity.ToPubKey(false)

	return pubKey
}
