package db

import (
	"context"
	"fmt"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/qubic/go-archiver-v2/protobuf"
	"github.com/stretchr/testify/require"
	"os"
	"testing"
)

func TestPebbleStore_SetAndGetComputors(t *testing.T) {
	store, err := createTestDataStore(t)
	require.NoError(t, err)
	defer closeDb(store)

	comps := &protobuf.Computors{
		Epoch:        1,
		Identities:   []string{"QJRRSSKMJRDKUDTYVNYGAMQPULKAMILQQYOWBEXUDEUWQUMNGDHQYLOAJMEB", "IXTSDANOXIVIWGNDCNZVWSAVAEPBGLGSQTLSVHHBWEGKSEKPRQGWIJJCTUZB"},
		SignatureHex: "01020304ff0607080910010203040506070809100000000000000000000000000000000000000000000000000000000000000000000000000000000000000000",
	}

	compsList := &protobuf.ComputorsList{
		Computors: []*protobuf.Computors{comps},
	}

	err = store.SetComputors(context.Background(), 42, compsList)
	require.NoError(t, err)

	reloaded, err := store.GetComputors(context.Background(), 42)
	require.NoError(t, err)
	require.NotNil(t, reloaded)

	diff := cmp.Diff(&compsList, &reloaded, cmpopts.IgnoreUnexported(protobuf.ComputorsList{}, protobuf.Computors{}))
	require.Emptyf(t, diff, "unexpected diff result (-got +want):\n%s", diff)
}

func createTestDataStore(t *testing.T) (*PebbleStore, error) {
	testDir := t.TempDir()
	err := os.Mkdir(fmt.Sprintf("%s/%s", testDir, "42"), 0755)
	require.NoError(t, err)
	return CreateStore(testDir, 42)
}

func closeDb(database *PebbleStore) {
	_ = database.Close()
}
