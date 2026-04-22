package computors

import (
	"context"
	"encoding/binary"
	"fmt"
	"os"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/qubic/go-archiver-v2/db"
	"github.com/qubic/go-archiver-v2/network"
	"github.com/qubic/go-node-connector/types"
	"github.com/stretchr/testify/require"
)

func TestComputorsDataAccess_WhenGet_GivenStoredInDb_ThenLoadFromDb(t *testing.T) {
	dataStore, err := createTestDataStore(t)
	defer closeDb(dataStore)
	require.NoError(t, err)

	computorsList := []*Computors{{
		Epoch:     123,
		PubKeys:   [NumberOfComputors][32]byte{{1}, {2}, {3}},
		Signature: [64]byte{},
	}}

	computors := types.Computors{
		Epoch:     123,
		PubKeys:   [NumberOfComputors][32]byte{{1}, {2}, {3}},
		Signature: [64]byte{},
	}

	fetcher := &TestFetcher{
		computors: computors,
	}

	err = Save(context.Background(), dataStore, 42, computorsList)
	require.NoError(t, err)

	loaded, err := Get(context.Background(), dataStore, fetcher, 0, 0, 42, 0, true)
	require.NoError(t, err)

	diff := cmp.Diff(loaded, computorsList)
	require.Emptyf(t, diff, "comparision error (-got +want): %s", diff)
}

func TestComputorsDataAccess_WhenGet_GiveNotStored_ThenLoadClient(t *testing.T) {
	dataStore, err := createTestDataStore(t)
	defer closeDb(dataStore)
	require.NoError(t, err)

	computors := types.Computors{
		Epoch:     123,
		PubKeys:   [NumberOfComputors][32]byte{{1}, {2}, {3}},
		Signature: [64]byte{},
	}

	fetcher := &TestFetcher{
		computors: computors,
	}

	loaded, err := Get(context.Background(), dataStore, fetcher, 0, 0, 42, 0, true)
	require.NoError(t, err)
	diff := cmp.Diff(loaded, []*Computors{{
		Epoch:     123,
		PubKeys:   [NumberOfComputors][32]byte{{1}, {2}, {3}},
		Signature: [64]byte{},
	}})
	require.Emptyf(t, diff, "comparision error (-got +want): %s", diff)
}

func TestComputorsDataAccess_WhenLoad_GivenNotFound_ThenReturnNil(t *testing.T) {
	dataStore, err := createTestDataStore(t)
	defer closeDb(dataStore)
	require.NoError(t, err)

	computorsList, err := load(context.Background(), dataStore, 42)
	require.NoError(t, err)
	require.Nil(t, computorsList)
}

func TestComputorsDataAccess_WhenLoad_GivenEmptyListStored_ThenReturnEmptyList(t *testing.T) {
	dataStore, err := createTestDataStore(t)
	defer closeDb(dataStore)
	require.NoError(t, err)

	err = Save(context.Background(), dataStore, 42, []*Computors{})
	require.NoError(t, err)

	computorsList, err := load(context.Background(), dataStore, 42)
	require.NoError(t, err)
	require.NotNil(t, computorsList)
	require.Equal(t, 0, len(computorsList))

}

func TestGet_ExistingList_SameSignature_NoFetch(t *testing.T) {
	dataStore, err := createTestDataStore(t)
	defer closeDb(dataStore)
	require.NoError(t, err)

	// Create a signature with known first 8 bytes
	signature := [64]byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08}
	signaturePackage := binary.LittleEndian.Uint64(signature[:8])

	computorsList := []*Computors{{
		Epoch:     123,
		PubKeys:   [NumberOfComputors][32]byte{{1}, {2}, {3}},
		Signature: signature,
	}}

	fetcher := &TestFetcher{
		computors: types.Computors{
			Epoch:     123,
			PubKeys:   [NumberOfComputors][32]byte{{1}, {2}, {3}},
			Signature: [64]byte{0xFF}, // Different signature - should NOT be fetched
		},
	}

	err = Save(context.Background(), dataStore, 42, computorsList)
	require.NoError(t, err)

	// Pass matching signature package - should NOT fetch from node
	loaded, err := Get(context.Background(), dataStore, fetcher, 100, 0, 42, signaturePackage, true)
	require.NoError(t, err)
	require.False(t, fetcher.getComputorsCalled, "GetComputors should NOT be called when signature matches")
	require.Len(t, loaded, 1)
}

func TestGet_ExistingList_NewSignature_AppendsNew(t *testing.T) {
	dataStore, err := createTestDataStore(t)
	defer closeDb(dataStore)
	require.NoError(t, err)

	// Stored computors with one signature
	storedSignature := [64]byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08}

	computorsList := []*Computors{{
		Epoch:      123,
		TickNumber: 50,
		PubKeys:    [NumberOfComputors][32]byte{{1}, {2}, {3}},
		Signature:  storedSignature,
	}}

	// New computors from node with different signature
	newSignature := [64]byte{0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18}
	newSignaturePackage := binary.LittleEndian.Uint64(newSignature[:8])

	fetcher := &TestFetcher{
		computors: types.Computors{
			Epoch:     123,
			PubKeys:   [NumberOfComputors][32]byte{{4}, {5}, {6}},
			Signature: newSignature,
		},
	}

	err = Save(context.Background(), dataStore, 42, computorsList)
	require.NoError(t, err)

	// Pass different signature package - should fetch from node and append
	loaded, err := Get(context.Background(), dataStore, fetcher, 100, 0, 42, newSignaturePackage, true)
	require.NoError(t, err)
	require.True(t, fetcher.getComputorsCalled, "GetComputors should be called when signature differs")
	require.Len(t, loaded, 2)
	require.Equal(t, uint32(100), loaded[1].TickNumber, "New computors should have the provided tick number")
	require.Equal(t, newSignature, loaded[1].Signature)
}

func TestGet_EmptyStore_SignatureMismatch_Errors(t *testing.T) {
	dataStore, err := createTestDataStore(t)
	defer closeDb(dataStore)
	require.NoError(t, err)

	// Node returns computors with this signature
	nodeSignature := [64]byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08}

	fetcher := &TestFetcher{
		computors: types.Computors{
			Epoch:     123,
			PubKeys:   [NumberOfComputors][32]byte{{1}, {2}, {3}},
			Signature: nodeSignature,
		},
	}

	// Pass a different signature package than what node returns
	differentSignaturePackage := uint64(0xDEADBEEF)

	// No stored computors - will fetch from node, but signature won't match
	_, err = Get(context.Background(), dataStore, fetcher, 100, 50, 42, differentSignaturePackage, true)
	require.Error(t, err)
	require.Contains(t, err.Error(), "initial computor list signature does not match")
	require.True(t, fetcher.getComputorsCalled, "GetComputors should be called for initial fetch")
}

func TestGet_EmptyStore_SignatureMatch_Succeeds(t *testing.T) {
	dataStore, err := createTestDataStore(t)
	defer closeDb(dataStore)
	require.NoError(t, err)

	// Node returns computors with this signature
	nodeSignature := [64]byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08}
	signaturePackage := binary.LittleEndian.Uint64(nodeSignature[:8])

	fetcher := &TestFetcher{
		computors: types.Computors{
			Epoch:     123,
			PubKeys:   [NumberOfComputors][32]byte{{1}, {2}, {3}},
			Signature: nodeSignature,
		},
	}

	// No stored computors - will fetch from node, signature matches
	loaded, err := Get(context.Background(), dataStore, fetcher, 100, 50, 42, signaturePackage, true)
	require.NoError(t, err)
	require.True(t, fetcher.getComputorsCalled)
	require.Len(t, loaded, 1)
	require.Equal(t, uint32(50), loaded[0].TickNumber, "Initial computors should use initialTick")
}

func TestGet_BobPath_FetchesOncePerEpoch(t *testing.T) {
	dataStore, err := createTestDataStore(t)
	defer closeDb(dataStore)
	require.NoError(t, err)

	fetcher := &TestFetcher{
		computors: types.Computors{
			Epoch:     123,
			PubKeys:   [NumberOfComputors][32]byte{{1}, {2}, {3}},
			Signature: [64]byte{},
		},
	}

	// First call with computorSignatureAvailable=false (bob path) - should fetch
	loaded, err := Get(context.Background(), dataStore, fetcher, 100, 50, 42, 0, false)
	require.NoError(t, err)
	require.True(t, fetcher.getComputorsCalled)
	require.Len(t, loaded, 1)
	require.True(t, loaded[0].Validated, "Bob-sourced computors should be marked as validated")

	// Save them
	err = Save(context.Background(), dataStore, 42, loaded)
	require.NoError(t, err)

	// Reset flag
	fetcher.getComputorsCalled = false

	// Second call - should load from store, not fetch again
	loaded, err = Get(context.Background(), dataStore, fetcher, 200, 50, 42, 0, false)
	require.NoError(t, err)
	require.False(t, fetcher.getComputorsCalled, "GetComputors should NOT be called when already stored (bob path)")
	require.Len(t, loaded, 1)
}

func createTestDataStore(t *testing.T) (*db.PebbleStore, error) {
	testDir := t.TempDir()
	err := os.Mkdir(fmt.Sprintf("%s/%s", testDir, "42"), 0755)
	require.NoError(t, err)
	return db.CreateStore(testDir, 42, false)
}

func closeDb(database *db.PebbleStore) {
	_ = database.Close()
}

// TestFetcher implements network.DataFetcher for testing computors.
type TestFetcher struct {
	computors             types.Computors
	getComputorsCalled    bool
	getComputorsCallCount int
}

func (f *TestFetcher) GetTickStatus(_ context.Context) (network.TickStatus, error) {
	panic("not implemented")
}

func (f *TestFetcher) GetSystemMetadata(_ context.Context) (network.SystemMetadata, error) {
	panic("not implemented")
}

func (f *TestFetcher) GetQuorumVotes(_ context.Context, _ uint32) (types.QuorumVotes, error) {
	panic("not implemented")
}

func (f *TestFetcher) GetTickData(_ context.Context, _ uint32) (types.TickData, error) {
	panic("not implemented")
}

func (f *TestFetcher) GetTickTransactions(_ context.Context, _ uint32) (types.Transactions, error) {
	panic("not implemented")
}

func (f *TestFetcher) GetComputors(_ context.Context) (types.Computors, error) {
	f.getComputorsCalled = true
	f.getComputorsCallCount++
	return f.computors, nil
}

func (f *TestFetcher) GetTxStatus(_ context.Context, _ uint32) (types.TransactionStatus, bool, error) {
	panic("not implemented")
}

func (f *TestFetcher) Release(_ error) {}
