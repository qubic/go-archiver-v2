package computors

import (
	"context"
	"encoding/binary"
	"fmt"
	"os"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/qubic/go-archiver-v2/db"
	qubic "github.com/qubic/go-node-connector"
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

	client := TestClient{
		computors: computors,
	}

	err = Save(context.Background(), dataStore, 42, computorsList)
	require.NoError(t, err)

	loaded, err := Get(context.Background(), dataStore, &client, 0, 0, 42, 0)
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

	client := TestClient{
		computors: computors,
	}

	loaded, err := Get(context.Background(), dataStore, &client, 0, 0, 42, 0)
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

	client := &TestClient{
		computors: types.Computors{
			Epoch:     123,
			PubKeys:   [NumberOfComputors][32]byte{{1}, {2}, {3}},
			Signature: [64]byte{0xFF}, // Different signature - should NOT be fetched
		},
	}

	err = Save(context.Background(), dataStore, 42, computorsList)
	require.NoError(t, err)

	// Pass matching signature package - should NOT fetch from node
	loaded, err := Get(context.Background(), dataStore, client, 100, 0, 42, signaturePackage)
	require.NoError(t, err)
	require.False(t, client.getComputorsCalled, "GetComputors should NOT be called when signature matches")
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

	client := &TestClient{
		computors: types.Computors{
			Epoch:     123,
			PubKeys:   [NumberOfComputors][32]byte{{4}, {5}, {6}},
			Signature: newSignature,
		},
	}

	err = Save(context.Background(), dataStore, 42, computorsList)
	require.NoError(t, err)

	// Pass different signature package - should fetch from node and append
	loaded, err := Get(context.Background(), dataStore, client, 100, 0, 42, newSignaturePackage)
	require.NoError(t, err)
	require.True(t, client.getComputorsCalled, "GetComputors should be called when signature differs")
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

	client := &TestClient{
		computors: types.Computors{
			Epoch:     123,
			PubKeys:   [NumberOfComputors][32]byte{{1}, {2}, {3}},
			Signature: nodeSignature,
		},
	}

	// Pass a different signature package than what node returns
	differentSignaturePackage := uint64(0xDEADBEEF)

	// No stored computors - will fetch from node, but signature won't match
	_, err = Get(context.Background(), dataStore, client, 100, 50, 42, differentSignaturePackage)
	require.Error(t, err)
	require.Contains(t, err.Error(), "initial computor list signature does not match")
	require.True(t, client.getComputorsCalled, "GetComputors should be called for initial fetch")
}

func TestGet_EmptyStore_SignatureMatch_Succeeds(t *testing.T) {
	dataStore, err := createTestDataStore(t)
	defer closeDb(dataStore)
	require.NoError(t, err)

	// Node returns computors with this signature
	nodeSignature := [64]byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08}
	signaturePackage := binary.LittleEndian.Uint64(nodeSignature[:8])

	client := &TestClient{
		computors: types.Computors{
			Epoch:     123,
			PubKeys:   [NumberOfComputors][32]byte{{1}, {2}, {3}},
			Signature: nodeSignature,
		},
	}

	// No stored computors - will fetch from node, signature matches
	loaded, err := Get(context.Background(), dataStore, client, 100, 50, 42, signaturePackage)
	require.NoError(t, err)
	require.True(t, client.getComputorsCalled)
	require.Len(t, loaded, 1)
	require.Equal(t, uint32(50), loaded[0].TickNumber, "Initial computors should use initialTick")
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

type TestClient struct {
	computors             types.Computors
	getComputorsCalled    bool
	getComputorsCallCount int
}

func (t *TestClient) GetIssuedAssets(_ context.Context, _ string) (types.IssuedAssets, error) {
	panic("implement me")
}

func (t *TestClient) GetPossessedAssets(_ context.Context, _ string) (types.PossessedAssets, error) {
	panic("implement me")
}

func (t *TestClient) GetOwnedAssets(_ context.Context, _ string) (types.OwnedAssets, error) {
	panic("implement me")
}

func (t *TestClient) GetIdentity(_ context.Context, _ string) (types.AddressInfo, error) {
	panic("implement me")
}

func (t *TestClient) GetTickInfo(_ context.Context) (types.TickInfo, error) {
	panic("implement me")
}

func (t *TestClient) GetSystemInfo(_ context.Context) (types.SystemInfo, error) {
	panic("implement me")
}

func (t *TestClient) GetTxStatus(_ context.Context, _ uint32) (types.TransactionStatus, error) {
	panic("implement me")
}

func (t *TestClient) GetTickData(_ context.Context, _ uint32) (types.TickData, error) {
	panic("implement me")
}

func (t *TestClient) GetTickTransactions(_ context.Context, _ uint32) (types.Transactions, error) {
	panic("implement me")
}

func (t *TestClient) SendRawTransaction(_ context.Context, _ []byte) error {
	panic("implement me")
}

func (t *TestClient) GetQuorumVotes(_ context.Context, _ uint32) (types.QuorumVotes, error) {
	panic("implement me")
}

func (t *TestClient) GetComputors(_ context.Context) (types.Computors, error) {
	t.getComputorsCalled = true
	t.getComputorsCallCount++
	return t.computors, nil
}

func (t *TestClient) QuerySmartContract(_ context.Context, _ qubic.RequestContractFunction, _ []byte) (types.SmartContractData, error) {
	panic("implement me")
}

func (t *TestClient) GetAssetPossessionsByFilter(_ context.Context, _, _, _, _ string, _, _ uint16) (types.AssetPossessions, error) {
	panic("implement me")
}

func (t *TestClient) GetAssetOwnershipsByFilter(_ context.Context, _, _, _ string, _ uint16) (types.AssetOwnerships, error) {
	panic("implement me")
}

func (t *TestClient) GetAssetIssuancesByFilter(_ context.Context, _, _ string) (types.AssetIssuances, error) {
	panic("implement me")
}

func (t *TestClient) GetAssetIssuancesByUniverseIndex(_ context.Context, _ uint32) (types.AssetIssuances, error) {
	panic("implement me")
}

func (t *TestClient) GetAssetOwnershipsByUniverseIndex(_ context.Context, _ uint32) (types.AssetOwnerships, error) {
	panic("implement me")
}

func (t *TestClient) GetAssetPossessionsByUniverseIndex(_ context.Context, _ uint32) (types.AssetPossessions, error) {
	panic("implement me")
}

func (t *TestClient) Close() error {
	panic("implement me")
}
