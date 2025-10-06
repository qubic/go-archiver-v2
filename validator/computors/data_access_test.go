package computors

import (
	"context"
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

	err = Save(context.Background(), dataStore, 42, computorsList)
	require.NoError(t, err)

	loaded, err := Get(context.Background(), dataStore, nil, 0, 42)
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

	loaded, err := Get(context.Background(), dataStore, &client, 0, 42)
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

func createTestDataStore(t *testing.T) (*db.PebbleStore, error) {
	testDir := t.TempDir()
	err := os.Mkdir(fmt.Sprintf("%s/%s", testDir, "42"), 0755)
	require.NoError(t, err)
	return db.CreateStore(testDir, 42)
}

func closeDb(database *db.PebbleStore) {
	_ = database.Close()
}

type TestClient struct {
	computors types.Computors
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
