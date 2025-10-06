package processor

import (
	"context"
	"log"
	"testing"
	"time"

	"github.com/qubic/go-archiver-v2/db"
	"github.com/qubic/go-archiver-v2/network"
	qubic "github.com/qubic/go-node-connector"
	"github.com/qubic/go-node-connector/types"
	"github.com/stretchr/testify/require"
)

type TestPool struct {
	client network.QubicClient
}

func (t *TestPool) Get() (network.QubicClient, error) {
	return t.client, nil
}

func (t *TestPool) Put(_ network.QubicClient) error {
	log.Println("Returning client")
	return nil
}

func (t *TestPool) Close(_ network.QubicClient) error {
	log.Println("Closing client")
	return nil
}

type TestValidator struct{}

func (t TestValidator) Validate(_ context.Context, _ *db.PebbleStore, _ network.QubicClient, epoch uint16, tickNumber uint32) error {
	log.Printf("Mock validated tick [%d] in epoch [%d].", tickNumber, epoch)
	return nil
}

type TestClient struct {
	epoch       uint16
	tick        uint32
	InitialTick uint32
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
	return types.TickInfo{
		Epoch:                t.epoch,
		Tick:                 t.tick,
		NumberOfAlignedVotes: 500,
		InitialTick:          t.InitialTick,
	}, nil
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
	panic("implement me")
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
	log.Println("Closing client")
	return nil
}

func TestProcessor_processOneByOne(t *testing.T) {
	client := &TestClient{
		epoch:       42,
		tick:        101,
		InitialTick: 100,
	}
	clientPool := &TestPool{
		client: client,
	}
	testDir := t.TempDir()
	dataPool, err := db.NewDatabasePool(testDir, 5)
	require.NoError(t, err)

	processor := NewProcessor(clientPool, dataPool, &TestValidator{}, Config{time.Millisecond})
	err = processor.processOneByOne()
	require.NoError(t, err)
	err = processor.processOneByOne()
	require.NoError(t, err)
	err = processor.processOneByOne()
	require.ErrorContains(t, err, "next tick is in the future")
}

func TestProcessor_processOneByOne_epochChange(t *testing.T) {
	client := &TestClient{
		epoch:       42,
		tick:        101,
		InitialTick: 100,
	}
	clientPool := &TestPool{
		client: client,
	}
	testDir := t.TempDir()
	dataPool, err := db.NewDatabasePool(testDir, 5)
	require.NoError(t, err)

	processor := NewProcessor(clientPool, dataPool, &TestValidator{}, Config{time.Millisecond})
	err = processor.processOneByOne()
	require.NoError(t, err)
	err = processor.processOneByOne()
	require.NoError(t, err)
	err = processor.processOneByOne()
	require.ErrorContains(t, err, "next tick is in the future")

	// new epoch
	client.epoch = 43
	client.tick = 202
	client.InitialTick = 200

	err = processor.processOneByOne()
	require.NoError(t, err)
	err = processor.processOneByOne()
	require.NoError(t, err)
	err = processor.processOneByOne()
	require.NoError(t, err)
	err = processor.processOneByOne()
	require.ErrorContains(t, err, "next tick is in the future")

	_, err = dataPool.GetDbForEpoch(42) // old epoch
	require.NoError(t, err)
	_, err = dataPool.GetDbForEpoch(43) // new epoch
	require.NoError(t, err)
}
