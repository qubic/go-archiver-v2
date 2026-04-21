package processor

import (
	"context"
	"log"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/qubic/go-archiver-v2/db"
	"github.com/qubic/go-archiver-v2/metrics"
	"github.com/qubic/go-archiver-v2/network"
	"github.com/qubic/go-node-connector/types"
	"github.com/stretchr/testify/require"
)

type TestFetcher struct {
	epoch       uint16
	tick        uint32
	initialTick uint32
}

func (f *TestFetcher) GetTickStatus(_ context.Context) (network.TickStatus, error) {
	return network.TickStatus{
		Epoch:                f.epoch,
		Tick:                 f.tick,
		InitialTick:          f.initialTick,
		NumberOfAlignedVotes: 500,
	}, nil
}

func (f *TestFetcher) GetSystemMetadata(_ context.Context) (network.SystemMetadata, error) {
	return network.SystemMetadata{
		InitialTick:                f.initialTick,
		ComputorSignatureAvailable: true,
		VoteSignatureAvailable:     true,
	}, nil
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
	panic("not implemented")
}

func (f *TestFetcher) GetTxStatus(_ context.Context, _ uint32) (types.TransactionStatus, bool, error) {
	panic("not implemented")
}

func (f *TestFetcher) Release(_ error) {
	log.Println("Releasing fetcher")
}

type TestValidator struct{}

func (t TestValidator) Validate(_ context.Context, _ *db.PebbleStore, _ network.DataFetcher, epoch uint16, tickNumber uint32) error {
	log.Printf("Mock validated tick [%d] in epoch [%d].", tickNumber, epoch)
	return nil
}

var (
	dummyMetrics = metrics.NewProcessingMetrics(prometheus.DefaultRegisterer.(*prometheus.Registry), "")
)

func TestProcessor_processOneByOne(t *testing.T) {
	fetcher := &TestFetcher{
		epoch:       42,
		tick:        101,
		initialTick: 100,
	}
	fetcherFactory := func() (network.DataFetcher, error) {
		return fetcher, nil
	}
	testDir := t.TempDir()
	dataPool, err := db.NewDatabasePool(testDir, 5)
	require.NoError(t, err)

	processor := NewProcessor(fetcherFactory, dataPool, &TestValidator{}, Config{time.Millisecond}, dummyMetrics)
	err = processor.processOneByOne()
	require.NoError(t, err)
	err = processor.processOneByOne()
	require.NoError(t, err)
	err = processor.processOneByOne()
	require.ErrorContains(t, err, "next tick is in the future")
}

func TestProcessor_processOneByOne_epochChange(t *testing.T) {
	fetcher := &TestFetcher{
		epoch:       42,
		tick:        101,
		initialTick: 100,
	}
	fetcherFactory := func() (network.DataFetcher, error) {
		return fetcher, nil
	}
	testDir := t.TempDir()
	dataPool, err := db.NewDatabasePool(testDir, 5)
	require.NoError(t, err)

	processor := NewProcessor(fetcherFactory, dataPool, &TestValidator{}, Config{time.Millisecond}, dummyMetrics)
	err = processor.processOneByOne()
	require.NoError(t, err)
	err = processor.processOneByOne()
	require.NoError(t, err)
	err = processor.processOneByOne()
	require.ErrorContains(t, err, "next tick is in the future")

	// new epoch
	fetcher.epoch = 43
	fetcher.tick = 202
	fetcher.initialTick = 200

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
