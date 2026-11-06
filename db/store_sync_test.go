package db

import (
	"context"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/vfs"
	"github.com/qubic/go-archiver-v2/protobuf"
	"github.com/stretchr/testify/require"
)

// walSyncCountingFS wraps a vfs.FS and counts fsyncs issued on WAL (*.log) files, which is
// what a pebble.Sync write costs on disk.
type walSyncCountingFS struct {
	vfs.FS
	syncs atomic.Int64
}

func (fs *walSyncCountingFS) wrap(name string, f vfs.File, err error) (vfs.File, error) {
	if err != nil || !strings.HasSuffix(name, ".log") {
		return f, err
	}
	return &walSyncCountingFile{File: f, syncs: &fs.syncs}, nil
}

func (fs *walSyncCountingFS) Create(name string, c vfs.DiskWriteCategory) (vfs.File, error) {
	f, err := fs.FS.Create(name, c)
	return fs.wrap(name, f, err)
}

func (fs *walSyncCountingFS) ReuseForWrite(oldname, newname string, c vfs.DiskWriteCategory) (vfs.File, error) {
	f, err := fs.FS.ReuseForWrite(oldname, newname, c)
	return fs.wrap(newname, f, err)
}

func (fs *walSyncCountingFS) OpenReadWrite(name string, c vfs.DiskWriteCategory, opts ...vfs.OpenOption) (vfs.File, error) {
	f, err := fs.FS.OpenReadWrite(name, c, opts...)
	return fs.wrap(name, f, err)
}

type walSyncCountingFile struct {
	vfs.File
	syncs *atomic.Int64
}

func (f *walSyncCountingFile) Sync() error {
	f.syncs.Add(1)
	return f.File.Sync()
}

func (f *walSyncCountingFile) SyncData() error {
	f.syncs.Add(1)
	return f.File.SyncData()
}

func (f *walSyncCountingFile) SyncTo(length int64) (bool, error) {
	f.syncs.Add(1)
	return f.File.SyncTo(length)
}

func openStoreWithFS(t testing.TB, fs vfs.FS, dir string) *PebbleStore {
	t.Helper()
	opts := getDefaultPebbleOptions(false)
	opts.FS = fs
	pdb, err := pebble.Open(dir, opts)
	require.NoError(t, err)
	return NewPebbleStore(pdb)
}

// storeTick performs the writes the processor issues for one processed tick, in the same
// order and through one batch: quorum data, quorum data per interval, tick data,
// transactions, transaction status and, last, the processed tick marker together with the
// tick intervals. With commit=false the batch is discarded (a tick that failed mid-way).
func storeTick(t testing.TB, store *PebbleStore, epoch, tick uint32, nrTxs int, commit bool) {
	t.Helper()
	ctx := context.Background()

	tickStore := store.BeginBatch()
	defer tickStore.DiscardBatch()

	require.NoError(t, tickStore.SetQuorumTickData(ctx, tick, &protobuf.QuorumTickDataStored{
		QuorumTickStructure: &protobuf.QuorumTickStructure{Epoch: epoch, TickNumber: tick},
	}))
	require.NoError(t, tickStore.SetQuorumDataForCurrentEpochInterval(epoch, &protobuf.QuorumTickData{
		QuorumTickStructure: &protobuf.QuorumTickStructure{Epoch: epoch, TickNumber: tick},
	}))

	ids := make([]string, nrTxs)
	txs := make([]*protobuf.Transaction, nrTxs)
	statuses := make([]*protobuf.TransactionStatus, nrTxs)
	for i := range txs {
		ids[i] = fmt.Sprintf("tx-%d-%d", tick, i)
		txs[i] = &protobuf.Transaction{TxId: ids[i], TickNumber: tick}
		statuses[i] = &protobuf.TransactionStatus{TxId: ids[i], MoneyFlew: true}
	}
	require.NoError(t, tickStore.SetTickData(ctx, tick, &protobuf.TickData{Epoch: epoch, TickNumber: tick, TransactionIds: ids}))
	require.NoError(t, tickStore.SetTransactions(ctx, txs))
	require.NoError(t, tickStore.SetTickTransactionsStatus(ctx, uint64(tick), &protobuf.TickTransactionsStatus{Transactions: statuses}))
	require.NoError(t, tickStore.SetLastProcessedTickAndUpdateTickIntervals(ctx, &protobuf.ProcessedTick{TickNumber: tick, Epoch: epoch}))

	if commit {
		require.NoError(t, tickStore.CommitBatch())
	}
}

func TestPebbleStore_processedTickCostsOneWALSync(t *testing.T) {
	fs := &walSyncCountingFS{FS: vfs.NewMem()}
	store := openStoreWithFS(t, fs, "db")
	defer closeDb(store)

	storeTick(t, store, 42, 100, 3, true) // warm-up: first interval, first quorum interval entry
	before := fs.syncs.Load()

	storeTick(t, store, 42, 101, 3, true)

	require.Equal(t, int64(1), fs.syncs.Load()-before,
		"a processed tick should fsync the WAL once, not once per write")
}

func TestPebbleStore_processedTickIsDurableAfterCrash(t *testing.T) {
	ctx := context.Background()
	mem := vfs.NewCrashableMem()
	require.NoError(t, mem.MkdirAll("db", 0755))
	store := openStoreWithFS(t, mem, "db")

	storeTick(t, store, 42, 100, 2, true)
	storeTick(t, store, 42, 101, 2, true)

	crashed := mem.CrashClone(vfs.CrashCloneCfg{UnsyncedDataPercent: 0})
	defer closeDb(store)

	reopened := openStoreWithFS(t, crashed, "db")
	defer closeDb(reopened)

	last, err := reopened.GetLastProcessedTick(ctx)
	require.NoError(t, err)
	require.Equal(t, uint32(101), last.TickNumber, "the committed tick must survive the crash")

	td, err := reopened.GetTickData(ctx, 101)
	require.NoError(t, err)
	require.Equal(t, uint32(101), td.TickNumber)
	_, err = reopened.GetQuorumTickData(ctx, 101)
	require.NoError(t, err)
	txs, err := reopened.GetTickTransactions(ctx, 101)
	require.NoError(t, err)
	require.Len(t, txs, 2)
	status, err := reopened.GetTickTransactionsStatus(ctx, 101)
	require.NoError(t, err)
	require.Len(t, status.Transactions, 2)

	intervals, err := reopened.GetProcessedTickIntervalsPerEpoch(ctx, 42)
	require.NoError(t, err)
	require.Len(t, intervals.Intervals, 1)
	require.Equal(t, uint32(100), intervals.Intervals[0].InitialProcessedTick)
	require.Equal(t, uint32(101), intervals.Intervals[0].LastProcessedTick)
}

// TestPebbleStore_uncommittedTickWritesNothing: a tick whose batch is not committed (e.g.
// validation or a store call failed half-way) leaves no trace, not even partially.
func TestPebbleStore_uncommittedTickWritesNothing(t *testing.T) {
	ctx := context.Background()
	store, err := createTestDataStore(t)
	require.NoError(t, err)
	defer closeDb(store)

	storeTick(t, store, 42, 100, 2, true)
	storeTick(t, store, 42, 101, 2, false)

	last, err := store.GetLastProcessedTick(ctx)
	require.NoError(t, err)
	require.Equal(t, uint32(100), last.TickNumber)

	_, err = store.GetTickData(ctx, 101)
	require.ErrorIs(t, err, ErrNotFound)
	_, err = store.GetQuorumTickData(ctx, 101)
	require.ErrorIs(t, err, ErrNotFound)
	_, err = store.GetTransaction(ctx, "tx-101-0")
	require.ErrorIs(t, err, ErrNotFound)
	_, err = store.GetTickTransactionsStatus(ctx, 101)
	require.ErrorIs(t, err, ErrNotFound)

	intervals, err := store.GetProcessedTickIntervalsPerEpoch(ctx, 42)
	require.NoError(t, err)
	require.Equal(t, uint32(100), intervals.Intervals[0].LastProcessedTick)
	quorumPerInterval, err := store.GetLastTickQuorumDataListPerEpochInterval(42)
	require.NoError(t, err)
	require.Equal(t, uint32(100), quorumPerInterval.QuorumDataPerInterval[0].QuorumTickStructure.TickNumber)
}

// TestPebbleStore_batchReadsItsOwnWrites: read-modify-write store calls inside one tick (e.g.
// a new tick interval followed by the quorum data for it) see the earlier pending writes.
func TestPebbleStore_batchReadsItsOwnWrites(t *testing.T) {
	ctx := context.Background()
	store, err := createTestDataStore(t)
	require.NoError(t, err)
	defer closeDb(store)

	storeTick(t, store, 42, 100, 0, true)

	tickStore := store.BeginBatch()
	defer tickStore.DiscardBatch()

	require.NoError(t, tickStore.AppendProcessedTickInterval(ctx, 42, &protobuf.ProcessedTickInterval{InitialProcessedTick: 200, LastProcessedTick: 200}))
	require.NoError(t, tickStore.SetQuorumDataForCurrentEpochInterval(42, &protobuf.QuorumTickData{
		QuorumTickStructure: &protobuf.QuorumTickStructure{Epoch: 42, TickNumber: 200},
	}))
	require.NoError(t, tickStore.SetLastProcessedTickAndUpdateTickIntervals(ctx, &protobuf.ProcessedTick{TickNumber: 200, Epoch: 42}))

	pending, err := tickStore.GetProcessedTickIntervalsPerEpoch(ctx, 42)
	require.NoError(t, err)
	require.Len(t, pending.Intervals, 2, "the batch sees its own pending interval")

	committed, err := store.GetProcessedTickIntervalsPerEpoch(ctx, 42)
	require.NoError(t, err)
	require.Len(t, committed.Intervals, 1, "nothing is visible outside the batch before commit")

	require.NoError(t, tickStore.CommitBatch())

	committed, err = store.GetProcessedTickIntervalsPerEpoch(ctx, 42)
	require.NoError(t, err)
	require.Len(t, committed.Intervals, 2)
	require.Equal(t, uint32(200), committed.Intervals[1].LastProcessedTick)
	quorumPerInterval, err := store.GetLastTickQuorumDataListPerEpochInterval(42)
	require.NoError(t, err)
	require.Equal(t, uint32(200), quorumPerInterval.QuorumDataPerInterval[1].QuorumTickStructure.TickNumber,
		"quorum data lands in the interval appended earlier in the same batch")
}

// TestPebbleStore_batchRejectsWritesBetweenCommitAndDiscard: the processor defers the discard,
// so writes right after a commit must fail cleanly instead of reusing the committed batch.
func TestPebbleStore_batchRejectsWritesBetweenCommitAndDiscard(t *testing.T) {
	ctx := context.Background()
	store, err := createTestDataStore(t)
	require.NoError(t, err)
	defer closeDb(store)

	tickStore := store.BeginBatch()
	defer tickStore.DiscardBatch()
	require.NoError(t, tickStore.CommitBatch())

	require.ErrorIs(t, tickStore.SetTickData(ctx, 2, &protobuf.TickData{TickNumber: 2}), errBatchDone)
	require.ErrorIs(t, tickStore.SetTransactions(ctx, []*protobuf.Transaction{{TxId: "x"}}), errBatchDone)
	require.ErrorIs(t, tickStore.SetTickTransactionsStatus(ctx, 2, &protobuf.TickTransactionsStatus{}), errBatchDone)
	require.ErrorIs(t, tickStore.SetLastProcessedTick(ctx, &protobuf.ProcessedTick{TickNumber: 2, Epoch: 42}), errBatchDone)
}

func TestPebbleStore_batchRejectsWritesAfterCommit(t *testing.T) {
	ctx := context.Background()
	store, err := createTestDataStore(t)
	require.NoError(t, err)
	defer closeDb(store)

	tickStore := store.BeginBatch()
	require.NoError(t, tickStore.SetTickData(ctx, 1, &protobuf.TickData{TickNumber: 1}))
	require.NoError(t, tickStore.CommitBatch())
	tickStore.DiscardBatch()

	require.ErrorIs(t, tickStore.SetTickData(ctx, 2, &protobuf.TickData{TickNumber: 2}), errBatchDone)
	require.ErrorIs(t, tickStore.SetTransactions(ctx, []*protobuf.Transaction{{TxId: "x"}}), errBatchDone)
	require.ErrorIs(t, tickStore.CommitBatch(), errBatchDone)
	_, err = store.GetTickData(ctx, 2)
	require.ErrorIs(t, err, ErrNotFound)
}
