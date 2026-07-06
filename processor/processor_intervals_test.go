package processor

import (
	"context"
	"testing"

	"github.com/qubic/go-archiver-v2/db"
	"github.com/qubic/go-archiver-v2/protobuf"
	"github.com/stretchr/testify/require"
)

// Covers issue #35: when the node's InitialTick jumps ahead of the last
// processed tick (skipped ticks / restart within an epoch), the skip must open
// a NEW processed-tick interval rather than silently extending the previous one.
// It runs the handleTickIntervals + updateProcessedTick sequence that
// processOneByOne performs on each tick and asserts the append-vs-extend
// bookkeeping is correct.
//
// Scope: this calls handleTickIntervals directly, so it does NOT exercise the
// `lastProcessedTick >= InitialTick` gate that once wrapped the call in
// processOneByOne (that path needs a mocked client pool / tick-info fetch we
// don't have). It guards the interval logic the gate removal relies on, not the
// gate removal itself.
func TestProcessor_NewIntervalUponSkippedTicks(t *testing.T) {
	ctx := context.Background()
	epoch := uint16(200)

	dataPool, err := db.NewDatabasePool(t.TempDir(), 1)
	require.NoError(t, err)
	epochDb, err := dataPool.GetOrCreateDbForEpoch(epoch)
	require.NoError(t, err)

	p := NewProcessor(nil, dataPool, nil, Config{}, nil)

	// Seed state: we've processed up to tick 1000 -> one interval [{1000, 1000}].
	require.NoError(t, p.updateProcessedTick(ctx, epochDb,
		&protobuf.ProcessedTick{TickNumber: 1000, Epoch: uint32(epoch)}))

	intervals, err := epochDb.GetProcessedTickIntervalsPerEpoch(ctx, uint32(epoch))
	require.NoError(t, err)
	require.Len(t, intervals.Intervals, 1)

	// Skip: node reports InitialTick 1050, so nextTick jumps 1000 -> 1050.
	lastTick := &protobuf.ProcessedTick{TickNumber: 1000, Epoch: uint32(epoch)}
	nextTick := &protobuf.ProcessedTick{TickNumber: 1050, Epoch: uint32(epoch)}

	require.NoError(t, p.handleTickIntervals(ctx, epochDb, lastTick, nextTick))
	require.NoError(t, p.updateProcessedTick(ctx, epochDb, nextTick))

	// Expect a second interval opened at the skip boundary, not an extension of the first.
	intervals, err = epochDb.GetProcessedTickIntervalsPerEpoch(ctx, uint32(epoch))
	require.NoError(t, err)
	require.Len(t, intervals.Intervals, 2)
	require.Equal(t, uint32(1000), intervals.Intervals[0].InitialProcessedTick)
	require.Equal(t, uint32(1000), intervals.Intervals[0].LastProcessedTick)
	require.Equal(t, uint32(1050), intervals.Intervals[1].InitialProcessedTick)
	require.Equal(t, uint32(1050), intervals.Intervals[1].LastProcessedTick)

	// Contiguous follow-up tick 1051 extends the current interval (no new interval).
	next := &protobuf.ProcessedTick{TickNumber: 1051, Epoch: uint32(epoch)}
	require.NoError(t, p.handleTickIntervals(ctx, epochDb, nextTick, next))
	require.NoError(t, p.updateProcessedTick(ctx, epochDb, next))

	intervals, err = epochDb.GetProcessedTickIntervalsPerEpoch(ctx, uint32(epoch))
	require.NoError(t, err)
	require.Len(t, intervals.Intervals, 2)
	require.Equal(t, uint32(1051), intervals.Intervals[1].LastProcessedTick)
}
