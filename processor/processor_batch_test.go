package processor

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/qubic/go-archiver-v2/db"
	"github.com/qubic/go-archiver-v2/protobuf"
	"github.com/qubic/go-archiver-v2/validator"
	"github.com/stretchr/testify/require"
)

// writingValidator stores a marker transaction per validation attempt (like the real
// validator's store phase) and fails the attempts listed in failAttempts.
type writingValidator struct {
	attempts     int
	failAttempts map[int]bool
}

func (w *writingValidator) Validate(ctx context.Context, store *db.PebbleStore, _ validator.Clients, _ uint16, tickNumber uint32) error {
	w.attempts++
	err := store.SetTransactions(ctx, []*protobuf.Transaction{{TxId: attemptTxID(tickNumber, w.attempts), TickNumber: tickNumber}})
	if err != nil {
		return err
	}
	if w.failAttempts[w.attempts] {
		return errors.New("validation failed after storing")
	}
	return nil
}

func attemptTxID(tick uint32, attempt int) string {
	return fmt.Sprintf("tick-%d-attempt-%d", tick, attempt)
}

func TestProcessor_failedTickWritesNothing(t *testing.T) {
	client := &TestClient{epoch: 42, tick: 1000, InitialTick: 100}
	dataPool, err := db.NewDatabasePool(t.TempDir(), 5)
	require.NoError(t, err)
	wv := &writingValidator{failAttempts: map[int]bool{1: true}}
	proc := NewProcessor(&TrackingPool{client: client}, dataPool, wv, Config{ProcessTickTimeout: time.Second}, dummyMetrics)

	require.Error(t, proc.processOneByOne())

	store, err := dataPool.GetDbForEpoch(42)
	require.NoError(t, err)
	_, err = store.GetTransaction(context.Background(), attemptTxID(100, 1))
	require.ErrorIs(t, err, db.ErrNotFound, "writes of a failed tick must not be committed")
	_, err = store.GetLastProcessedTick(context.Background())
	require.ErrorIs(t, err, db.ErrNotFound)

	// the retry commits the tick's writes together with the processed tick marker
	require.NoError(t, proc.processOneByOne())
	_, err = store.GetTransaction(context.Background(), attemptTxID(100, 2))
	require.NoError(t, err)
	last, err := store.GetLastProcessedTick(context.Background())
	require.NoError(t, err)
	require.Equal(t, uint32(100), last.TickNumber)
}

// TestProcessor_prefetchFallbackDropsFailedAttemptWrites: when a prefetched tick fails
// validation and is retried live in the same iteration, only the live attempt's writes land.
func TestProcessor_prefetchFallbackDropsFailedAttemptWrites(t *testing.T) {
	bobClient, stop := emptyBobServer(t)
	defer stop()

	client := &TestClient{epoch: 42, tick: 1000, InitialTick: 100, prefetchFn: cannedPrefetch}
	dataPool, err := db.NewDatabasePool(t.TempDir(), 5)
	require.NoError(t, err)
	wv := &writingValidator{failAttempts: map[int]bool{1: true}} // prefetched attempt fails
	proc := NewProcessor(&TrackingPool{client: client}, dataPool, wv, Config{
		ProcessTickTimeout: time.Second,
		BobProvider:        stubBobProvider{client: bobClient},
		PrefetchEnabled:    true,
		PrefetchNrTicks:    5,
	}, dummyMetrics)

	require.NoError(t, proc.processOneByOne())
	require.Equal(t, 2, wv.attempts, "prefetched attempt, then live fallback")

	store, err := dataPool.GetDbForEpoch(42)
	require.NoError(t, err)
	_, err = store.GetTransaction(context.Background(), attemptTxID(100, 1))
	require.ErrorIs(t, err, db.ErrNotFound, "the failed prefetched attempt must leave nothing behind")
	_, err = store.GetTransaction(context.Background(), attemptTxID(100, 2))
	require.NoError(t, err)
}
