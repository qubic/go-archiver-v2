package processor

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/qubic/go-archiver-v2/db"
	"github.com/qubic/go-archiver-v2/network/bob"
	"github.com/qubic/go-archiver-v2/validator"
	qubic "github.com/qubic/go-node-connector/v2"
	"github.com/qubic/go-node-connector/v2/types"
	"github.com/stretchr/testify/require"
)

// recordingValidator records the ticks it was asked to validate and returns a fixed result.
type recordingValidator struct {
	validated []uint32
	err       error
}

func (r *recordingValidator) Validate(_ context.Context, _ *db.PebbleStore, _ validator.Clients, _ uint16, tickNumber uint32) error {
	r.validated = append(r.validated, tickNumber)
	return r.err
}

// emptyBobServer returns a qubic_getTickByNumber response with no transactions, so
// bob.FetchTick succeeds for any tick (the recording validator ignores the data).
func emptyBobServer(t *testing.T) (*bob.Client, func()) {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":1,"result":{"transactions":[]}}`))
	}))
	return bob.NewClient(srv.URL), srv.Close
}

func cannedPrefetch(startTick, n uint32) (qubic.PrefetchResult, error) {
	res := qubic.PrefetchResult{
		SystemInfo: types.SystemInfo{InitialTick: 100},
		TickInfo:   types.TickInfo{Tick: 1000, Epoch: 42, InitialTick: 100},
		Ticks:      make([]qubic.TickPrefetch, n),
	}
	for i := uint32(0); i < n; i++ {
		res.Ticks[i] = qubic.TickPrefetch{Tick: startTick + i}
	}
	return res, nil
}

func TestProcessor_prefetchServesBatchFromCacheThenReprefetches(t *testing.T) {
	bobClient, stop := emptyBobServer(t)
	defer stop()

	const nrTicks = 5
	client := &TestClient{epoch: 42, tick: 1000, InitialTick: 100, prefetchFn: cannedPrefetch}
	pool := &TrackingPool{client: client}

	dataPool, err := db.NewDatabasePool(t.TempDir(), 5)
	require.NoError(t, err)

	rv := &recordingValidator{}
	proc := NewProcessor(pool, dataPool, rv, Config{
		ProcessTickTimeout: time.Second,
		BobClient:          bobClient,
		PrefetchEnabled:    true,
		PrefetchNrTicks:    nrTicks,
	}, dummyMetrics)

	// the first 5 ticks come from one prefetched batch
	for i := 0; i < nrTicks; i++ {
		require.NoError(t, proc.processOneByOne())
	}
	require.Equal(t, []uint32{100, 101, 102, 103, 104}, rv.validated)
	require.Equal(t, 1, client.prefetchCalls, "one batch should serve all 5 ticks from cache")

	// the 6th tick falls outside the batch -> a second prefetch
	require.NoError(t, proc.processOneByOne())
	require.Equal(t, 2, client.prefetchCalls, "exhausting the batch triggers a re-prefetch")
	require.Equal(t, uint32(105), rv.validated[len(rv.validated)-1])
}

func TestProcessor_prefetchFallsBackToLiveNearTip(t *testing.T) {
	bobClient, stop := emptyBobServer(t)
	defer stop()

	// tick=101 with nextTick=100 and nrTicks=5 cannot fit a full batch (needs tick >= 104),
	// so prefetch is skipped and the live path runs. prefetchFn is nil => panics if called.
	client := &TestClient{epoch: 42, tick: 101, InitialTick: 100}
	pool := &TrackingPool{client: client}

	dataPool, err := db.NewDatabasePool(t.TempDir(), 5)
	require.NoError(t, err)

	rv := &recordingValidator{}
	proc := NewProcessor(pool, dataPool, rv, Config{
		ProcessTickTimeout: time.Second,
		BobClient:          bobClient,
		PrefetchEnabled:    true,
		PrefetchNrTicks:    5,
	}, dummyMetrics)

	require.NoError(t, proc.processOneByOne())
	require.Equal(t, 0, client.prefetchCalls, "no prefetch when a full batch can't fit before the tip")
	require.Equal(t, []uint32{100}, rv.validated)
}
