package processor

import (
	"context"
	"encoding/json"
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

// blockingBobServer responds instantly for every tick except blockTick, for which it
// blocks until the request context is cancelled. It lets us prove a batch fails fast on
// a single slow tick rather than draining the full per-tick budget.
func blockingBobServer(t *testing.T, blockTick uint32) (*bob.Client, func()) {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Params []json.RawMessage `json:"params"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)
		var tick uint32
		if len(req.Params) > 0 {
			_ = json.Unmarshal(req.Params[0], &tick)
		}
		if tick == blockTick {
			<-r.Context().Done() // hang until the per-fetch timeout cancels us
			return
		}
		_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":1,"result":{"transactions":[]}}`))
	}))
	return bob.NewClient(srv.URL), srv.Close
}

func TestProcessor_prefetchFailsFastWhenOneBobTickHangs(t *testing.T) {
	const nrTicks = 5
	// batch covers ticks 100..104 (InitialTick=100, nothing processed yet); hang tick 102.
	bobClient, stop := blockingBobServer(t, 102)
	defer stop()

	client := &TestClient{epoch: 42, tick: 1000, InitialTick: 100, prefetchFn: cannedPrefetch}
	pool := &TrackingPool{client: client}

	dataPool, err := db.NewDatabasePool(t.TempDir(), 5)
	require.NoError(t, err)

	rv := &recordingValidator{}
	proc := NewProcessor(pool, dataPool, rv, Config{
		ProcessTickTimeout: 5 * time.Second, // generous: must NOT be what bounds the failure
		BobClient:          bobClient,
		PrefetchEnabled:    true,
		PrefetchNrTicks:    nrTicks,
		PrefetchBobTimeout: 100 * time.Millisecond,
	}, dummyMetrics)

	start := time.Now()
	err = proc.processOneByOne()
	elapsed := time.Since(start)

	require.Error(t, err)
	require.Contains(t, err.Error(), "bob fetch tick 102", "the hung tick should be named in the error")
	require.Less(t, elapsed, time.Second, "batch should fail fast (≈ bob timeout), not drain the 5s process budget")
	require.Empty(t, rv.validated, "no tick should be validated when the batch fails")
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
