package processor

import (
	"context"
	"encoding/json"
	"fmt"
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

// stubBobProvider hands out one fixed client, standing in for the real provider's random
// pick over the instances the fetcher reports. Since the processor takes a client once per
// iteration, that is all these tests need.
type stubBobProvider struct{ client *bob.Client }

func (s stubBobProvider) GetClient() (*bob.Client, error) { return s.client, nil }

// bobServer answers GET /status with the given indexing frontier and every
// qubic_getTickByNumber with an empty transaction list (bob.FetchTick succeeds for any
// tick; the recording validator ignores the data).
func bobServer(t *testing.T, indexingTick uint32) (*bob.Client, func()) {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/status" {
			fmt.Fprintf(w, `{"currentFetchingTick":%d,"currentIndexingTick":%d}`, indexingTick, indexingTick)
			return
		}
		_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":1,"result":{"transactions":[]}}`))
	}))
	return bob.NewClient(srv.Client(), srv.URL), srv.Close
}

// emptyBobServer reports an indexing frontier far ahead so the indexing gate never trips;
// tests that exercise the gate use bobServer with a specific frontier.
func emptyBobServer(t *testing.T) (*bob.Client, func()) {
	return bobServer(t, 1_000_000)
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
		BobProvider:        stubBobProvider{client: bobClient},
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
		if r.URL.Path == "/status" {
			// indexing far ahead so the gate passes and the batch is built (then a single
			// tick's fetch hangs, exercising fail-fast)
			fmt.Fprint(w, `{"currentFetchingTick":1000000,"currentIndexingTick":1000000}`)
			return
		}
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
	return bob.NewClient(srv.Client(), srv.URL), srv.Close
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
		BobProvider:        stubBobProvider{client: bobClient},
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

func TestBobCeiling(t *testing.T) {
	require.Equal(t, uint32(98), bobCeiling(1000, 99), "indexing-1 below node tick wins")
	require.Equal(t, uint32(1000), bobCeiling(1000, 5000), "node tick wins when indexing is far ahead")
	require.Equal(t, uint32(99), bobCeiling(100, 100), "indexing-1 == node tick-1")
	require.Equal(t, uint32(0), bobCeiling(100, 0), "indexing 0 clamps to 0 (forces wait)")
	require.Equal(t, uint32(0), bobCeiling(100, 1), "indexing 1 -> 0")
}

// capturingPrefetch wraps cannedPrefetch and records the nrTicks it was asked for, so the
// clamp tests can assert the batch was shrunk to the ceiling.
func capturingPrefetch(got *uint32) func(startTick, n uint32) (qubic.PrefetchResult, error) {
	return func(startTick, n uint32) (qubic.PrefetchResult, error) {
		*got = n
		return cannedPrefetch(startTick, n)
	}
}

// TestProcessor_prefetchClampsToNodeTip: with bob indexed far ahead, the batch is clamped
// to the node tip (nextTick 100, node 101 -> 2 ticks) instead of falling back to live.
func TestProcessor_prefetchClampsToNodeTip(t *testing.T) {
	bobClient, stop := emptyBobServer(t) // indexing far ahead -> ceiling = node tip
	defer stop()

	var gotNrTicks uint32
	client := &TestClient{epoch: 42, tick: 101, InitialTick: 100, prefetchFn: capturingPrefetch(&gotNrTicks)}
	pool := &TrackingPool{client: client}

	dataPool, err := db.NewDatabasePool(t.TempDir(), 5)
	require.NoError(t, err)

	rv := &recordingValidator{}
	proc := NewProcessor(pool, dataPool, rv, Config{
		ProcessTickTimeout: time.Second,
		BobProvider:        stubBobProvider{client: bobClient},
		PrefetchEnabled:    true,
		PrefetchNrTicks:    5,
	}, dummyMetrics)

	require.NoError(t, proc.processOneByOne())
	require.Equal(t, 1, client.prefetchCalls, "prefetch still runs, just clamped")
	require.Equal(t, uint32(2), gotNrTicks, "clamped to node tip: 101-100+1")
	require.Equal(t, []uint32{100}, rv.validated)
}

// TestProcessor_prefetchClampsToBobIndexingFrontier: the node has the whole batch but bob
// hasn't indexed it, so the batch is clamped to bob's indexing frontier (not the node tip).
func TestProcessor_prefetchClampsToBobIndexingFrontier(t *testing.T) {
	// node tick 1000, nextTick 100, indexing 103 => ceiling 102 => avail 3 ticks.
	bobClient, stop := bobServer(t, 103)
	defer stop()

	var gotNrTicks uint32
	client := &TestClient{epoch: 42, tick: 1000, InitialTick: 100, prefetchFn: capturingPrefetch(&gotNrTicks)}
	pool := &TrackingPool{client: client}

	dataPool, err := db.NewDatabasePool(t.TempDir(), 5)
	require.NoError(t, err)

	rv := &recordingValidator{}
	proc := NewProcessor(pool, dataPool, rv, Config{
		ProcessTickTimeout: time.Second,
		BobProvider:        stubBobProvider{client: bobClient},
		PrefetchEnabled:    true,
		PrefetchNrTicks:    5,
	}, dummyMetrics)

	require.NoError(t, proc.processOneByOne())
	require.Equal(t, 1, client.prefetchCalls)
	require.Equal(t, uint32(3), gotNrTicks, "clamped to bob indexing frontier: 102-100+1")
	require.Equal(t, []uint32{100}, rv.validated)
}

// TestProcessor_idleWhenNextTickBeyondIndexing: bob has not indexed up to nextTick, so
// there is nothing to sync this round — the processor returns an error and validates nothing.
func TestProcessor_idleWhenNextTickBeyondIndexing(t *testing.T) {
	// indexing 100 => ceiling 99; nextTick 100 > 99 => nothing to sync.
	bobClient, stop := bobServer(t, 100)
	defer stop()

	client := &TestClient{epoch: 42, tick: 1000, InitialTick: 100} // prefetchFn nil => panics if prefetched
	pool := &TrackingPool{client: client}

	dataPool, err := db.NewDatabasePool(t.TempDir(), 5)
	require.NoError(t, err)

	rv := &recordingValidator{}
	proc := NewProcessor(pool, dataPool, rv, Config{
		ProcessTickTimeout: time.Second,
		BobProvider:        stubBobProvider{client: bobClient},
		PrefetchEnabled:    true,
		PrefetchNrTicks:    5,
	}, dummyMetrics)

	err = proc.processOneByOne()
	require.Error(t, err)
	require.Contains(t, err.Error(), "no ticks to sync")
	require.Equal(t, 0, client.prefetchCalls)
	require.Empty(t, rv.validated, "nothing should be validated when caught up to bob's frontier")
}
