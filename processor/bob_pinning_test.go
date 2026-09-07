package processor

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/qubic/go-archiver-v2/db"
	"github.com/qubic/go-archiver-v2/network/bob"
	"github.com/qubic/go-archiver-v2/validator"
	"github.com/stretchr/testify/require"
)

// countingBobServer is bobServer plus a per-instance request tally, so a test can tell
// which instance a call actually landed on.
func countingBobServer(t *testing.T, indexingTick uint32) (*bob.Client, *atomic.Int64) {
	t.Helper()
	var hits atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		if r.URL.Path == "/status" {
			fmt.Fprintf(w, `{"currentFetchingTick":%d,"currentIndexingTick":%d}`, indexingTick, indexingTick)
			return
		}
		_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":1,"result":{"transactions":[]}}`))
	}))
	t.Cleanup(srv.Close)
	return bob.NewClient(srv.Client(), srv.URL), &hits
}

// rotatingBobProvider hands out a different instance on every call, standing in for the
// real provider's next random pick landing elsewhere, and records how many times the
// processor asked for one. Calls past the last client keep returning it.
type rotatingBobProvider struct {
	clients []*bob.Client
	calls   int
}

func (p *rotatingBobProvider) GetClient() (*bob.Client, error) {
	c := p.clients[min(p.calls, len(p.clients)-1)]
	p.calls++
	return c, nil
}

// bobFetchingValidator calls the bob tick fetcher the way the real validator does, so the
// live path issues a tick read and not just the /status round-trip. recordingValidator
// never touches clients.BobTicks, which would leave nothing to observe here.
type bobFetchingValidator struct {
	validated []uint32
}

func (v *bobFetchingValidator) Validate(ctx context.Context, _ *db.PebbleStore, clients validator.Clients, _ uint16, tickNumber uint32) error {
	v.validated = append(v.validated, tickNumber)
	if clients.BobTicks == nil {
		return nil
	}
	_, err := clients.BobTicks.FetchTick(ctx, tickNumber)
	return err
}

// A processing iteration must take one bob instance and use it for every bob call it
// makes. GetStatus sets the indexing ceiling, and the tick reads that ceiling authorises
// are only valid against the instance that reported it: instances are in sync within a
// tolerance, not at an identical tick, so a second instance can sit behind that ceiling
// and answer with a premature executed=false rather than an error. That validates clean
// and stores wrong moneyFlew, so the mistake would be silent.
//
// This covers routing only, i.e. which instance each call reached. Validation correctness
// belongs to the validator packages; here it is stubbed out.
func TestProcessor_usesOneBobInstancePerIteration(t *testing.T) {
	const nrTicks = 5

	newProcessor := func(t *testing.T, tickValidator Validator, provider *rotatingBobProvider, prefetch bool) *Processor {
		t.Helper()
		dataPool, err := db.NewDatabasePool(t.TempDir(), 5)
		require.NoError(t, err)
		client := &TestClient{epoch: 42, tick: 1000, InitialTick: 100, prefetchFn: cannedPrefetch}
		return NewProcessor(&TrackingPool{client: client}, dataPool, tickValidator, Config{
			ProcessTickTimeout: time.Second,
			BobProvider:        provider,
			PrefetchEnabled:    prefetch,
			PrefetchNrTicks:    nrTicks,
		}, dummyMetrics)
	}

	t.Run("prefetch batch", func(t *testing.T) {
		first, firstHits := countingBobServer(t, 1_000_000)
		second, secondHits := countingBobServer(t, 1_000_000)
		provider := &rotatingBobProvider{clients: []*bob.Client{first, second}}

		proc := newProcessor(t, &recordingValidator{}, provider, true)
		require.NoError(t, proc.processOneByOne())

		require.Equal(t, 1, provider.calls, "an iteration must take exactly one bob client")
		require.Zero(t, secondHits.Load(), "every bob call must go to the instance that reported the ceiling")
		// one GetStatus plus one tick read per tick in the batch
		require.Equal(t, int64(1+nrTicks), firstHits.Load())
	})

	// The default configuration: Prefetch.Enabled is false, so this is the path that runs
	// in production.
	t.Run("live path", func(t *testing.T) {
		first, firstHits := countingBobServer(t, 1_000_000)
		second, secondHits := countingBobServer(t, 1_000_000)
		provider := &rotatingBobProvider{clients: []*bob.Client{first, second}}

		rv := &bobFetchingValidator{}
		proc := newProcessor(t, rv, provider, false)
		require.NoError(t, proc.processOneByOne())

		require.Len(t, rv.validated, 1, "sanity: the validator ran and fetched the bob tick")
		require.Equal(t, 1, provider.calls, "an iteration must take exactly one bob client")
		require.Zero(t, secondHits.Load(), "every bob call must go to the instance that reported the ceiling")
		require.Equal(t, int64(2), firstHits.Load(), "one GetStatus plus one tick read")
	})
}
