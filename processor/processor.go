package processor

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/qubic/go-archiver-v2/db"
	"github.com/qubic/go-archiver-v2/metrics"
	"github.com/qubic/go-archiver-v2/network"
	"github.com/qubic/go-archiver-v2/network/bob"
	"github.com/qubic/go-archiver-v2/protobuf"
	"github.com/qubic/go-archiver-v2/tracing"
	"github.com/qubic/go-archiver-v2/validator"
	qubic "github.com/qubic/go-node-connector/v2"
	"github.com/qubic/go-node-connector/v2/types"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"golang.org/x/sync/errgroup"
)

type Validator interface {
	Validate(ctx context.Context, store *db.PebbleStore, client validator.Clients, epoch uint16, tickNumber uint32) error
}

type TickStatus struct {
	ProcessingEpoch uint16
	ProcessedTick   uint32
	LiveEpoch       uint16
	LiveTick        uint32
}

type Processor struct {
	clientPool           network.QubicClientPool
	databasePool         *db.DatabasePool
	tickValidator        Validator
	arbitratorPubKey     [32]byte
	processTickTimeout   time.Duration
	retryDelay           time.Duration
	tickStatus           *TickStatus
	startFromCurrentTick bool
	metrics              *metrics.ProcessingMetrics

	bobClient       *bob.Client
	prefetchEnabled bool
	prefetchNrTicks uint32
	bobFetchTimeout time.Duration
	currentBatch    *prefetchBatch
}

type Config struct {
	ProcessTickTimeout time.Duration
	RetryDelay         time.Duration
	BobClient          *bob.Client
	PrefetchEnabled    bool
	PrefetchNrTicks    uint32
	PrefetchBobTimeout time.Duration
}

func NewProcessor(clientPool network.QubicClientPool, dbPool *db.DatabasePool, tickValidator Validator, config Config, metrics *metrics.ProcessingMetrics) *Processor {
	retryDelay := config.RetryDelay
	if retryDelay <= 0 {
		retryDelay = 200 * time.Millisecond
	}
	return &Processor{
		clientPool:         clientPool,
		databasePool:       dbPool,
		processTickTimeout: config.ProcessTickTimeout,
		retryDelay:         retryDelay,
		tickValidator:      tickValidator,
		tickStatus:         &TickStatus{},
		metrics:            metrics,
		bobClient:          config.BobClient,
		prefetchEnabled:    config.PrefetchEnabled,
		prefetchNrTicks:    config.PrefetchNrTicks,
		bobFetchTimeout:    config.PrefetchBobTimeout,
	}
}

// prefetchBatch caches one pipelined PrefetchTicks response (node data) plus the
// bob tx-status fetched in parallel for the same ticks. It is reused across
// processOneByOne calls until exhausted or the epoch changes.
type prefetchBatch struct {
	epoch     uint16
	result    qubic.PrefetchResult
	byTick    map[uint32]qubic.TickPrefetch
	bobByTick map[uint32]bob.TickData
}

func (b *prefetchBatch) covers(tick uint32, epoch uint16) bool {
	if b == nil || b.epoch != epoch {
		return false
	}
	_, ok := b.byTick[tick]
	return ok
}

func (p *Processor) Start() error {
	for {
		err := p.processOneByOne()
		if err != nil {
			log.Printf("Processing failed: %s", err.Error())
			time.Sleep(p.retryDelay)
		}
	}
}

func (p *Processor) GetTickStatus() *TickStatus {
	return p.tickStatus
}

func (p *Processor) processOneByOne() (err error) {
	start := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), p.processTickTimeout)
	defer cancel()

	ctx, rootSpan := tracing.Tracer().Start(ctx, "process_tick")
	defer func() {
		if err != nil {
			rootSpan.RecordError(err)
			rootSpan.SetStatus(codes.Error, err.Error())
		}
		rootSpan.End()
	}()

	// note: do not reassign ctx here, otherwise sibling phases would nest under pool_get
	_, poolSpan := tracing.Tracer().Start(ctx, "pool_get")
	rawClient, err := p.clientPool.Get()
	if err != nil {
		poolSpan.End()
		return fmt.Errorf("getting client connection: %w", err)
	}
	defer func() {
		p.releaseClient(err, rawClient)
	}()
	poolSpan.End()

	// wrap the raw client with the tracing decorator; the raw client is returned to the
	// pool. The second connection is only acquired on the live path (see validateLive).
	client := network.NewTracingQubicClient(rawClient)

	tickInfo, err := client.GetTickInfo(ctx)
	if err != nil {
		return fmt.Errorf("getting tick info: %w", err)
	}

	p.tickStatus.LiveTick = tickInfo.Tick
	p.tickStatus.LiveEpoch = tickInfo.Epoch

	dataStore, err := p.databasePool.GetOrCreateDbForEpoch(tickInfo.Epoch)
	if err != nil {
		return fmt.Errorf("getting database: %w", err)
	}

	lastProcessedTick, err := p.getLastProcessedTick(ctx, dataStore, tickInfo.Epoch)
	if err != nil {
		return fmt.Errorf("getting last processed tick: %w", err)
	}
	p.tickStatus.ProcessedTick = lastProcessedTick.TickNumber
	p.tickStatus.ProcessingEpoch = uint16(lastProcessedTick.Epoch)

	nextTick, err := p.getNextProcessingTick(ctx, lastProcessedTick, tickInfo)
	if err != nil {
		return fmt.Errorf("getting next tick to process: %w", err)
	}
	rootSpan.SetAttributes(
		attribute.Int64("tick.number", int64(nextTick.TickNumber)),
		attribute.Int64("tick.live", int64(tickInfo.Tick)),
		attribute.Int64("tick.delta", int64(tickInfo.Tick)-int64(nextTick.TickNumber)),
	)
	log.Printf("Next tick to process: [%d]. Current tick: [%d]. Delta [%d]", nextTick.TickNumber, tickInfo.Tick, int64(tickInfo.Tick)-int64(nextTick.TickNumber))

	if nextTick.TickNumber > tickInfo.Tick {
		return fmt.Errorf("next tick is in the future. processed: %d, next %d, available %d",
			lastProcessedTick.TickNumber, nextTick.TickNumber, tickInfo.Tick)
	}

	// not sure if this helps because we will often be aligned at time of processing
	if nextTick.TickNumber == tickInfo.Tick && tickInfo.NumberOfAlignedVotes < 451 {
		return fmt.Errorf("tick not ready ([%d] aligned votes)", tickInfo.NumberOfAlignedVotes)
	}

	// Gate on bob's indexing frontier: bob serves a premature executed=false for ticks it
	// has fetched but not yet indexed, so never consume bob beyond min(node tick,
	// currentIndexingTick-1). A cached batch was already gated at build time and the
	// ceiling only rises, so cache hits skip the /status round-trip and stay valid.
	ceiling := tickInfo.Tick
	var bobIndexingTick uint32
	useBob := p.bobClient != nil
	cachedHit := p.prefetchEnabled && useBob && p.currentBatch.covers(nextTick.TickNumber, tickInfo.Epoch)
	if useBob && !cachedHit {
		st, serr := p.bobClient.GetStatus(ctx)
		if serr != nil {
			err = fmt.Errorf("getting bob status: %w", serr)
			return err
		}
		bobIndexingTick = st.CurrentIndexingTick
		ceiling = bobCeiling(tickInfo.Tick, st.CurrentIndexingTick)
		rootSpan.SetAttributes(
			attribute.Int64("bob.indexing_tick", int64(st.CurrentIndexingTick)),
			attribute.Int64("tick.ceiling", int64(ceiling)),
		)
		if int64(nextTick.TickNumber) > int64(ceiling) {
			// caught up to bob's indexed frontier: no tick is safe to sync yet
			return fmt.Errorf("no ticks to sync up to bob indexed frontier: next tick %d > ceiling %d (node %d, bob indexing %d)",
				nextTick.TickNumber, ceiling, tickInfo.Tick, st.CurrentIndexingTick)
		}
	}

	// Prefetch fast path: serve this tick (and the next nrTicks-1) from one pipelined
	// batch when possible; otherwise fall back to the live single-tick path.
	if p.prefetchEnabled && p.bobClient != nil {
		served, pErr := p.processFromPrefetch(ctx, client, dataStore, tickInfo, nextTick, ceiling, bobIndexingTick)
		if pErr != nil {
			err = pErr
			return err
		}
		if !served {
			if err = p.validateLive(ctx, client, dataStore, tickInfo, nextTick); err != nil {
				return err
			}
		}
	} else {
		if err = p.validateLive(ctx, client, dataStore, tickInfo, nextTick); err != nil {
			return err
		}
	}

	err = p.handleTickIntervals(ctx, dataStore, lastProcessedTick, nextTick)
	if err != nil {
		return fmt.Errorf("handling skipped ticks: %w", err)
	}

	err = p.updateProcessedTick(ctx, dataStore, nextTick)
	if err != nil {
		return fmt.Errorf("storing processed tick: %w", err)
	}

	p.metrics.SetLastProcessedTick(nextTick.TickNumber)
	p.metrics.SetCurrentEpoch(nextTick.Epoch)

	log.Printf("Successfully processed tick [%d] in %dms.", nextTick.TickNumber, time.Since(start).Milliseconds())
	return nil
}

// processFromPrefetch serves nextTick from the cached batch, fetching a new batch first
// when the current one doesn't cover it. The new batch is clamped to bob's indexing
// ceiling so it never reads bob past the indexed frontier. It returns served=false (with
// no error) when a prefetched tick fails validation, so the caller falls back to the live
// path. A non-nil error is a prefetch-infrastructure failure that should abort the iteration.
func (p *Processor) processFromPrefetch(ctx context.Context, client network.QubicClient, dataStore *db.PebbleStore, tickInfo types.TickInfo, nextTick *protobuf.ProcessedTick, ceiling, bobIndexingTick uint32) (served bool, err error) {
	epoch := tickInfo.Epoch
	if !p.currentBatch.covers(nextTick.TickNumber, epoch) {
		// Clamp the batch so it never reads bob past the ceiling (bob's indexing frontier):
		// sync only the ticks available up to the ceiling. The caller guarantees
		// nextTick <= ceiling, so this is always >= 1; near the frontier the batch just
		// shrinks instead of overshooting or falling back to the live path.
		nrTicks := p.prefetchNrTicks
		if avail := ceiling - nextTick.TickNumber + 1; avail < nrTicks {
			nrTicks = avail
		}
		log.Printf("Prefetch batch [%d..%d] (%d ticks). Node tick [%d], bob indexing tick [%d].",
			nextTick.TickNumber, nextTick.TickNumber+nrTicks-1, nrTicks, tickInfo.Tick, bobIndexingTick)
		batch, bErr := p.prefetchBatch(ctx, client, nextTick.TickNumber, nrTicks, epoch)
		if bErr != nil {
			return false, fmt.Errorf("prefetching batch at tick %d: %w", nextTick.TickNumber, bErr)
		}
		p.currentBatch = batch
	}

	tp := p.currentBatch.byTick[nextTick.TickNumber]
	adapter := network.NewPrefetchedClient(client, tickInfo, p.currentBatch.result.SystemInfo, tp)
	clients := validator.Clients{
		Main:     adapter,
		Alt:      adapter,
		BobTicks: prefetchedBobFetcher{ticks: p.currentBatch.bobByTick},
	}
	if vErr := p.tickValidator.Validate(ctx, dataStore, clients, epoch, nextTick.TickNumber); vErr != nil {
		log.Printf("[WARN] prefetched tick %d failed validation: %v; dropping batch, retrying live.", nextTick.TickNumber, vErr)
		p.currentBatch = nil
		return false, nil
	}
	return true, nil
}

// bobCeiling returns the newest tick safe to consume from bob: min(node tick,
// currentIndexingTick-1). The -1 keeps a one-tick margin below bob's indexing frontier
// (ticks at/above it may still report a premature executed=false). int64 math guards the
// indexing==0 / underflow case, which clamps to 0 and forces a wait.
func bobCeiling(nodeTick, indexingTick uint32) uint32 {
	ceil := int64(nodeTick)
	if bobMax := int64(indexingTick) - 1; bobMax < ceil {
		ceil = bobMax
	}
	if ceil < 0 {
		ceil = 0
	}
	return uint32(ceil)
}

// prefetchBatch pipelines the node responses for [startTick, startTick+nrTicks) and,
// in parallel, fetches bob's tx status for each of those ticks. nrTicks is clamped by the
// caller to bob's indexing frontier, so it can be smaller than the configured size.
func (p *Processor) prefetchBatch(ctx context.Context, client network.QubicClient, startTick, nrTicks uint32, epoch uint16) (*prefetchBatch, error) {
	ctx, span := tracing.Tracer().Start(ctx, "prefetch_batch")
	defer span.End()

	result, err := client.PrefetchTicks(ctx, startTick, nrTicks)
	if err != nil {
		return nil, fmt.Errorf("prefetch ticks: %w", err)
	}

	byTick := make(map[uint32]qubic.TickPrefetch, len(result.Ticks))
	for _, t := range result.Ticks {
		byTick[t.Tick] = t
	}

	bobByTick := make(map[uint32]bob.TickData, len(result.Ticks))
	var mu sync.Mutex
	eg, egCtx := errgroup.WithContext(ctx)
	for _, t := range result.Ticks {
		tick := t.Tick
		eg.Go(func() error {
			// Each fetch gets its own (shorter) deadline so a single slow/unreachable
			// tick trips quickly; errgroup then cancels the siblings and the whole batch
			// fails fast, instead of every fetch draining the full per-tick budget.
			fctx, fcancel := egCtx, context.CancelFunc(func() {})
			if p.bobFetchTimeout > 0 {
				fctx, fcancel = context.WithTimeout(egCtx, p.bobFetchTimeout)
			}
			defer fcancel()

			bt, err := bob.FetchTick(fctx, p.bobClient, tick)
			if err != nil {
				return fmt.Errorf("bob fetch tick %d: %w", tick, err)
			}
			mu.Lock()
			bobByTick[tick] = bt
			mu.Unlock()
			return nil
		})
	}
	if err := eg.Wait(); err != nil {
		return nil, fmt.Errorf("prefetching bob statuses: %w", err)
	}

	return &prefetchBatch{epoch: epoch, result: result, byTick: byTick, bobByTick: bobByTick}, nil
}

// validateLive runs the validator against live node connections (the normal,
// non-prefetch path). It acquires the second connection lazily, since the prefetch
// path doesn't need it.
func (p *Processor) validateLive(ctx context.Context, client network.QubicClient, dataStore *db.PebbleStore, tickInfo types.TickInfo, nextTick *protobuf.ProcessedTick) (err error) {
	rawAltClient, err := p.clientPool.Get()
	if err != nil {
		return fmt.Errorf("getting 2nd client connection: %w", err)
	}
	defer func() {
		p.releaseClient(err, rawAltClient)
	}()
	alternativeClient := network.NewTracingQubicClient(rawAltClient)

	var bobSource validator.BobTickFetcher
	if p.bobClient != nil {
		bobSource = liveBobFetcher{client: p.bobClient}
	}

	clients := validator.Clients{Main: client, Alt: alternativeClient, BobTicks: bobSource}
	if err = p.tickValidator.Validate(ctx, dataStore, clients, tickInfo.Epoch, nextTick.TickNumber); err != nil {
		return fmt.Errorf("validating tick %d: %w", nextTick.TickNumber, err)
	}
	return nil
}

func (p *Processor) releaseClient(err error, client network.QubicClient) {
	if err == nil {
		pErr := p.clientPool.Put(client)
		if pErr != nil {
			log.Printf("[ERROR] putting connection back to pool: %s", pErr.Error())
		}
	} else {
		log.Printf("Closing connection because of error: %v", err)
		cErr := p.clientPool.Close(client)
		if cErr != nil {
			log.Printf("[ERROR] closing connection: %s", cErr.Error())
		}
	}
}

func (p *Processor) getLastProcessedTick(ctx context.Context, dataStore *db.PebbleStore, epoch uint16) (*protobuf.ProcessedTick, error) {

	lastTick, err := dataStore.GetLastProcessedTick(ctx)
	if err != nil {
		// handles first run of the archiver where there is nothing in storage or epoch change
		// in this case last tick is 0 and epoch is current tick info epoch
		if errors.Is(err, db.ErrNotFound) {
			return &protobuf.ProcessedTick{TickNumber: 0, Epoch: uint32(epoch)}, nil
		}
		return nil, fmt.Errorf("getting last processed tick for epoch %d: %w", epoch, err)
	}
	return lastTick, nil
}

func (p *Processor) getNextProcessingTick(_ context.Context, lastTick *protobuf.ProcessedTick, currentTickInfo types.TickInfo) (*protobuf.ProcessedTick, error) {
	// handles the case where the initial tick of epoch returned by the node is greater than the last processed tick
	// which means that we are in the next epoch, and we should start from the initial tick of the current epoch
	if currentTickInfo.InitialTick > lastTick.TickNumber {
		return &protobuf.ProcessedTick{TickNumber: currentTickInfo.InitialTick, Epoch: uint32(currentTickInfo.Epoch)}, nil
	}

	// otherwise we are in the same epoch, and we should start from the last processed tick + 1
	return &protobuf.ProcessedTick{TickNumber: lastTick.TickNumber + 1, Epoch: lastTick.Epoch}, nil
}

func (p *Processor) updateProcessedTick(ctx context.Context, dataStore *db.PebbleStore, tick *protobuf.ProcessedTick) error {
	err := dataStore.SetLastProcessedTickAndUpdateTickIntervals(ctx, tick)
	if err != nil {
		return fmt.Errorf("setting last processed tick [%d]: %w", tick.TickNumber, err)
	}
	return nil
}

func (p *Processor) handleTickIntervals(ctx context.Context, dataStore *db.PebbleStore, lastTick *protobuf.ProcessedTick, nextTick *protobuf.ProcessedTick) error {

	if nextTick.TickNumber-lastTick.TickNumber == 1 {
		// no skipped ticks. default case. no new tick interval.
		return nil
	}

	if nextTick.TickNumber-lastTick.TickNumber == 0 {
		// illegal state
		return fmt.Errorf("next tick equals last tick [%d]", nextTick.TickNumber)
	}

	// in case of skipped ticks we have a new processed tick interval. Typically on epoch change but can also happen
	// in epoch on restart within epoch.

	// start new tick interval (will be modified in next tick)
	err := dataStore.AppendProcessedTickInterval(ctx, nextTick.Epoch, &protobuf.ProcessedTickInterval{
		InitialProcessedTick: nextTick.TickNumber,
		LastProcessedTick:    nextTick.TickNumber,
	})
	if err != nil {
		return fmt.Errorf("appending tick interval data: %w", err)
	}

	return nil
}
