package processor

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/qubic/go-archiver-v2/db"
	"github.com/qubic/go-archiver-v2/metrics"
	"github.com/qubic/go-archiver-v2/network"
	"github.com/qubic/go-archiver-v2/protobuf"
	"github.com/qubic/go-archiver-v2/validator"
	"github.com/qubic/go-node-connector/v2/types"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
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
	tickStatus           *TickStatus
	startFromCurrentTick bool
	metrics              *metrics.ProcessingMetrics
	tracer               trace.Tracer
	stopTick             uint32
}

type Config struct {
	ProcessTickTimeout time.Duration
	StopTick           uint32
}

func NewProcessor(clientPool network.QubicClientPool, dbPool *db.DatabasePool, tickValidator Validator, config Config, metrics *metrics.ProcessingMetrics, tracer trace.Tracer) *Processor {
	return &Processor{
		clientPool:         clientPool,
		databasePool:       dbPool,
		processTickTimeout: config.ProcessTickTimeout,
		tickValidator:      tickValidator,
		tickStatus:         &TickStatus{},
		metrics:            metrics,
		tracer:             tracer,
		stopTick:           config.StopTick,
	}
}

func (p *Processor) Start() error {
	for {
		err := p.processOneByOne()
		if err != nil {
			log.Printf("Processing failed: %s", err.Error())
			time.Sleep(1 * time.Second)
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

	ctx, rootSpan := p.tracer.Start(ctx, "process_tick")
	defer func() {
		if err != nil {
			rootSpan.RecordError(err)
			rootSpan.SetStatus(codes.Error, err.Error())
		}
		rootSpan.End()
	}()

	// note: do not reassign ctx here, otherwise sibling phases would nest under pool_get
	_, poolSpan := p.tracer.Start(ctx, "pool_get")
	rawClient, err := p.clientPool.Get()
	if err != nil {
		poolSpan.End()
		return fmt.Errorf("getting 1st client connection: %w", err)
	}
	defer func() {
		p.releaseClient(err, rawClient)
	}()

	rawAltClient, err := p.clientPool.Get()
	if err != nil {
		poolSpan.End()
		return fmt.Errorf("getting 2nd client connection: %w", err)
	}
	defer func() {
		p.releaseClient(err, rawAltClient)
	}()
	poolSpan.End()

	// wrap raw clients with tracing decorators; raw clients are returned to the pool
	client := network.NewTracingQubicClient(rawClient, p.tracer)
	alternativeClient := network.NewTracingQubicClient(rawAltClient, p.tracer)

	tickInfo, err := client.GetTickInfo(ctx)
	if err != nil {
		return fmt.Errorf("getting tick info: %w", err)
	}

	if p.stopTick != 0 {
		log.Printf("Using stop tick: [%d] instead of live tick [%d].", p.stopTick, tickInfo.Tick)
		tickInfo.Tick = p.stopTick
		tickInfo.Epoch = 217
		tickInfo.InitialTick = 57700000
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

	clients := validator.Clients{Main: client, Alt: alternativeClient}
	err = p.tickValidator.Validate(ctx, dataStore, clients, tickInfo.Epoch, nextTick.TickNumber)
	if err != nil {
		return fmt.Errorf("validating tick %d: %w", nextTick.TickNumber, err)
	}

	if lastProcessedTick.TickNumber >= tickInfo.InitialTick { // no skipped ticks before initial tick
		err = p.handleTickIntervals(ctx, dataStore, lastProcessedTick, nextTick)
		if err != nil {
			return fmt.Errorf("handling skipped ticks: %w", err)
		}
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
