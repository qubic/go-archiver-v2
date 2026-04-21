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
)

type Validator interface {
	Validate(ctx context.Context, store *db.PebbleStore, fetcher network.DataFetcher, epoch uint16, tickNumber uint32) error
}

type TickStatus struct {
	ProcessingEpoch uint16
	ProcessedTick   uint32
	LiveEpoch       uint16
	LiveTick        uint32
}

type Processor struct {
	fetcherFactory       func() (network.DataFetcher, error)
	databasePool         *db.DatabasePool
	tickValidator        Validator
	arbitratorPubKey     [32]byte
	processTickTimeout   time.Duration
	tickStatus           *TickStatus
	startFromCurrentTick bool
	metrics              *metrics.ProcessingMetrics
}

type Config struct {
	ProcessTickTimeout time.Duration
}

func NewProcessor(fetcherFactory func() (network.DataFetcher, error), dbPool *db.DatabasePool, tickValidator Validator, config Config, metrics *metrics.ProcessingMetrics) *Processor {
	return &Processor{
		fetcherFactory:     fetcherFactory,
		databasePool:       dbPool,
		processTickTimeout: config.ProcessTickTimeout,
		tickValidator:      tickValidator,
		tickStatus:         &TickStatus{},
		metrics:            metrics,
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

func (p *Processor) processOneByOne() error {
	start := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), p.processTickTimeout)
	defer cancel()

	var err error
	fetcher, err := p.fetcherFactory()
	if err != nil {
		return fmt.Errorf("creating data fetcher: %w", err)
	}
	defer func() {
		fetcher.Release(err)
	}()

	tickStatus, err := fetcher.GetTickStatus(ctx)
	if err != nil {
		return fmt.Errorf("getting tick status: %w", err)
	}
	p.tickStatus.LiveTick = tickStatus.Tick
	p.tickStatus.LiveEpoch = tickStatus.Epoch

	dataStore, err := p.databasePool.GetOrCreateDbForEpoch(tickStatus.Epoch)
	if err != nil {
		return fmt.Errorf("getting database: %w", err)
	}

	lastProcessedTick, err := p.getLastProcessedTick(ctx, dataStore, tickStatus.Epoch)
	if err != nil {
		return fmt.Errorf("getting last processed tick: %w", err)
	}
	p.tickStatus.ProcessedTick = lastProcessedTick.TickNumber
	p.tickStatus.ProcessingEpoch = uint16(lastProcessedTick.Epoch)

	nextTick, err := p.getNextProcessingTick(ctx, lastProcessedTick, tickStatus)
	if err != nil {
		return fmt.Errorf("getting next tick to process: %w", err)
	}
	log.Printf("Next tick to process: [%d]. Current tick: [%d]. Delta [%d]", nextTick.TickNumber, tickStatus.Tick, int64(tickStatus.Tick)-int64(nextTick.TickNumber))

	if nextTick.TickNumber > tickStatus.Tick {
		return fmt.Errorf("next tick is in the future. processed: %d, next %d, available %d",
			lastProcessedTick.TickNumber, nextTick.TickNumber, tickStatus.Tick)
	}

	// not sure if this helps because we will often be aligned at time of processing
	if nextTick.TickNumber == tickStatus.Tick && tickStatus.NumberOfAlignedVotes < 451 {
		return fmt.Errorf("tick not ready ([%d] aligned votes)", tickStatus.NumberOfAlignedVotes)
	}

	err = p.tickValidator.Validate(ctx, dataStore, fetcher, tickStatus.Epoch, nextTick.TickNumber)
	if err != nil {
		return fmt.Errorf("validating tick %d: %w", nextTick.TickNumber, err)
	}

	if lastProcessedTick.TickNumber >= tickStatus.InitialTick { // no skipped ticks before initial tick
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

func (p *Processor) getNextProcessingTick(_ context.Context, lastTick *protobuf.ProcessedTick, currentTickStatus network.TickStatus) (*protobuf.ProcessedTick, error) {
	// handles the case where the initial tick of epoch returned by the node is greater than the last processed tick
	// which means that we are in the next epoch, and we should start from the initial tick of the current epoch
	if currentTickStatus.InitialTick > lastTick.TickNumber {
		return &protobuf.ProcessedTick{TickNumber: currentTickStatus.InitialTick, Epoch: uint32(currentTickStatus.Epoch)}, nil
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
