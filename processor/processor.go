package processor

import (
	"context"
	"errors"
	"fmt"
	"github.com/qubic/go-archiver-v2/db"
	"github.com/qubic/go-archiver-v2/network"
	"github.com/qubic/go-archiver-v2/protobuf"
	"github.com/qubic/go-node-connector/types"
	"log"
	"time"
)

type Validator interface {
	Validate(ctx context.Context, store *db.PebbleStore, client network.QubicClient, epoch uint16, tickNumber uint32) error
}

type TickStatus struct {
	ProcessingEpoch uint16
	ProcessedTick   uint32
	LiveEpoch       uint16
	LiveTick        uint32
}

type Processor struct {
	clientPool         network.QubicClientPool
	databasePool       *db.DatabasePool
	tickValidator      Validator
	arbitratorPubKey   [32]byte
	processTickTimeout time.Duration
	enableStatusAddon  bool
	tickStatus         *TickStatus
}

func NewProcessor(clientPool network.QubicClientPool, dbPool *db.DatabasePool, tickValidator Validator, processTickTimeout time.Duration) *Processor {
	return &Processor{
		clientPool:         clientPool,
		databasePool:       dbPool,
		processTickTimeout: processTickTimeout,
		tickValidator:      tickValidator,
		tickStatus:         &TickStatus{},
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
	client, err := p.clientPool.Get()
	if err != nil {
		return fmt.Errorf("getting client connection: %w", err)
	}
	defer p.releaseClient(err, client)

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
	p.tickStatus.ProcessedTick = tickInfo.Tick
	p.tickStatus.ProcessingEpoch = tickInfo.Epoch

	nextTick, err := p.getNextProcessingTick(ctx, lastProcessedTick, tickInfo)
	if err != nil {
		return fmt.Errorf("getting next tick to process: %w", err)
	}
	log.Printf("Next tick to process: [%d].", nextTick.TickNumber)

	if nextTick.TickNumber > tickInfo.Tick {
		return fmt.Errorf("next tick is in the future. processed: %d, next %d, available %d",
			lastProcessedTick.TickNumber, nextTick.TickNumber, tickInfo.Tick)
	}

	err = p.tickValidator.Validate(ctx, dataStore, client, tickInfo.Epoch, nextTick.TickNumber)
	if err != nil {
		return fmt.Errorf("validating tick %d: %w", nextTick.TickNumber, err)
	}

	if lastProcessedTick.TickNumber >= tickInfo.InitialTick { // no skipped ticks before initial tick
		err = p.handleTickIntervals(ctx, dataStore, lastProcessedTick, nextTick)
		if err != nil {
			return fmt.Errorf("handling skipped ticks: %w", err)
		}
	}

	err = p.storeProcessedTick(ctx, dataStore, nextTick)
	if err != nil {
		return fmt.Errorf("storing processed tick: %w", err)
	}

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

func (p *Processor) storeProcessedTick(ctx context.Context, dataStore *db.PebbleStore, tick *protobuf.ProcessedTick) error {
	err := dataStore.SetLastProcessedTick(ctx, tick)
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
