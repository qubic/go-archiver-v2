package processor

import (
	"context"
	"errors"
	"fmt"
	"github.com/qubic/go-archiver/db"
	"github.com/qubic/go-archiver/network"
	"github.com/qubic/go-archiver/protobuf"
	"github.com/qubic/go-node-connector/types"
	"log"
	"time"
)

type Processor struct {
	clientPool         network.QubicClientPool
	databasePool       *db.DatabasePool
	arbitratorPubKey   [32]byte
	processTickTimeout time.Duration
	enableStatusAddon  bool
}

func NewProcessor(clientPool network.QubicClientPool, dbPool *db.DatabasePool, processTickTimeout time.Duration, arbitratorPubKey [32]byte, enableStatusAddon bool) *Processor {
	return &Processor{
		clientPool:         clientPool,
		databasePool:       dbPool,
		processTickTimeout: processTickTimeout,
		arbitratorPubKey:   arbitratorPubKey,
		enableStatusAddon:  enableStatusAddon,
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

func (p *Processor) processOneByOne() error {
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

	lastProcessedTick, err := p.getLastProcessedTick(ctx, tickInfo.Epoch)
	if err != nil {
		return fmt.Errorf("getting last processed tick: %w", err)
	}

	nextTick, err := p.getNextProcessingTick(ctx, lastProcessedTick, tickInfo)
	if err != nil {
		return fmt.Errorf("getting next tick to process: %w", err)
	}

	if nextTick.TickNumber > tickInfo.Tick {
		return fmt.Errorf("next tick is in the future. processed: [%d], next [%d], available [%d]",
			lastProcessedTick.TickNumber, nextTick.TickNumber, tickInfo.Tick)
	}

	log.Printf("Processing tick [%d].", nextTick.TickNumber)

	// FIXME
	//val := validator.New(client, p.ps, p.arbitratorPubKey)
	//err = val.ValidateTick(ctx, tickInfo.InitialTick, nextTick.TickNumber, !p.enableStatusAddon)
	//if err != nil {
	//	return fmt.Errorf(err, "validating tick %d", nextTick.TickNumber)
	//}

	err = p.handleSkippedTicks(ctx, lastProcessedTick, nextTick)
	if err != nil {
		return fmt.Errorf("handling skipped ticks: %w", err)
	}

	err = p.storeProcessedTick(ctx, nextTick)
	if err != nil {
		return fmt.Errorf("storing processed tick: %w", err)
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
		cErr := p.clientPool.Close(client)
		if cErr != nil {
			log.Printf("[ERROR] closing connection: %s", cErr.Error())
		}
	}
}

func (p *Processor) getLastProcessedTick(ctx context.Context, epoch uint16) (*protobuf.ProcessedTick, error) {
	dataStore, err := p.databasePool.GetOrCreateDbForEpoch(epoch)
	if err != nil {
		return nil, fmt.Errorf("getting data store for epoch %d: %w", epoch, err)
	}

	lastTick, err := dataStore.GetLastProcessedTick(ctx)
	if err != nil {
		// handles first run of the archiver where there is nothing in storage or epoch change
		// in this case last tick is 0 and epoch is current tick info epoch
		if errors.Is(err, db.ErrNotFound) {
			return &protobuf.ProcessedTick{TickNumber: 0, Epoch: uint32(epoch)}, nil
		}
		return nil, fmt.Errorf("getting last processed tick: %w", err)
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

func (p *Processor) handleSkippedTicks(ctx context.Context, lastTick *protobuf.ProcessedTick, nextTick *protobuf.ProcessedTick) error {

	if nextTick.TickNumber-lastTick.TickNumber == 1 {
		// no skipped ticks. default case.
		return nil
	}

	if nextTick.TickNumber-lastTick.TickNumber == 0 {
		// illegal state
		return fmt.Errorf("next tick equals last tick [%d]", nextTick.TickNumber)
	}

	// in case of skipped ticks we have a new processed tick interval. Typically on epoch change but can also happen
	// in epoch on restart within epoch.

	// use next tick epoch in case of epoch change
	dataStore, err := p.databasePool.GetOrCreateDbForEpoch(uint16(nextTick.Epoch))
	if err != nil {
		return fmt.Errorf("appending tick interval: %w", err)
	}

	// start new tick interval (will be modified in next tick)
	err = dataStore.AppendProcessedTickInterval(ctx, nextTick.Epoch, &protobuf.ProcessedTickInterval{
		InitialProcessedTick: nextTick.TickNumber,
		LastProcessedTick:    nextTick.TickNumber,
	})
	if err != nil {
		return fmt.Errorf("appending tick interval data: %w", err)
	}

	// TODO we could remove writing to the old data store and not store the last tick interval
	if nextTick.Epoch > lastTick.Epoch {
		dataStore, err = p.databasePool.GetOrCreateDbForEpoch(uint16(lastTick.Epoch))
		if err != nil {
			return fmt.Errorf("appending skipped ticks: %w", err)
		}
	}
	err = dataStore.SetSkippedTicksInterval(ctx, &protobuf.SkippedTicksInterval{
		StartTick: lastTick.TickNumber + 1,
		EndTick:   nextTick.TickNumber - 1,
	})
	if err != nil {
		return fmt.Errorf("appending skipped ticks data: %w", err)
	}

	return nil
}

func (p *Processor) storeProcessedTick(ctx context.Context, tick *protobuf.ProcessedTick) error {
	dataStore, err := p.databasePool.GetOrCreateDbForEpoch(uint16(tick.Epoch))
	if err != nil {
		return fmt.Errorf("getting data store: %w", err)
	}

	err = dataStore.SetLastProcessedTick(ctx, tick)
	if err != nil {
		return fmt.Errorf("setting last processed tick [%d]: %w", tick.TickNumber, err)
	}

	return nil
}
