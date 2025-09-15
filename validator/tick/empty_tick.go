package tick

import (
	"context"
	"fmt"
	"github.com/cockroachdb/pebble"
	"github.com/pkg/errors"
	"github.com/qubic/go-archiver/db"
	"github.com/qubic/go-archiver/protobuf"
	"github.com/qubic/go-node-connector/types"
	"google.golang.org/protobuf/proto"
	"time"
)

var emptyTickData = &protobuf.TickData{}

func CalculateEmptyTicksForEpoch(ctx context.Context, ps *db.PebbleStore, epoch uint32) ([]uint32, error) {

	epochs, err := ps.GetProcessedTickIntervals(ctx)
	if err != nil {
		return nil, fmt.Errorf("getting processed tick intervals: %w", err)
	}

	for _, e := range epochs {
		if e.Epoch != epoch {
			continue
		}

		var emptyTicks []uint32

		for _, interval := range e.Intervals {
			fmt.Printf("Interval: %d -> %d\n", interval.InitialProcessedTick, interval.LastProcessedTick)
			for tickOffset := range interval.LastProcessedTick - interval.InitialProcessedTick + 1 {
				tickNumber := tickOffset + interval.InitialProcessedTick

				tickData, err := ps.GetTickData(ctx, tickNumber)
				if err != nil {
					return nil, fmt.Errorf("getting tick data for tick %d: %w", tickNumber, err)
				}

				if CheckIfTickIsEmptyProto(tickData) {
					fmt.Printf("Found empty tick: %d\n", tickNumber)
					emptyTicks = append(emptyTicks, tickNumber)
					continue
				}
			}
		}
		return emptyTicks, err
	}
	return make([]uint32, 0), nil
}

func CheckIfTickIsEmptyProto(tickData *protobuf.TickData) bool {

	if tickData == nil || proto.Equal(tickData, emptyTickData) {
		return true
	}

	return false
}

func CheckIfTickIsEmpty(tickData types.TickData) (bool, error) {
	data, err := qubicToProto(tickData)
	if err != nil {
		return false, fmt.Errorf("converting tick data to protobuf: %w", err)
	}

	return CheckIfTickIsEmptyProto(data), nil
}

func CalculateEmptyTicksForAllEpochs(ps *db.PebbleStore) error {

	// FIXME this will not work anymore with new db format

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	epochs, err := ps.GetLastProcessedTicksPerEpoch(ctx)
	if err != nil {
		return fmt.Errorf("loading last processed ticks per epoch: %w", err)
	}

	for epoch, _ := range epochs {

		_, err := ps.GetEmptyTicksForEpoch(epoch)
		if err == nil {
			return nil // We have the empty ticks
		}
		if !errors.Is(err, pebble.ErrNotFound) {
			return fmt.Errorf("loading empty ticks for epoch %d: %w", epoch, err)
		}

		fmt.Printf("Calculating empty ticks for epoch %d\n", epoch)
		emptyTicksPerEpoch, err := CalculateEmptyTicksForEpoch(ctx, ps, epoch)
		if err != nil {
			return fmt.Errorf("calculating empty ticks for epoch %d: %w", epoch, err)
		}

		err = ps.SetEmptyTicksForEpoch(epoch, uint32(len(emptyTicksPerEpoch)))
		if err != nil {
			return fmt.Errorf("saving empty tick count for epoch %d: %w", epoch, err)
		}
		err = ps.SetEmptyTickListPerEpoch(epoch, emptyTicksPerEpoch)
		if err != nil {
			return fmt.Errorf("saving empty tick list for epoch %d: %w", epoch, err)
		}

	}
	return nil
}

func ResetEmptyTicksForAllEpochs(ps *db.PebbleStore) error {

	// FIXME this will not work anymore with new db format

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	epochs, err := ps.GetLastProcessedTicksPerEpoch(ctx)
	if err != nil {
		return fmt.Errorf("loading last processed ticks per epoch: %w", err)
	}

	for epoch, _ := range epochs {
		fmt.Printf("Reseting empty ticks for epoch: %d\n", epoch)
		err := ps.DeleteEmptyTicksKeyForEpoch(epoch)
		if err != nil {
			return fmt.Errorf("deleting empty ticks key for epoch %d: %w", epoch, err)
		}
		err = ps.DeleteEmptyTickListKeyForEpoch(epoch)
		if err != nil {
			return fmt.Errorf("deleting empty tick list key for epoch %d: %w", epoch, err)
		}
	}

	return nil
}
