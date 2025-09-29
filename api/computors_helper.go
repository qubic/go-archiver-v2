package api

import (
	"cmp"
	"context"
	"fmt"
	"github.com/qubic/go-archiver-v2/db"
	"github.com/qubic/go-archiver-v2/protobuf"
	"slices"
)

func getComputors(ctx context.Context, database *db.PebbleStore, tick uint32, epoch uint16) (*protobuf.Computors, error) {
	computors, err := database.GetComputors(ctx, uint32(epoch))
	if err != nil {
		return nil, fmt.Errorf("error getting computors for epoch [%d]: %w", epoch, err)
	}
	if computors == nil || len(computors.Computors) == 0 {
		return nil, fmt.Errorf("no computors stored for epoch [%d]", epoch)
	}

	slices.SortFunc(computors.Computors, func(a, b *protobuf.Computors) int {
		return cmp.Compare(b.TickNumber, a.TickNumber) // switch for descending order
	})
	for _, computor := range computors.Computors {
		if computor.TickNumber <= tick {
			return computor, nil
		}
	}
	return computors.Computors[len(computors.Computors)-1], nil // could happen in first ticks of an epoch
}
