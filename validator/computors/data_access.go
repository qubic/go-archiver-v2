package computors

import (
	"context"
	"errors"
	"fmt"
	"github.com/qubic/go-archiver/db"
	qubic "github.com/qubic/go-node-connector"
	"github.com/qubic/go-node-connector/types"
)

// Get computors from store, otherwise get it from a node
func Get(ctx context.Context, store *db.PebbleStore, client *qubic.Client, epoch uint16) (types.Computors, error) {
	comps, err := load(ctx, store, uint32(epoch))
	if err != nil {
		if !errors.Is(err, db.ErrNotFound) {
			return types.Computors{}, fmt.Errorf("loading computors from data store: %w", err)
		}
		comps, err = client.GetComputors(ctx)
		if err != nil {
			return types.Computors{}, fmt.Errorf("getting computors from node: %w", err)
		}
	}
	return comps, nil
}

func load(ctx context.Context, store *db.PebbleStore, epoch uint32) (types.Computors, error) {
	protoModel, err := store.GetComputors(ctx, epoch)
	if err != nil {
		return types.Computors{}, fmt.Errorf("loading computors: %w", err)
	}

	model, err := protoToQubic(protoModel)
	if err != nil {
		return types.Computors{}, fmt.Errorf("converting proto to computors: %w", err)
	}

	return model, nil
}

// Save computors to store
func Save(ctx context.Context, store *db.PebbleStore, epoch uint16, computors types.Computors) error {
	protoModel, err := qubicToProto(computors)
	if err != nil {
		return fmt.Errorf("converting computors to proto: %w", err)
	}

	err = store.SetComputors(ctx, uint32(epoch), protoModel)
	if err != nil {
		return fmt.Errorf("storing computors: %w", err)
	}

	return nil
}
