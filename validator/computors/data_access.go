package computors

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"log"

	"github.com/qubic/go-archiver-v2/db"
	"github.com/qubic/go-archiver-v2/network"
)

// Get computors list
func Get(ctx context.Context, store *db.PebbleStore, client network.QubicClient, tickNumber uint32, computorListSignatureChanged bool, initialTick uint32, epoch uint16) ([]*Computors, error) {
	computorsList, err := load(ctx, store, uint32(epoch))
	if err != nil {
		return nil, fmt.Errorf("loading computors from data store: %w", err)
	}
	// Handles start of the epoch
	if computorsList == nil || len(computorsList) == 0 {
		computors, err := getFromNode(ctx, client, initialTick)
		if err != nil {
			return nil, fmt.Errorf("getting computors from node: %w", err)
		}
		log.Printf("[INFO] Initial computors list. Signature: [%s]", hex.EncodeToString(computors.Signature[:]))
		return []*Computors{computors}, nil
	}

	if computorListSignatureChanged {
		computors, err := getFromNode(ctx, client, tickNumber)
		if err != nil {
			return nil, fmt.Errorf("getting computors from node: %w", err)
		}
		log.Printf("[INFO] Computors list changed in tick [%d]. Signature: [%s]", tickNumber, hex.EncodeToString(computors.Signature[:]))
		computorsList = append(computorsList, computors)
	}

	return computorsList, nil
}

func load(ctx context.Context, store *db.PebbleStore, epoch uint32) ([]*Computors, error) {
	protoModel, err := store.GetComputors(ctx, epoch)
	if err != nil && !errors.Is(err, db.ErrNotFound) {
		return nil, fmt.Errorf("loading computors: %w", err)
	}
	if errors.Is(err, db.ErrNotFound) {
		return nil, nil
	}
	model, err := protoToQubic(protoModel)
	if err != nil {
		return nil, fmt.Errorf("converting proto to computors: %w", err)
	}
	return model, nil
}

func getFromNode(ctx context.Context, client network.QubicClient, tickNumber uint32) (*Computors, error) {
	comps, err := client.GetComputors(ctx)
	if err != nil {
		return nil, fmt.Errorf("get computors call: %w", err)
	}
	return &Computors{
		Epoch:      comps.Epoch,
		TickNumber: tickNumber,
		PubKeys:    comps.PubKeys,
		Signature:  comps.Signature,
	}, nil
}

// Save computors to store
func Save(ctx context.Context, store *db.PebbleStore, epoch uint16, computors []*Computors) error {
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
