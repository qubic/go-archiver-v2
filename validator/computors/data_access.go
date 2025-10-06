package computors

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"log"
	"math/rand/v2"

	"github.com/qubic/go-archiver-v2/db"
	"github.com/qubic/go-archiver-v2/network"
)

// Get computors list
func Get(ctx context.Context, store *db.PebbleStore, client network.QubicClient, tickNumber uint32, epoch uint16) ([]*Computors, error) {
	compsList, err := load(ctx, store, uint32(epoch))
	if err != nil {
		return nil, fmt.Errorf("loading computors from data store: %w", err)
	}

	if len(compsList) == 0 {
		comps, err := getFromNode(ctx, client, 0) // set not tick number for first entry (valid with initial tick of epoch)
		if err != nil {
			return nil, fmt.Errorf("getting computors from node: %w", err)
		}
		log.Printf("[INFO] New computors list. Signature: [%s]", hex.EncodeToString(comps.Signature[:]))
		return []*Computors{comps}, nil
	} else if rand.IntN(100) < 10 { // don't update computors every time. Call costs some time.
		log.Printf("[INFO] checking for new computors...")
		comps, err := getFromNode(ctx, client, tickNumber)
		if err != nil {
			return nil, fmt.Errorf("getting computors from node: %w", err)
		}
		if compsList[len(compsList)-1].Signature != comps.Signature {
			log.Printf("[INFO] Computors list changed in tick [%d]. Signature: [%s]", tickNumber, hex.EncodeToString(comps.Signature[:]))
			compsList = append(compsList, comps)
		}
	}

	return compsList, nil
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
