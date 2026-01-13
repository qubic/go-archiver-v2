package computors

import (
	"context"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"log"

	"github.com/qubic/go-archiver-v2/db"
	"github.com/qubic/go-archiver-v2/network"
)

// Get computors list
func Get(ctx context.Context, store *db.PebbleStore, client network.QubicClient, tickNumber, initialTick uint32, epoch uint16, computorSignaturePackage uint64) ([]*Computors, error) {
	compsList, err := load(ctx, store, uint32(epoch))
	if err != nil {
		return nil, fmt.Errorf("loading computors from data store: %w", err)
	}

	//Start of epoch
	if compsList == nil || len(compsList) == 0 {
		computors, err := getFromNode(ctx, client, initialTick)
		if err != nil {
			return nil, fmt.Errorf("getting computors from node: %w", err)
		}
		if binary.LittleEndian.Uint64(computors.Signature[:8]) != computorSignaturePackage {
			log.Printf("[WARN] Initial computors signature mismatch")
			return nil, fmt.Errorf("initial computor list signature does not match expected system info signature")
		}
		log.Printf("[INFO] Initial computors list. Signature: [%s]", hex.EncodeToString(computors.Signature[:]))
		return []*Computors{computors}, nil
	}

	lastComputorList := compsList[len(compsList)-1]
	lastComputorSignaturePackage := binary.LittleEndian.Uint64(lastComputorList.Signature[:8])
	if computorSignaturePackage != lastComputorSignaturePackage {
		computors, err := getFromNode(ctx, client, tickNumber)
		if err != nil {
			return nil, fmt.Errorf("getting computors from node: %w", err)
		}
		log.Printf("[INFO] Computors list changed in tick [%d]. Signature: [%s]", tickNumber, hex.EncodeToString(computors.Signature[:]))
		compsList = append(compsList, computors)

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
