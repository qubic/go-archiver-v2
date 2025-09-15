package validator

import (
	"context"
	"errors"
	"fmt"
	"github.com/qubic/go-archiver/db"
	"github.com/qubic/go-archiver/validator/computors"
	"github.com/qubic/go-archiver/validator/quorum"
	qubic "github.com/qubic/go-node-connector"
	"github.com/qubic/go-node-connector/types"
	"log"
)

type Validator struct {
	arbitratorPubKey  [32]byte
	enableStatusAddon bool
}

func NewValidator(arbitratorPubKey [32]byte, enableStatusAddon bool) *Validator {
	return &Validator{
		arbitratorPubKey: arbitratorPubKey,
	}
}

func (v *Validator) ValidateTick(ctx context.Context, store *db.PebbleStore, client *qubic.Client, tickNumber uint32) error {
	quorumVotes, err := client.GetQuorumVotes(ctx, tickNumber)
	if err != nil {
		return fmt.Errorf("getting quorum votes: %w", err)
	}
	if len(quorumVotes) < 0 {
		return errors.New("no quorum votes fetched")
	}
	epoch := quorumVotes[0].Epoch

	comps, err := v.validateComputors(ctx, store, client, epoch)
	if err != nil {
		return fmt.Errorf("validating computors: %w", err)
	}

	alignedVotes, err := quorum.Validate(ctx, store, client, quorumVotes, comps, epoch)
	if err != nil {
		return fmt.Errorf("validating quorum votes: %w", err)
	}
	log.Printf("Quorum validated. Aligned %d. Misaligned %d.\n", len(alignedVotes), len(quorumVotes)-len(alignedVotes))

	/*

		// TODO move this out
		var tickData types.TickData
		var validTxs = make([]types.Transaction, 0)
		approvedTxs := &protobuf.TickTransactionsStatus{}

		// FIXME
		if quorumVotes[0].TxDigest != [32]byte{} {
			td, err := client.GetTickData(ctx, tickNumber)
			if err != nil {
				return fmt.Errorf("getting tick data: %w", err)
			}

			tickData = td
			log.Println("Got tick data")

			// FIXME
			//err = tick.Validate(ctx, GoSchnorrqVerify, tickData, alignedVotes[0], comps)
			//if err != nil {
			//	return fmt.Errorf("validating tick data: %w", err)
			//}

			log.Println("Tick data validated")

			transactions, err := client.GetTickTransactions(ctx, tickNumber)
			if err != nil {
				return fmt.Errorf("getting tick transactions: %w", err)
			}

			log.Printf("Validating %d transactions\n", len(transactions))

			// FIXME
			//validTxs, err = tx.Validate(ctx, GoSchnorrqVerify, transactions, tickData)
			//if err != nil {
			//	return fmt.Errorf("validating transactions: %w", err)
			//}

			log.Printf("Validated %d transactions\n", len(validTxs))

			var tickTxStatus types.TransactionStatus
			if v.enableStatusAddon {
				tickTxStatus, err = client.GetTxStatus(ctx, tickNumber)
				if err != nil {
					return fmt.Errorf("getting tx status: %w", err)
				}
			} else {
				tickTxStatus = types.TransactionStatus{
					CurrentTickOfNode:  tickNumber,
					Tick:               tickNumber,
					TxCount:            uint32(len(validTxs)),
					MoneyFlew:          [128]byte{},
					TransactionDigests: nil,
				}
			}

			// FIXME
			log.Printf("tx status: %v\n", tickTxStatus) // TODO remove me
			//approvedTxs, err = txstatus.Validate(ctx, tickTxStatus, validTxs)
			//if err != nil {
			//	return fmt.Errorf("validating tx status: %w", err)
			//}
		}

	*/

	// proceed to storing tick information
	err = quorum.Store(ctx, store, tickNumber, alignedVotes)
	if err != nil {
		return fmt.Errorf("storing aligned votes: %w", err)
	}

	return nil
}

func (v *Validator) validateComputors(ctx context.Context, store *db.PebbleStore, client *qubic.Client, epoch uint16) (types.Computors, error) {

	comps, err := computors.Get(ctx, store, client, epoch)
	if err != nil {
		return types.Computors{}, fmt.Errorf("getting computors: %w", err)
	}

	err = computors.Validate(ctx, comps, v.arbitratorPubKey)
	if err != nil {
		return types.Computors{}, fmt.Errorf("validating computors: %w", err)
	}

	err = computors.Save(ctx, store, epoch, comps)
	if err != nil {
		return types.Computors{}, fmt.Errorf("saving computors: %w", err)
	}

	return comps, nil
}
