package validator

import (
	"context"
	"errors"
	"fmt"
	"github.com/qubic/go-archiver/db"
	"github.com/qubic/go-archiver/network"
	"github.com/qubic/go-archiver/protobuf"
	"github.com/qubic/go-archiver/validator/computors"
	"github.com/qubic/go-archiver/validator/quorum"
	"github.com/qubic/go-archiver/validator/tick"
	"github.com/qubic/go-archiver/validator/tx"
	"github.com/qubic/go-archiver/validator/txstatus"
	"github.com/qubic/go-node-connector/types"
	"log"
)

type Validator struct {
	arbitratorPubKey   [32]byte
	statusAddonEnabled bool
}

func NewValidator(arbitratorPubKey [32]byte, enableStatusAddon bool) *Validator {
	return &Validator{
		arbitratorPubKey:   arbitratorPubKey,
		statusAddonEnabled: enableStatusAddon,
	}
}

func (v *Validator) Validate(ctx context.Context, store *db.PebbleStore, client network.QubicClient, tickNumber uint32) error {

	// verify quorum is reached

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
	log.Printf("Quorum valid. Aligned %d. Misaligned %d.", len(alignedVotes), len(quorumVotes)-len(alignedVotes))

	// validate tick data and transactions

	isEmpty := isEmptyTick(alignedVotes)

	tickData, err := v.validateTickData(ctx, client, comps, alignedVotes, tickNumber, isEmpty)
	if err != nil {
		return err
	}

	validTxs, txStatus, err := v.validateTransactions(ctx, client, tickData, tickNumber, isEmpty)
	if err != nil {
		return err
	}

	// store data

	err = quorum.Store(ctx, store, tickNumber, alignedVotes)
	if err != nil {
		return fmt.Errorf("storing aligned quorum votes: %w", err)
	}

	err = tick.Store(ctx, store, tickNumber, tickData)
	if err != nil {
		return fmt.Errorf("storing tick data: %w", err)
	}

	err = tx.Store(ctx, store, tickNumber, validTxs)
	if err != nil {
		return fmt.Errorf("storing transactions: %w", err)
	}

	err = txstatus.Store(ctx, store, tickNumber, txStatus)
	if err != nil {
		return fmt.Errorf("storing transactions status: %w", err)
	}

	log.Println("Stored tick data and transactions.")

	return nil
}

func (v *Validator) validateComputors(ctx context.Context, store *db.PebbleStore, client network.QubicClient, epoch uint16) (types.Computors, error) {

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

func (v *Validator) validateTickData(ctx context.Context, client network.QubicClient, comps types.Computors, quorumVotes types.QuorumVotes, tickNumber uint32, isEmptyTick bool) (types.TickData, error) {

	if isEmptyTick {

		log.Println("Empty quorum digest. Empty tick.")
		return types.TickData{}, nil

	} else {

		tickData, err := client.GetTickData(ctx, tickNumber)
		if err != nil {
			return types.TickData{}, fmt.Errorf("getting tick data: %w", err)
		}

		err = tick.Validate(ctx, tickData, quorumVotes[0], comps)
		if err != nil {
			return types.TickData{}, fmt.Errorf("validating tick data: %w", err)
		}

		return tickData, nil
	}
}

func (v *Validator) validateTransactions(ctx context.Context, client network.QubicClient, tickData types.TickData, tickNumber uint32, isEmptyTick bool) ([]types.Transaction, *protobuf.TickTransactionsStatus, error) {

	if isEmptyTick {

		return make([]types.Transaction, 0), // no valid transactions
			&protobuf.TickTransactionsStatus{}, // no approved transactions
			nil // no error

	} else {

		// get's all non-zero transactions
		transactions, err := client.GetTickTransactions(ctx, tickNumber)
		if err != nil {
			return nil, nil, fmt.Errorf("getting tick transactions: %w", err)
		}

		// keeps all transactions that are in the tick data digests
		validTxs, err := tx.Validate(ctx, transactions, tickData)
		if err != nil {
			return nil, nil, fmt.Errorf("validating transactions: %w", err)
		}

		if len(validTxs) == len(transactions) {
			log.Printf("All [%d] transactions are valid.", len(validTxs))
		} else {
			log.Printf("[%d] out of [%d] transactions are valid.", len(validTxs), len(transactions))
		}

		// get tx status information, if tx status addon is enabled
		tickTxStatus, err := getTxStatus(ctx, client, len(validTxs), tickNumber, v.statusAddonEnabled)
		if err != nil {
			return nil, nil, fmt.Errorf("getting tx status: %w", err)
		}

		// combine valid transactions with money flew status
		transactionsWithTxStatus, err := txstatus.Validate(ctx, tickTxStatus, validTxs)
		if err != nil {
			return nil, nil, fmt.Errorf("validating tx status: %w", err)
		}

		return validTxs, transactionsWithTxStatus, nil
	}

}

func getTxStatus(ctx context.Context, client network.QubicClient, transactionsCount int, tickNumber uint32, enabled bool) (types.TransactionStatus, error) {
	if enabled {
		return client.GetTxStatus(ctx, tickNumber)
	} else {
		// empty transaction status
		return types.TransactionStatus{
			CurrentTickOfNode:  tickNumber,
			Tick:               tickNumber,
			TxCount:            uint32(transactionsCount),
			MoneyFlew:          [128]byte{},
			TransactionDigests: nil,
		}, nil
	}
}

func isEmptyTick(quorumVotes types.QuorumVotes) bool {
	return quorumVotes[0].TxDigest == [32]byte{}
}
