package validator

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log"

	"github.com/qubic/go-archiver-v2/db"
	"github.com/qubic/go-archiver-v2/network"
	"github.com/qubic/go-archiver-v2/protobuf"
	"github.com/qubic/go-archiver-v2/validator/computors"
	"github.com/qubic/go-archiver-v2/validator/quorum"
	"github.com/qubic/go-archiver-v2/validator/tick"
	"github.com/qubic/go-archiver-v2/validator/tx"
	"github.com/qubic/go-archiver-v2/validator/txstatus"
	"github.com/qubic/go-node-connector/types"
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

func (v *Validator) Validate(ctx context.Context, store *db.PebbleStore, client network.QubicClient, epoch uint16, tickNumber uint32) error {
	systemInfo, err := client.GetSystemInfo(ctx)
	if err != nil {
		return fmt.Errorf("getting system info: %w", err)
	}

	// validate quorum
	quorumVotes, err := client.GetQuorumVotes(ctx, tickNumber) // this takes long
	if err != nil {
		return fmt.Errorf("getting quorum votes: %w", err)
	}
	if len(quorumVotes) <= 0 {
		return errors.New("no quorum votes fetched")
	}
	if len(quorumVotes) < 451 {
		return fmt.Errorf("not enough quorum votes yet: [%d]", len(quorumVotes))
	}

	// takes long if node is called. otherwise fast.
	comps, err := v.validateComputors(ctx, store, client, tickNumber, systemInfo, epoch)
	if err != nil {
		return fmt.Errorf("validating computors: %w", err)
	}

	alignedVotes, err := quorum.Validate(ctx, store, quorumVotes, comps, systemInfo, epoch) // fast
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

	return nil
}

func (v *Validator) validateComputors(ctx context.Context, store *db.PebbleStore, client network.QubicClient, tickNumber uint32, systemInfo types.SystemInfo, epoch uint16) (computors.Computors, error) {

	storedComputorPacketSignature, err := store.GetComputorPacketSignature(uint32(epoch))
	if err != nil {
		if !errors.Is(err, db.ErrNotFound) {
			return computors.Computors{}, fmt.Errorf("getting computor packet signature: %w", err)
		}
	}
	computorListSignatureChanged := storedComputorPacketSignature != systemInfo.ComputorPacketSignature

	comps, err := computors.Get(ctx, store, client, tickNumber, computorListSignatureChanged, systemInfo.InitialTick, epoch)
	if err != nil {
		return computors.Computors{}, fmt.Errorf("getting computors: %w", err)
	}
	if len(comps) == 0 {
		return computors.Computors{}, errors.New("no computors fetched")
	}

	latestComps := comps[len(comps)-1]
	if !latestComps.Validated || bytes.Compare(v.arbitratorPubKey[:], latestComps.Arbitrator[:]) != 0 {
		err = computors.Validate(ctx, *latestComps, v.arbitratorPubKey)
		if err != nil {
			return computors.Computors{}, fmt.Errorf("validating computors: %w", err)
		}
		latestComps.Validated = true
		latestComps.Arbitrator = v.arbitratorPubKey

		err = computors.Save(ctx, store, epoch, comps)
		if err != nil {
			return computors.Computors{}, fmt.Errorf("saving computors: %w", err)
		}
	}

	// The updated computor packet signature must be saved only after a new computor list has been saved
	if computorListSignatureChanged {
		err := store.SetComputorPacketSignature(uint32(epoch), systemInfo.ComputorPacketSignature)
		if err != nil {
			return computors.Computors{}, fmt.Errorf("saving computor packet signature: %w", err)
		}
	}

	return *latestComps, nil
}

func (v *Validator) validateTickData(ctx context.Context, client network.QubicClient, comps computors.Computors, quorumVotes types.QuorumVotes, tickNumber uint32, isEmptyTick bool) (types.TickData, error) {

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
