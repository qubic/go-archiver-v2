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
	"golang.org/x/sync/errgroup"
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

func (v *Validator) Validate(ctx context.Context, store *db.PebbleStore, fetcher network.DataFetcher, epoch uint16, tickNumber uint32) error {

	var quorumVotes types.QuorumVotes

	eg, egCtx := errgroup.WithContext(ctx)
	eg.Go(func() error {
		// validate quorum
		var err error
		quorumVotes, err = fetcher.GetQuorumVotes(egCtx, tickNumber)
		if err != nil {
			return fmt.Errorf("getting quorum votes: %w", err)
		}
		if len(quorumVotes) <= 0 {
			return errors.New("no quorum votes fetched")
		}
		if len(quorumVotes) < 451 {
			return fmt.Errorf("not enough quorum votes yet: [%d]", len(quorumVotes))
		}
		return nil
	})

	var systemMeta network.SystemMetadata
	eg.Go(func() error {
		var err error
		systemMeta, err = fetcher.GetSystemMetadata(egCtx)
		if err != nil {
			return fmt.Errorf("getting system metadata: %w", err)
		}
		return nil
	})

	err := eg.Wait()
	if err != nil {
		return fmt.Errorf("getting quorum votes and/or system metadata: %w", err)
	}

	// typically only one client call needed per epoch
	comps, err := v.validateComputors(ctx, store, fetcher, tickNumber, systemMeta.InitialTick, epoch, systemMeta.ComputorPacketSignature, systemMeta.ComputorSignatureAvailable)
	if err != nil {
		return fmt.Errorf("validating computors: %w", err)
	}

	skipScoreCheck := !systemMeta.VoteSignatureAvailable
	alignedVotes, err := quorum.Validate(ctx, quorumVotes, comps, systemMeta.TargetTickVoteSignature, skipScoreCheck) // fast
	if err != nil {
		return fmt.Errorf("validating quorum votes: %w", err)
	}
	log.Printf("Quorum valid. Aligned %d. Misaligned %d.", len(alignedVotes), len(quorumVotes)-len(alignedVotes))

	// validate tick data and transactions
	tickData, validTxs, txStatus, err := v.validateTickDataAndTransactions(ctx, alignedVotes, fetcher, comps, tickNumber)
	if err != nil {
		return fmt.Errorf("validating tick data and transactions: %w", err)
	}

	// store data

	err = quorum.Store(ctx, store, tickNumber, alignedVotes)
	if err != nil {
		return fmt.Errorf("storing aligned quorum votes: %w", err)
	}

	if systemMeta.VoteSignatureAvailable {
		err = quorum.StoreTargetTickVoteSignature(store, uint32(epoch), tickNumber, systemMeta.InitialTick, systemMeta.TargetTickVoteSignature)
		if err != nil {
			return fmt.Errorf("storing target tick signature: %w", err)
		}
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

func (v *Validator) validateTickDataAndTransactions(ctx context.Context, alignedVotes types.QuorumVotes, fetcher network.DataFetcher, comps computors.Computors, tickNumber uint32) (tickData types.TickData, validTxs []types.Transaction, txStatus *protobuf.TickTransactionsStatus, err error) {

	if isEmptyTick(alignedVotes) {
		return types.TickData{}, make([]types.Transaction, 0), &protobuf.TickTransactionsStatus{}, nil
	}

	eg, egCtx := errgroup.WithContext(ctx)

	eg.Go(func() error {
		var err error
		tickData, err = v.validateTickData(egCtx, fetcher, comps, alignedVotes, tickNumber)
		if err != nil {
			return fmt.Errorf("getting tick data: %w", err)
		}
		return nil
	})

	var transactions []types.Transaction
	eg.Go(func() error {
		var err error
		transactions, err = fetcher.GetTickTransactions(egCtx, tickNumber)
		if err != nil {
			return fmt.Errorf("getting transactions: %w", err)
		}
		return nil
	})

	err = eg.Wait()
	if err != nil {
		return tickData, nil, nil, fmt.Errorf("getting tick data and/or transactions: %w", err)
	}

	validTxs, txStatus, err = v.validateTransactions(ctx, fetcher, transactions, tickData, tickNumber)
	if err != nil {
		return tickData, nil, nil, fmt.Errorf("validating transactions: %w", err)
	}

	return tickData, validTxs, txStatus, nil

}

func (v *Validator) validateComputors(ctx context.Context, store *db.PebbleStore, fetcher network.DataFetcher, tickNumber, initialTick uint32, epoch uint16, computorPacketSignature uint64, computorSignatureAvailable bool) (computors.Computors, error) {

	comps, err := computors.Get(ctx, store, fetcher, tickNumber, initialTick, epoch, computorPacketSignature, computorSignatureAvailable)
	if err != nil {
		return computors.Computors{}, fmt.Errorf("getting computors: %w", err)
	}
	if len(comps) == 0 {
		return computors.Computors{}, errors.New("no computors fetched")
	}

	latestComps := comps[len(comps)-1]
	if !latestComps.Validated || bytes.Compare(v.arbitratorPubKey[:], latestComps.Arbitrator[:]) != 0 {
		if computorSignatureAvailable {
			err = computors.Validate(ctx, *latestComps, v.arbitratorPubKey)
			if err != nil {
				return computors.Computors{}, fmt.Errorf("validating computors: %w", err)
			}
		}
		latestComps.Validated = true
		latestComps.Arbitrator = v.arbitratorPubKey

		err = computors.Save(ctx, store, epoch, comps)
		if err != nil {
			return computors.Computors{}, fmt.Errorf("saving computors: %w", err)
		}
	}

	return *latestComps, nil
}

func (v *Validator) validateTickData(ctx context.Context, fetcher network.DataFetcher, comps computors.Computors, quorumVotes types.QuorumVotes, tickNumber uint32) (types.TickData, error) {
	tickData, err := fetcher.GetTickData(ctx, tickNumber)
	if err != nil {
		return types.TickData{}, fmt.Errorf("getting tick data: %w", err)
	}

	err = tick.Validate(ctx, tickData, quorumVotes[0], comps)
	if err != nil {
		return types.TickData{}, fmt.Errorf("validating tick data: %w", err)
	}

	return tickData, nil
}

func (v *Validator) validateTransactions(ctx context.Context, fetcher network.DataFetcher, transactions []types.Transaction, tickData types.TickData, tickNumber uint32) ([]types.Transaction, *protobuf.TickTransactionsStatus, error) {

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

	// get tx status
	tickTxStatus, txStatusAvailable, err := fetcher.GetTxStatus(ctx, tickNumber)
	if err != nil {
		return nil, nil, fmt.Errorf("getting tx status: %w", err)
	}

	shouldValidateStatus := txStatusAvailable && v.statusAddonEnabled
	if !txStatusAvailable {
		// fill in stub data when status is not available (e.g. bob backend)
		tickTxStatus = types.TransactionStatus{
			CurrentTickOfNode:  tickNumber,
			Tick:               tickNumber,
			TxCount:            uint32(len(validTxs)),
			MoneyFlew:          [128]byte{},
			TransactionDigests: nil,
		}
	}

	// combine valid transactions with money flew status
	transactionsWithTxStatus, err := txstatus.ValidateAndConvert(ctx, tickTxStatus, validTxs, shouldValidateStatus)
	if err != nil {
		return nil, nil, fmt.Errorf("validating tx status: %w", err)
	}

	return validTxs, transactionsWithTxStatus, nil

}

func isEmptyTick(quorumVotes types.QuorumVotes) bool {
	return quorumVotes[0].TxDigest == [32]byte{}
}
