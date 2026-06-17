package validator

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log"

	"github.com/qubic/go-archiver-v2/db"
	"github.com/qubic/go-archiver-v2/network"
	"github.com/qubic/go-archiver-v2/network/bob"
	"github.com/qubic/go-archiver-v2/protobuf"
	"github.com/qubic/go-archiver-v2/validator/computors"
	"github.com/qubic/go-archiver-v2/validator/quorum"
	"github.com/qubic/go-archiver-v2/validator/tick"
	"github.com/qubic/go-archiver-v2/validator/tx"
	"github.com/qubic/go-archiver-v2/validator/txstatus"
	"github.com/qubic/go-node-connector/v2/types"
	"go.opentelemetry.io/otel/trace"
	"golang.org/x/sync/errgroup"
)

type Validator struct {
	arbitratorPubKey   [32]byte
	statusAddonEnabled bool
	bobClient          *bob.Client // nil = use node's GetTxStatus for moneyFlew
	tracer             trace.Tracer
}

func NewValidator(arbitratorPubKey [32]byte, enableStatusAddon bool, bobClient *bob.Client, tracer trace.Tracer) *Validator {
	return &Validator{
		arbitratorPubKey:   arbitratorPubKey,
		statusAddonEnabled: enableStatusAddon,
		bobClient:          bobClient,
		tracer:             tracer,
	}
}

type Clients struct {
	Main network.QubicClient
	Alt  network.QubicClient
}

func (v *Validator) Validate(ctx context.Context, store *db.PebbleStore, clients Clients, epoch uint16, tickNumber uint32) error {

	ctx, span := v.tracer.Start(ctx, "validate")
	defer span.End()

	var quorumVotes types.QuorumVotes

	eg, egCtx := errgroup.WithContext(ctx)
	egCtx, fetchSpan := v.tracer.Start(egCtx, "fetch_quorum_and_systeminfo")
	eg.Go(func() error {
		// validate quorum
		var err error
		quorumVotes, err = clients.Main.GetQuorumVotes(egCtx, tickNumber)
		if err != nil {
			return fmt.Errorf("getting quorum votes: %w", err)
		}
		if len(quorumVotes) <= 0 {
			return errors.New("no quorum votes fetched")
		}
		if len(quorumVotes) < quorum.EmptyTickMinVoteCount {
			return fmt.Errorf("not enough quorum votes yet: [%d]", len(quorumVotes))
		}
		return nil
	})

	var systemInfo types.SystemInfo
	eg.Go(func() error {
		var err error
		systemInfo, err = clients.Alt.GetSystemInfo(egCtx)
		if err != nil {
			return fmt.Errorf("getting system info: %w", err)
		}
		return nil
	})

	err := eg.Wait()
	fetchSpan.End()
	if err != nil {
		return fmt.Errorf("getting quorum votes and/or system info: %w", err)
	}

	// typically only one client call needed per epoch
	compCtx, compSpan := v.tracer.Start(ctx, "validate_computors")
	comps, err := v.validateComputors(compCtx, store, clients.Main, tickNumber, systemInfo.InitialTick, epoch, systemInfo.ComputorPacketSignature)
	compSpan.End()
	if err != nil {
		return fmt.Errorf("validating computors: %w", err)
	}

	quorumCtx, quorumSpan := v.tracer.Start(ctx, "quorum.verify_signatures")
	alignedVotes, err := quorum.Validate(quorumCtx, quorumVotes, comps, systemInfo.TargetTickVoteSignature) // fast
	quorumSpan.End()
	if err != nil {
		return fmt.Errorf("validating quorum votes: %w", err)
	}
	log.Printf("Quorum valid. Aligned %d. Misaligned %d.", len(alignedVotes), len(quorumVotes)-len(alignedVotes))

	// validate tick data and transactions
	tdCtx, tdSpan := v.tracer.Start(ctx, "validate_tickdata_and_txs")
	tickData, validTxs, txStatus, err := v.validateTickDataAndTransactions(tdCtx, alignedVotes, clients, comps, tickNumber)
	tdSpan.End()
	if err != nil {
		return fmt.Errorf("validating tick data and transactions: %w", err)
	}

	// store data

	ctx, storeSpan := v.tracer.Start(ctx, "store_phase")
	defer storeSpan.End()

	storeCtx, s := v.tracer.Start(ctx, "store.quorum")
	err = quorum.Store(storeCtx, store, tickNumber, alignedVotes)
	s.End()
	if err != nil {
		return fmt.Errorf("storing aligned quorum votes: %w", err)
	}

	_, s = v.tracer.Start(ctx, "store.target_tick_signature")
	err = quorum.StoreTargetTickVoteSignature(store, uint32(epoch), tickNumber, systemInfo.InitialTick, systemInfo.TargetTickVoteSignature)
	s.End()
	if err != nil {
		return fmt.Errorf("storing target tick signature: %w", err)
	}

	storeCtx, s = v.tracer.Start(ctx, "store.tick")
	err = tick.Store(storeCtx, store, tickNumber, tickData)
	s.End()
	if err != nil {
		return fmt.Errorf("storing tick data: %w", err)
	}

	storeCtx, s = v.tracer.Start(ctx, "store.transactions")
	err = tx.Store(storeCtx, store, tickNumber, validTxs)
	s.End()
	if err != nil {
		return fmt.Errorf("storing transactions: %w", err)
	}

	storeCtx, s = v.tracer.Start(ctx, "store.tx_status")
	err = txstatus.Store(storeCtx, store, tickNumber, txStatus)
	s.End()
	if err != nil {
		return fmt.Errorf("storing transactions status: %w", err)
	}

	return nil
}

func (v *Validator) validateTickDataAndTransactions(ctx context.Context, alignedVotes types.QuorumVotes, clients Clients, comps computors.Computors, tickNumber uint32) (tickData types.TickData, validTxs []types.Transaction, txStatus *protobuf.TickTransactionsStatus, err error) {

	if quorum.IsEmptyTick(alignedVotes) {
		return types.TickData{}, make([]types.Transaction, 0), &protobuf.TickTransactionsStatus{}, nil
	}

	eg, egCtx := errgroup.WithContext(ctx)
	egCtx, fetchSpan := v.tracer.Start(egCtx, "fetch_tickdata_and_transactions")

	eg.Go(func() error {
		var err error
		tickData, err = v.validateTickData(egCtx, clients.Alt, comps, alignedVotes, tickNumber)
		if err != nil {
			return fmt.Errorf("getting tick data: %w", err)
		}
		return nil
	})

	var transactions []types.Transaction
	eg.Go(func() error {
		var err error
		transactions, err = clients.Main.GetTickTransactions(egCtx, tickNumber)
		if err != nil {
			return fmt.Errorf("getting transactions: %w", err)
		}
		return nil
	})

	// When the bob backend is enabled, prefetch its tick concurrently: the bob RPC
	// only needs the tick number, so its round-trip overlaps the node fetches above
	// instead of running serially after tx validation. The cross-check against the
	// node-validated validTxs happens later in computeTxsAndStatus.
	var bobTick bob.TickData
	bobPrefetched := v.statusAddonEnabled && v.bobClient != nil
	if bobPrefetched {
		eg.Go(func() error {
			var err error
			bobTick, err = bob.FetchTick(egCtx, v.bobClient, tickNumber)
			if err != nil {
				return fmt.Errorf("prefetching bob tick: %w", err)
			}
			return nil
		})
	}

	err = eg.Wait()
	fetchSpan.End()
	if err != nil {
		return tickData, nil, nil, fmt.Errorf("getting tick data and/or transactions: %w", err)
	}

	validTxs, txStatus, err = v.computeTxsAndStatus(ctx, clients.Main, transactions, tickData, tickNumber, bobTick, bobPrefetched)
	if err != nil {
		return tickData, nil, nil, fmt.Errorf("getting valid transactions and txStatus: %w", err)
	}

	return tickData, validTxs, txStatus, nil

}

func (v *Validator) validateComputors(ctx context.Context, store *db.PebbleStore, client network.QubicClient, tickNumber, initialTick uint32, epoch uint16, computorPacketSignature uint64) (computors.Computors, error) {

	comps, err := computors.Get(ctx, store, client, tickNumber, initialTick, epoch, computorPacketSignature)
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

	return *latestComps, nil
}

func (v *Validator) validateTickData(ctx context.Context, client network.QubicClient, comps computors.Computors, quorumVotes types.QuorumVotes, tickNumber uint32) (types.TickData, error) {
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

func (v *Validator) computeTxsAndStatus(ctx context.Context, client network.QubicClient, transactions []types.Transaction, tickData types.TickData, tickNumber uint32, bobTick bob.TickData, bobPrefetched bool) ([]types.Transaction, *protobuf.TickTransactionsStatus, error) {

	// keeps all transactions that are in the tick data digests
	txCtx, txSpan := v.tracer.Start(ctx, "tx.validate")
	validTxs, err := tx.Validate(txCtx, transactions, tickData)
	txSpan.End()
	if err != nil {
		return nil, nil, fmt.Errorf("getting valid transactions: %w", err)
	}

	if len(validTxs) == len(transactions) {
		log.Printf("All [%d] transactions are valid.", len(validTxs))
	} else {
		log.Printf("[%d] out of [%d] transactions are valid.", len(validTxs), len(transactions))
	}

	// get tx status only if status addon is enabled
	statusCtx, statusSpan := v.tracer.Start(ctx, "get_tx_status")
	tickTxStatus, err := v.getTxStatus(statusCtx, client, validTxs, tickNumber, bobTick, bobPrefetched)
	statusSpan.End()
	if err != nil {
		return nil, nil, fmt.Errorf("getting tx status: %w", err)
	}

	// combine valid transactions with money flew status
	transactionsWithTxStatus, err := txstatus.ValidateAndConvert(ctx, tickTxStatus, validTxs, v.statusAddonEnabled)
	if err != nil {
		return nil, nil, fmt.Errorf("validating tx status: %w", err)
	}

	return validTxs, transactionsWithTxStatus, nil

}

func (v *Validator) getTxStatus(ctx context.Context, client network.QubicClient, validTxs []types.Transaction, tickNumber uint32, bobTick bob.TickData, bobPrefetched bool) (types.TransactionStatus, error) {
	if !v.statusAddonEnabled {
		return types.TransactionStatus{
			CurrentTickOfNode:  tickNumber,
			Tick:               tickNumber,
			TxCount:            uint32(len(validTxs)),
			MoneyFlew:          [(types.NumberOfTransactionsPerTick + 7) / 8]byte{},
			TransactionDigests: nil,
		}, nil
	}
	if v.bobClient != nil {
		// Use the bob tick prefetched in parallel with the node fetches; the cross-check
		// here is CPU-only. Fall back to a serial fetch if it wasn't prefetched.
		if bobPrefetched {
			return bob.ComputeMoneyFlew(bobTick, tickNumber, validTxs)
		}
		return bob.GetMoneyFlew(ctx, v.bobClient, tickNumber, validTxs)
	}
	return client.GetTxStatus(ctx, tickNumber)
}
