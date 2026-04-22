package network

import (
	"context"

	"github.com/qubic/go-node-connector/types"
)

// TickStatus represents current network tick/epoch information.
type TickStatus struct {
	Epoch                uint16
	Tick                 uint32
	InitialTick          uint32
	NumberOfAlignedVotes uint16
}

// SystemMetadata contains system-level metadata needed for validation.
// For backends that don't provide certain fields (e.g. bob), the Available
// flags indicate which fields are meaningful.
type SystemMetadata struct {
	InitialTick                uint32
	ComputorPacketSignature    uint64
	TargetTickVoteSignature    uint32
	ComputorSignatureAvailable bool // false for bob
	VoteSignatureAvailable     bool // false for bob
}

// DataFetcher abstracts all data fetching needed by the processing pipeline.
// Implementations exist for both Qubic core nodes (NodeDataFetcher) and
// bob JSON-RPC (BobDataFetcher).
type DataFetcher interface {
	// GetTickStatus returns current network tick/epoch info.
	GetTickStatus(ctx context.Context) (TickStatus, error)

	// GetSystemMetadata returns system-level metadata needed for validation.
	GetSystemMetadata(ctx context.Context) (SystemMetadata, error)

	// GetQuorumVotes fetches quorum votes for a given tick.
	GetQuorumVotes(ctx context.Context, tickNumber uint32) (types.QuorumVotes, error)

	// GetTickData fetches tick data for a given tick.
	GetTickData(ctx context.Context, tickNumber uint32) (types.TickData, error)

	// GetTickTransactions fetches transactions for a given tick.
	GetTickTransactions(ctx context.Context, tickNumber uint32) (types.Transactions, error)

	// GetComputors fetches the current computor list.
	GetComputors(ctx context.Context) (types.Computors, error)

	// GetTxStatus fetches transaction status for a given tick.
	// The bool return indicates whether the data is real (true) or a stub (false).
	GetTxStatus(ctx context.Context, tick uint32) (types.TransactionStatus, bool, error)

	// Release signals that the current fetch cycle is done.
	// For node-connector: returns connections to pool or closes on error.
	// For bob: no-op.
	Release(err error)
}
