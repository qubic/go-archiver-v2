package bob

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/qubic/go-archiver-v2/network"
	"github.com/qubic/go-node-connector/types"
)

// BobDataFetcher implements network.DataFetcher by fetching data from a bob instance.
type BobDataFetcher struct {
	client *Client

	// Per-cycle cache for the tick response (GET /tick/{tick}).
	// Multiple DataFetcher methods read from the same tick endpoint.
	mu             sync.Mutex
	cachedTick     uint32
	cachedTickResp *bobTickResponse

	// Per-cycle cache for executed status (from qubic_getTickByNumber response).
	// Populated by GetTickTransactions, consumed by GetTxStatus.
	cachedExecuted     map[string]bool // txHash -> executed
	cachedExecutedTick uint32
}

// NewBobDataFetcher creates a new BobDataFetcher.
func NewBobDataFetcher(bobURL string) *BobDataFetcher {
	return &BobDataFetcher{
		client: NewClient(bobURL),
	}
}

func (b *BobDataFetcher) GetTickStatus(ctx context.Context) (network.TickStatus, error) {
	body, err := b.client.RESTGet(ctx, "/status")
	if err != nil {
		return network.TickStatus{}, fmt.Errorf("getting bob status: %w", err)
	}

	var status bobSyncStatus
	if err := json.Unmarshal(body, &status); err != nil {
		return network.TickStatus{}, fmt.Errorf("unmarshalling bob status: %w", err)
	}

	return network.TickStatus{
		Epoch:       status.Epoch,
		Tick:        status.CurrentVerifyLoggingTick,
		InitialTick: status.InitialTick,
		// Bob has already validated all data, so report full quorum alignment
		NumberOfAlignedVotes: 676,
	}, nil
}

func (b *BobDataFetcher) GetSystemMetadata(_ context.Context) (network.SystemMetadata, error) {
	// Bob doesn't expose ComputorPacketSignature or TargetTickVoteSignature.
	// These are marked as unavailable so the validator knows to skip related checks.
	// InitialTick is populated from the cached status obtained in GetTickStatus.
	return network.SystemMetadata{
		ComputorSignatureAvailable: false,
		VoteSignatureAvailable:     false,
	}, nil
}

func (b *BobDataFetcher) GetQuorumVotes(ctx context.Context, tickNumber uint32) (types.QuorumVotes, error) {
	tickResp, err := b.getTickResponse(ctx, tickNumber)
	if err != nil {
		return nil, fmt.Errorf("getting tick response: %w", err)
	}

	votes := make(types.QuorumVotes, 0, len(tickResp.Votes))
	for i, v := range tickResp.Votes {
		converted, err := convertVote(v)
		if err != nil {
			return nil, fmt.Errorf("converting vote[%d]: %w", i, err)
		}
		votes = append(votes, converted)
	}

	return votes, nil
}

func (b *BobDataFetcher) GetTickData(ctx context.Context, tickNumber uint32) (types.TickData, error) {
	tickResp, err := b.getTickResponse(ctx, tickNumber)
	if err != nil {
		return types.TickData{}, fmt.Errorf("getting tick response: %w", err)
	}

	return convertTickData(tickResp.TickData)
}

func (b *BobDataFetcher) GetTickTransactions(ctx context.Context, tickNumber uint32) (types.Transactions, error) {
	// Use RPC endpoint to get full transactions with signatures
	result, err := b.client.RPCCall(ctx, "qubic_getTickByNumber", []interface{}{tickNumber, true})
	if err != nil {
		return nil, fmt.Errorf("RPC qubic_getTickByNumber: %w", err)
	}

	var rpcTick bobRPCTickResponse
	if err := json.Unmarshal(result, &rpcTick); err != nil {
		return nil, fmt.Errorf("unmarshalling RPC tick response: %w", err)
	}

	txs := make(types.Transactions, 0, len(rpcTick.Transactions))
	executedMap := make(map[string]bool, len(rpcTick.Transactions))
	for i, rpcTx := range rpcTick.Transactions {
		tx, err := convertRPCTransaction(rpcTx)
		if err != nil {
			return nil, fmt.Errorf("converting transaction[%d]: %w", i, err)
		}
		tx.Tick = tickNumber
		txs = append(txs, tx)

		// Cache the executed status (nil/pending → not in map → treated as false)
		if rpcTx.Executed != nil {
			executedMap[rpcTx.Hash] = *rpcTx.Executed
		}
	}

	b.mu.Lock()
	b.cachedExecuted = executedMap
	b.cachedExecutedTick = tickNumber
	b.mu.Unlock()

	return txs, nil
}

func (b *BobDataFetcher) GetComputors(ctx context.Context) (types.Computors, error) {
	result, err := b.client.RPCCall(ctx, "qubic_getComputors", []interface{}{})
	if err != nil {
		return types.Computors{}, fmt.Errorf("RPC qubic_getComputors: %w", err)
	}

	var compsResp bobComputorsResponse
	if err := json.Unmarshal(result, &compsResp); err != nil {
		return types.Computors{}, fmt.Errorf("unmarshalling computors response: %w", err)
	}

	return convertComputors(compsResp.Computors, compsResp.Epoch)
}

func (b *BobDataFetcher) GetTxStatus(ctx context.Context, tick uint32) (types.TransactionStatus, bool, error) {
	tickResp, err := b.getTickResponse(ctx, tick)
	if err != nil {
		return types.TransactionStatus{}, false, fmt.Errorf("getting tick response for tx status: %w", err)
	}

	b.mu.Lock()
	executedMap := b.cachedExecuted
	b.mu.Unlock()

	if executedMap == nil {
		executedMap = make(map[string]bool)
	}

	status, err := computeMoneyFlew(tickResp, executedMap, tick)
	if err != nil {
		return types.TransactionStatus{}, false, fmt.Errorf("computing moneyFlew: %w", err)
	}

	return status, true, nil
}

func (b *BobDataFetcher) Release(_ error) {
	// Clear per-cycle caches
	b.mu.Lock()
	b.cachedTick = 0
	b.cachedTickResp = nil
	b.cachedExecuted = nil
	b.cachedExecutedTick = 0
	b.mu.Unlock()
}

// getTickResponse fetches and caches the full tick response from GET /tick/{tick}.
func (b *BobDataFetcher) getTickResponse(ctx context.Context, tickNumber uint32) (*bobTickResponse, error) {
	b.mu.Lock()
	if b.cachedTickResp != nil && b.cachedTick == tickNumber {
		resp := b.cachedTickResp
		b.mu.Unlock()
		return resp, nil
	}
	b.mu.Unlock()

	body, err := b.client.RESTGet(ctx, fmt.Sprintf("/tick/%d", tickNumber))
	if err != nil {
		return nil, fmt.Errorf("getting tick %d: %w", tickNumber, err)
	}

	var tickResp bobTickResponse
	if err := json.Unmarshal(body, &tickResp); err != nil {
		return nil, fmt.Errorf("unmarshalling tick response: %w", err)
	}

	b.mu.Lock()
	b.cachedTick = tickNumber
	b.cachedTickResp = &tickResp
	b.mu.Unlock()

	return &tickResp, nil
}
