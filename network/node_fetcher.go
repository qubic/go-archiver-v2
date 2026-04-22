package network

import (
	"context"
	"fmt"
	"log"

	"github.com/qubic/go-node-connector/types"
)

// NodeDataFetcher implements DataFetcher by wrapping a QubicClientPool and
// acquiring two connections (Main/Alt) per processing cycle, preserving the
// existing two-client pattern used by the validator.
type NodeDataFetcher struct {
	pool QubicClientPool
	main QubicClient
	alt  QubicClient
}

// NewNodeDataFetcher creates a new NodeDataFetcher that acquires two clients
// from the pool. Call Release() when done.
func NewNodeDataFetcher(pool QubicClientPool) (*NodeDataFetcher, error) {
	main, err := pool.Get()
	if err != nil {
		return nil, fmt.Errorf("getting 1st client connection: %w", err)
	}

	alt, err := pool.Get()
	if err != nil {
		// close the first client if we fail to get the second
		cErr := pool.Close(main)
		if cErr != nil {
			log.Printf("[ERROR] closing 1st client after 2nd client acquisition failure: %s", cErr.Error())
		}
		return nil, fmt.Errorf("getting 2nd client connection: %w", err)
	}

	return &NodeDataFetcher{
		pool: pool,
		main: main,
		alt:  alt,
	}, nil
}

func (n *NodeDataFetcher) GetTickStatus(ctx context.Context) (TickStatus, error) {
	tickInfo, err := n.main.GetTickInfo(ctx)
	if err != nil {
		return TickStatus{}, fmt.Errorf("getting tick info: %w", err)
	}
	return TickStatus{
		Epoch:                tickInfo.Epoch,
		Tick:                 tickInfo.Tick,
		InitialTick:          tickInfo.InitialTick,
		NumberOfAlignedVotes: tickInfo.NumberOfAlignedVotes,
	}, nil
}

func (n *NodeDataFetcher) GetSystemMetadata(ctx context.Context) (SystemMetadata, error) {
	systemInfo, err := n.alt.GetSystemInfo(ctx)
	if err != nil {
		return SystemMetadata{}, fmt.Errorf("getting system info: %w", err)
	}
	return SystemMetadata{
		InitialTick:                systemInfo.InitialTick,
		ComputorPacketSignature:    systemInfo.ComputorPacketSignature,
		TargetTickVoteSignature:    systemInfo.TargetTickVoteSignature,
		ComputorSignatureAvailable: true,
		VoteSignatureAvailable:     true,
	}, nil
}

func (n *NodeDataFetcher) GetQuorumVotes(ctx context.Context, tickNumber uint32) (types.QuorumVotes, error) {
	return n.main.GetQuorumVotes(ctx, tickNumber)
}

func (n *NodeDataFetcher) GetTickData(ctx context.Context, tickNumber uint32) (types.TickData, error) {
	return n.alt.GetTickData(ctx, tickNumber)
}

func (n *NodeDataFetcher) GetTickTransactions(ctx context.Context, tickNumber uint32) (types.Transactions, error) {
	return n.main.GetTickTransactions(ctx, tickNumber)
}

func (n *NodeDataFetcher) GetComputors(ctx context.Context) (types.Computors, error) {
	return n.main.GetComputors(ctx)
}

func (n *NodeDataFetcher) GetTxStatus(ctx context.Context, tick uint32) (types.TransactionStatus, bool, error) {
	status, err := n.main.GetTxStatus(ctx, tick)
	if err != nil {
		return types.TransactionStatus{}, false, err
	}
	return status, true, nil
}

func (n *NodeDataFetcher) Release(err error) {
	n.releaseClient(err, n.main)
	n.releaseClient(err, n.alt)
}

func (n *NodeDataFetcher) releaseClient(err error, client QubicClient) {
	if client == nil {
		return
	}
	if err == nil {
		pErr := n.pool.Put(client)
		if pErr != nil {
			log.Printf("[ERROR] putting connection back to pool: %s", pErr.Error())
		}
	} else {
		log.Printf("Closing connection because of error: %v", err)
		cErr := n.pool.Close(client)
		if cErr != nil {
			log.Printf("[ERROR] closing connection: %s", cErr.Error())
		}
	}
}
