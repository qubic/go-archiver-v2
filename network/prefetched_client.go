package network

import (
	"context"

	qubic "github.com/qubic/go-node-connector/v2"
	"github.com/qubic/go-node-connector/v2/types"
)

// prefetchedClient serves one tick's prefetched batch data through the QubicClient
// interface, so the validator runs unchanged against in-memory responses. It embeds
// a fallback QubicClient for everything not in the batch (notably GetComputors, which
// only hits the network on epoch start and is otherwise cached by computors.Get).
type prefetchedClient struct {
	QubicClient // fallback for non-prefetched calls
	tickInfo    types.TickInfo
	systemInfo  types.SystemInfo
	tick        qubic.TickPrefetch
}

// NewPrefetchedClient returns a QubicClient that serves the prefetched SystemInfo,
// TickInfo and the per-tick QuorumVotes/TickData/Transactions from memory, delegating
// any other call to fallback.
func NewPrefetchedClient(fallback QubicClient, tickInfo types.TickInfo, systemInfo types.SystemInfo, tick qubic.TickPrefetch) QubicClient {
	return &prefetchedClient{
		QubicClient: fallback,
		tickInfo:    tickInfo,
		systemInfo:  systemInfo,
		tick:        tick,
	}
}

func (c *prefetchedClient) GetTickInfo(context.Context) (types.TickInfo, error) {
	return c.tickInfo, nil
}

func (c *prefetchedClient) GetSystemInfo(context.Context) (types.SystemInfo, error) {
	return c.systemInfo, nil
}

func (c *prefetchedClient) GetQuorumVotes(_ context.Context, _ uint32) (types.QuorumVotes, error) {
	return c.tick.QuorumVotes, nil
}

func (c *prefetchedClient) GetTickData(_ context.Context, _ uint32) (types.TickData, error) {
	return c.tick.TickData, nil
}

func (c *prefetchedClient) GetTickTransactions(_ context.Context, _ uint32) (types.Transactions, error) {
	return c.tick.Transactions, nil
}
