package network

import (
	"context"
	"testing"

	qubic "github.com/qubic/go-node-connector/v2"
	"github.com/qubic/go-node-connector/v2/types"
	"github.com/stretchr/testify/require"
)

// fallbackStub satisfies QubicClient via an embedded nil interface; only GetComputors
// is implemented, so any non-prefetched call the adapter delegates is observable here
// and any unexpected delegation panics.
type fallbackStub struct {
	QubicClient
	computors       types.Computors
	computorsCalled bool
}

func (f *fallbackStub) GetComputors(context.Context) (types.Computors, error) {
	f.computorsCalled = true
	return f.computors, nil
}

func TestPrefetchedClient_ServesBatchAndDelegates(t *testing.T) {
	fallback := &fallbackStub{computors: types.Computors{Epoch: 7}}
	tickInfo := types.TickInfo{Tick: 1000, Epoch: 42}
	systemInfo := types.SystemInfo{InitialTick: 400}
	tp := qubic.TickPrefetch{
		Tick:         500,
		TickData:     types.TickData{Tick: 500},
		QuorumVotes:  types.QuorumVotes{{Tick: 500}},
		Transactions: types.Transactions{{Tick: 500}},
	}

	c := NewPrefetchedClient(fallback, tickInfo, systemInfo, tp)
	ctx := context.Background()

	gotTickInfo, err := c.GetTickInfo(ctx)
	require.NoError(t, err)
	require.Equal(t, tickInfo, gotTickInfo)

	gotSys, err := c.GetSystemInfo(ctx)
	require.NoError(t, err)
	require.Equal(t, systemInfo, gotSys)

	gotQV, err := c.GetQuorumVotes(ctx, 500)
	require.NoError(t, err)
	require.Equal(t, tp.QuorumVotes, gotQV)

	gotTD, err := c.GetTickData(ctx, 500)
	require.NoError(t, err)
	require.Equal(t, tp.TickData, gotTD)

	gotTxs, err := c.GetTickTransactions(ctx, 500)
	require.NoError(t, err)
	require.Equal(t, tp.Transactions, gotTxs)

	// GetComputors is not part of the batch -> must delegate to the fallback.
	gotComps, err := c.GetComputors(ctx)
	require.NoError(t, err)
	require.Equal(t, types.Computors{Epoch: 7}, gotComps)
	require.True(t, fallback.computorsCalled, "GetComputors must delegate to the fallback client")
}
