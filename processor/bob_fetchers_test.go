package processor

import (
	"context"
	"testing"

	"github.com/qubic/go-archiver-v2/network/bob"
	"github.com/stretchr/testify/require"
)

func TestPrefetchedBobFetcher_ServesCachedAndErrorsOnMissing(t *testing.T) {
	cached := bob.TickData{}
	f := prefetchedBobFetcher{ticks: map[uint32]bob.TickData{500: cached}}

	got, err := f.FetchTick(context.Background(), 500)
	require.NoError(t, err)
	require.Equal(t, cached, got)

	_, err = f.FetchTick(context.Background(), 999)
	require.Error(t, err)
	require.Contains(t, err.Error(), "no prefetched bob data for tick 999")
}
