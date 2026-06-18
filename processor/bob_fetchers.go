package processor

import (
	"context"
	"fmt"

	"github.com/qubic/go-archiver-v2/network/bob"
)

// liveBobFetcher fetches bob's per-tick tx status over HTTP on demand. It backs the
// non-prefetch path and satisfies validator.BobTickFetcher.
type liveBobFetcher struct {
	client *bob.Client
}

func (f liveBobFetcher) FetchTick(ctx context.Context, tick uint32) (bob.TickData, error) {
	return bob.FetchTick(ctx, f.client, tick)
}

// prefetchedBobFetcher serves bob tx status from a batch fetched in parallel up front.
// It satisfies validator.BobTickFetcher.
type prefetchedBobFetcher struct {
	ticks map[uint32]bob.TickData
}

func (f prefetchedBobFetcher) FetchTick(_ context.Context, tick uint32) (bob.TickData, error) {
	bt, ok := f.ticks[tick]
	if !ok {
		return bob.TickData{}, fmt.Errorf("no prefetched bob data for tick %d", tick)
	}
	return bt, nil
}
