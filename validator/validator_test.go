package validator

import (
	"context"
	"errors"
	"testing"

	"github.com/qubic/go-archiver-v2/network"
	"github.com/qubic/go-archiver-v2/protobuf"
	"github.com/qubic/go-archiver-v2/validator/computors"
	"github.com/qubic/go-node-connector/types"
	"github.com/stretchr/testify/require"
)

// stubFetcher implements network.DataFetcher with configurable behaviour for the methods
// exercised by the validator. Any method that is not configured panics, acting as a strict
// expectation that it must not be called.
type stubFetcher struct {
	getTickDataFn         func(ctx context.Context, tickNumber uint32) (types.TickData, error)
	getTickTransactionsFn func(ctx context.Context, tickNumber uint32) (types.Transactions, error)
	getTxStatusErr        error
}

func (s *stubFetcher) GetTickStatus(_ context.Context) (network.TickStatus, error) {
	panic("unexpected call to GetTickStatus")
}

func (s *stubFetcher) GetSystemMetadata(_ context.Context) (network.SystemMetadata, error) {
	panic("unexpected call to GetSystemMetadata")
}

func (s *stubFetcher) GetQuorumVotes(_ context.Context, _ uint32) (types.QuorumVotes, error) {
	panic("unexpected call to GetQuorumVotes")
}

func (s *stubFetcher) GetTickData(ctx context.Context, tickNumber uint32) (types.TickData, error) {
	if s.getTickDataFn == nil {
		panic("unexpected call to GetTickData")
	}
	return s.getTickDataFn(ctx, tickNumber)
}

func (s *stubFetcher) GetTickTransactions(ctx context.Context, tickNumber uint32) (types.Transactions, error) {
	if s.getTickTransactionsFn == nil {
		panic("unexpected call to GetTickTransactions")
	}
	return s.getTickTransactionsFn(ctx, tickNumber)
}

func (s *stubFetcher) GetComputors(_ context.Context) (types.Computors, error) {
	panic("unexpected call to GetComputors")
}

func (s *stubFetcher) GetTxStatus(_ context.Context, _ uint32) (types.TransactionStatus, bool, error) {
	return types.TransactionStatus{}, true, s.getTxStatusErr
}

func (s *stubFetcher) Release(_ error) {}

func TestValidateTickDataAndTransactions_EmptyTick(t *testing.T) {
	// When the quorum vote has a zero TxDigest the tick is considered empty and the function
	// must return immediately with empty-but-non-nil results without calling any client method.
	v := NewValidator([32]byte{}, false)

	emptyVotes := types.QuorumVotes{{TxDigest: [32]byte{}}}

	gotTickData, gotTxs, gotStatus, err := v.validateTickDataAndTransactions(
		context.Background(), emptyVotes, &stubFetcher{}, computors.Computors{}, 100,
	)
	require.NoError(t, err)
	require.Equal(t, types.TickData{}, gotTickData)
	require.Equal(t, []types.Transaction{}, gotTxs)
	require.Equal(t, &protobuf.TickTransactionsStatus{}, gotStatus)
}

func TestValidateTickDataAndTransactions_GetTickTransactionsError(t *testing.T) {
	v := NewValidator([32]byte{}, false)

	// Non-zero TxDigest so isEmptyTick returns false and the parallel fetch is executed.
	nonEmptyVotes := types.QuorumVotes{{TxDigest: [32]byte{1}}}

	// both goroutines use the same error group so we do not care which one errors first
	wantErr := errors.New("network error")
	fetcher := &stubFetcher{
		getTickTransactionsFn: func(_ context.Context, _ uint32) (types.Transactions, error) {
			return nil, wantErr
		},
		getTickDataFn: func(_ context.Context, _ uint32) (types.TickData, error) {
			return types.TickData{}, wantErr
		},
	}

	_, _, _, err := v.validateTickDataAndTransactions(
		context.Background(), nonEmptyVotes, fetcher, computors.Computors{}, 100,
	)
	require.ErrorIs(t, err, wantErr)
}

func TestValidateTickDataAndTransactions_GetTickDataError(t *testing.T) {
	v := NewValidator([32]byte{}, false)

	nonEmptyVotes := types.QuorumVotes{{TxDigest: [32]byte{1}}}

	// both goroutines use the same error group so we do not care which one errors first
	wantErr := errors.New("timeout")
	fetcher := &stubFetcher{
		getTickTransactionsFn: func(_ context.Context, _ uint32) (types.Transactions, error) {
			return nil, wantErr
		},
		getTickDataFn: func(_ context.Context, _ uint32) (types.TickData, error) {
			return types.TickData{}, wantErr
		},
	}

	_, _, _, err := v.validateTickDataAndTransactions(
		context.Background(), nonEmptyVotes, fetcher, computors.Computors{}, 100,
	)
	require.ErrorIs(t, err, wantErr)
}

func TestValidateTransactions_StatusAddonEnabled_GetTxStatusError(t *testing.T) {
	wantErr := errors.New("connection refused")
	fetcher := &stubFetcher{getTxStatusErr: wantErr}

	v := NewValidator([32]byte{}, true)

	// Empty tickData has no non-zero TransactionDigests, so tx.Validate returns [] immediately,
	// allowing the test to reach the getTxStatus call.
	_, _, err := v.validateTransactions(context.Background(), fetcher, nil, types.TickData{}, 100)
	require.ErrorIs(t, err, wantErr)
}
