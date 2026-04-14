package validator

import (
	"context"
	"errors"
	"testing"

	qubic "github.com/qubic/go-node-connector"
	"github.com/qubic/go-node-connector/types"
	"github.com/stretchr/testify/require"

	"github.com/qubic/go-archiver-v2/protobuf"
	"github.com/qubic/go-archiver-v2/validator/computors"
)

// stubClient implements network.QubicClient with configurable behaviour for the methods exercised
// by the validator. Any method that is not configured panics, acting as a strict expectation that
// it must not be called.
type stubClient struct {
	getTxStatusErr        error
	getTickDataFn         func(ctx context.Context, tickNumber uint32) (types.TickData, error)
	getTickTransactionsFn func(ctx context.Context, tickNumber uint32) (types.Transactions, error)
}

func (s *stubClient) GetTxStatus(context.Context, uint32) (types.TransactionStatus, error) {
	return types.TransactionStatus{}, s.getTxStatusErr
}

func (s *stubClient) GetTickData(ctx context.Context, tickNumber uint32) (types.TickData, error) {
	if s.getTickDataFn == nil {
		panic("unexpected call to GetTickData")
	}
	return s.getTickDataFn(ctx, tickNumber)
}

func (s *stubClient) GetTickTransactions(ctx context.Context, tickNumber uint32) (types.Transactions, error) {
	if s.getTickTransactionsFn == nil {
		panic("unexpected call to GetTickTransactions")
	}
	return s.getTickTransactionsFn(ctx, tickNumber)
}

func (s *stubClient) GetIssuedAssets(_ context.Context, _ string) (types.IssuedAssets, error) {
	panic("not implemented")
}
func (s *stubClient) GetPossessedAssets(_ context.Context, _ string) (types.PossessedAssets, error) {
	panic("not implemented")
}
func (s *stubClient) GetOwnedAssets(_ context.Context, _ string) (types.OwnedAssets, error) {
	panic("not implemented")
}
func (s *stubClient) GetIdentity(_ context.Context, _ string) (types.AddressInfo, error) {
	panic("not implemented")
}
func (s *stubClient) GetTickInfo(_ context.Context) (types.TickInfo, error) {
	panic("not implemented")
}
func (s *stubClient) GetSystemInfo(_ context.Context) (types.SystemInfo, error) {
	panic("not implemented")
}
func (s *stubClient) SendRawTransaction(_ context.Context, _ []byte) error {
	panic("not implemented")
}
func (s *stubClient) GetQuorumVotes(_ context.Context, _ uint32) (types.QuorumVotes, error) {
	panic("not implemented")
}
func (s *stubClient) GetComputors(_ context.Context) (types.Computors, error) {
	panic("not implemented")
}
func (s *stubClient) QuerySmartContract(_ context.Context, _ qubic.RequestContractFunction, _ []byte) (types.SmartContractData, error) {
	panic("not implemented")
}
func (s *stubClient) GetAssetPossessionsByFilter(_ context.Context, _, _, _, _ string, _, _ uint16) (types.AssetPossessions, error) {
	panic("not implemented")
}
func (s *stubClient) GetAssetOwnershipsByFilter(_ context.Context, _, _, _ string, _ uint16) (types.AssetOwnerships, error) {
	panic("not implemented")
}
func (s *stubClient) GetAssetIssuancesByFilter(_ context.Context, _, _ string) (types.AssetIssuances, error) {
	panic("not implemented")
}
func (s *stubClient) GetAssetIssuancesByUniverseIndex(_ context.Context, _ uint32) (types.AssetIssuances, error) {
	panic("not implemented")
}
func (s *stubClient) GetAssetOwnershipsByUniverseIndex(_ context.Context, _ uint32) (types.AssetOwnerships, error) {
	panic("not implemented")
}
func (s *stubClient) GetAssetPossessionsByUniverseIndex(_ context.Context, _ uint32) (types.AssetPossessions, error) {
	panic("not implemented")
}
func (s *stubClient) Close() error {
	panic("not implemented")
}

func TestValidateTickDataAndTransactions_EmptyTick(t *testing.T) {
	// When the quorum vote has a zero TxDigest the tick is considered empty and the function
	// must return immediately with empty-but-non-nil results without calling any client method.
	v := NewValidator([32]byte{}, false)

	emptyVotes := types.QuorumVotes{{TxDigest: [32]byte{}}}

	gotTickData, gotTxs, gotStatus, err := v.validateTickDataAndTransactions(
		context.Background(), emptyVotes, Clients{}, computors.Computors{}, 100,
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
	mainClient := &stubClient{
		getTickTransactionsFn: func(_ context.Context, _ uint32) (types.Transactions, error) {
			return nil, wantErr
		},
	}
	altClient := &stubClient{
		getTickDataFn: func(_ context.Context, _ uint32) (types.TickData, error) {
			return types.TickData{}, wantErr
		},
	}

	_, _, _, err := v.validateTickDataAndTransactions(
		context.Background(), nonEmptyVotes, Clients{Main: mainClient, Alt: altClient}, computors.Computors{}, 100,
	)
	require.ErrorIs(t, err, wantErr)
}

func TestValidateTickDataAndTransactions_GetTickDataError(t *testing.T) {
	v := NewValidator([32]byte{}, false)

	nonEmptyVotes := types.QuorumVotes{{TxDigest: [32]byte{1}}}

	// both goroutines use the same error group so we do not care which one errors first
	wantErr := errors.New("timeout")
	mainClient := &stubClient{
		getTickTransactionsFn: func(_ context.Context, _ uint32) (types.Transactions, error) {
			return nil, wantErr
		},
	}
	altClient := &stubClient{
		getTickDataFn: func(_ context.Context, _ uint32) (types.TickData, error) {
			return types.TickData{}, wantErr
		},
	}

	_, _, _, err := v.validateTickDataAndTransactions(
		context.Background(), nonEmptyVotes, Clients{Main: mainClient, Alt: altClient}, computors.Computors{}, 100,
	)
	require.ErrorIs(t, err, wantErr)
}

func TestValidateTransactions_StatusAddonEnabled_GetTxStatusError(t *testing.T) {
	wantErr := errors.New("connection refused")
	client := &stubClient{getTxStatusErr: wantErr}

	v := NewValidator([32]byte{}, true)

	// Empty tickData has no non-zero TransactionDigests, so tx.Validate returns [] immediately,
	// allowing the test to reach the getTxStatus call.
	_, _, err := v.validateTransactions(context.Background(), client, nil, types.TickData{}, 100)
	require.ErrorIs(t, err, wantErr)
}
