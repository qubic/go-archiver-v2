package network

import (
	"context"
	"github.com/qubic/go-node-connector"
	"github.com/qubic/go-node-connector/types"
)

// QubicClient interface extracted from qubic.Client in go-node-connector.
type QubicClient interface {
	GetIssuedAssets(ctx context.Context, id string) (types.IssuedAssets, error)
	GetPossessedAssets(ctx context.Context, id string) (types.PossessedAssets, error)
	GetOwnedAssets(ctx context.Context, id string) (types.OwnedAssets, error)
	GetIdentity(ctx context.Context, id string) (types.AddressInfo, error)
	GetTickInfo(ctx context.Context) (types.TickInfo, error)
	GetSystemInfo(ctx context.Context) (types.SystemInfo, error)
	GetTxStatus(ctx context.Context, tick uint32) (types.TransactionStatus, error)
	GetTickData(ctx context.Context, tickNumber uint32) (types.TickData, error)
	GetTickTransactions(ctx context.Context, tickNumber uint32) (types.Transactions, error)
	SendRawTransaction(ctx context.Context, rawTx []byte) error
	GetQuorumVotes(ctx context.Context, tickNumber uint32) (types.QuorumVotes, error)
	GetComputors(ctx context.Context) (types.Computors, error)
	QuerySmartContract(ctx context.Context, rcf qubic.RequestContractFunction, requestData []byte) (types.SmartContractData, error)
	GetAssetPossessionsByFilter(ctx context.Context, issuerIdentity, assetName,
		ownerIdentity, possessorIdentity string, ownerContract, possessorContract uint16) (types.AssetPossessions, error)
	GetAssetOwnershipsByFilter(ctx context.Context, issuerIdentity, assetName,
		ownerIdentity string, ownerContract uint16) (types.AssetOwnerships, error)
	GetAssetIssuancesByFilter(ctx context.Context, issuerIdentity, assetName string) (types.AssetIssuances, error)
	GetAssetIssuancesByUniverseIndex(ctx context.Context, index uint32) (types.AssetIssuances, error)
	GetAssetOwnershipsByUniverseIndex(ctx context.Context, index uint32) (types.AssetOwnerships, error)
	GetAssetPossessionsByUniverseIndex(ctx context.Context, index uint32) (types.AssetPossessions, error)
	Close() error
}
