package api

import (
	"context"
	"errors"
	"fmt"
	"github.com/qubic/go-archiver-v2/db"
	"github.com/qubic/go-archiver-v2/protobuf"
)

func getAllTickTransactionsV2(ctx context.Context, database *db.PebbleStore, tickNumber uint32) (*protobuf.GetTickTransactionsResponseV2, error) {
	txs, err := database.GetTickTransactions(ctx, tickNumber)
	if err != nil {
		if errors.Is(err, db.ErrNotFound) {
			return nil, err // return raw for status code handling
		}
		return nil, fmt.Errorf("getting tick transactions: %w", err)
	}

	var transactions []*protobuf.TransactionData
	for _, transaction := range txs {
		timestamp, moneyFlew, err := getMoreTransactionInformation(ctx, database, transaction.TxId, transaction.TickNumber)
		if err != nil {
			return nil, fmt.Errorf("failed to get transaction info: %w", err)
		}

		transactions = append(transactions, &protobuf.TransactionData{
			Transaction: transaction,
			Timestamp:   timestamp,
			MoneyFlew:   moneyFlew,
		})
	}

	return &protobuf.GetTickTransactionsResponseV2{Transactions: transactions}, nil
}

func getMoreTransactionInformation(ctx context.Context, database *db.PebbleStore, transactionHash string, tickNumber uint32) (uint64, bool, error) {
	tickData, err := database.GetTickData(ctx, tickNumber)
	if err != nil {
		return 0, false, fmt.Errorf("getting tick data for tick [%d]: %w", tickNumber, err)
	}
	txStatus, err := database.GetTransactionStatus(ctx, transactionHash)
	if err != nil {
		if errors.Is(err, db.ErrNotFound) {
			// default false
			return tickData.Timestamp, false, nil
		}
		return 0, false, fmt.Errorf("getting transaction status for transaction [%s]: %w", transactionHash, err)
	}
	return tickData.Timestamp, txStatus.MoneyFlew, nil
}
