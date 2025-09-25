package txstatus

import (
	"fmt"
	"github.com/qubic/go-archiver-v2/protobuf"
	"github.com/qubic/go-node-connector/types"
)

func qubicToProto(txs types.Transactions, txStatus types.TransactionStatus) (*protobuf.TickTransactionsStatus, error) {
	tickTransactions := make([]*protobuf.TransactionStatus, 0, txStatus.TxCount)
	txsIdMap, err := createTxsIdMap(txs)
	if err != nil {
		return nil, fmt.Errorf("creating txs id map: %w", err)
	}

	for index, txDigest := range txStatus.TransactionDigests {
		var id types.Identity
		id, err := id.FromPubKey(txDigest, true)
		if err != nil {
			return nil, fmt.Errorf("converting digest to id: %w", err)
		}
		if _, ok := txsIdMap[id.String()]; !ok {
			continue
		}

		moneyFlew := getMoneyFlewFromBits(txStatus.MoneyFlew, index)

		tx := &protobuf.TransactionStatus{
			TxId:      id.String(),
			MoneyFlew: moneyFlew,
		}

		tickTransactions = append(tickTransactions, tx)
	}

	return &protobuf.TickTransactionsStatus{Transactions: tickTransactions}, nil
}

func createTxsIdMap(txs types.Transactions) (map[string]struct{}, error) {
	txsIdMap := make(map[string]struct{}, len(txs))
	for _, tx := range txs {
		digest, err := tx.Digest()
		if err != nil {
			return nil, fmt.Errorf("creating tx digest for tx with src pubkey [%s]: %w", tx.SourcePublicKey, err)
		}

		id, err := tx.ID()
		if err != nil {
			return nil, fmt.Errorf("converting tx with digest [%s] to id: %w", digest, err)
		}

		txsIdMap[id] = struct{}{}
	}

	return txsIdMap, nil
}

func getMoneyFlewFromBits(input [(types.NumberOfTransactionsPerTick + 7) / 8]byte, digestIndex int) bool {
	pos := digestIndex / 8
	bitIndex := digestIndex % 8

	return getNthBit(input[pos], bitIndex)
}

func getNthBit(input byte, bitIndex int) bool {
	// Shift the input byte to the right by the bitIndex positions
	// This isolates the bit at the bitIndex position at the least significant bit position
	shifted := input >> bitIndex

	// Extract the least significant bit using a bitwise AND operation with 1
	// If the least significant bit is 1, the result will be 1; otherwise, it will be 0
	return shifted&1 == 1
}
