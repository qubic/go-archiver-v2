package tx

import (
	"context"
	"encoding/hex"
	"fmt"

	"github.com/pkg/errors"
	"github.com/qubic/go-archiver-v2/db"
	"github.com/qubic/go-archiver-v2/utils"
	"github.com/qubic/go-node-connector/types"
)

var emptyTxDigest [32]byte

func Validate(ctx context.Context, transactions []types.Transaction, tickData types.TickData) ([]types.Transaction, error) {
	digestsMap := createTxDigestsMap(tickData)
	// handles empty tick but with transactions
	if len(digestsMap) == 0 {
		return []types.Transaction{}, nil
	}

	validTxs, err := validateTransactions(ctx, transactions, digestsMap)
	if err != nil {
		return nil, fmt.Errorf("validating transactions: %w", err)
	}

	return validTxs, nil
}

// validateTransactions validates the tick transactions against the digests map, if a transaction is not part of the
// digests map, it is considered invalid. if we have more transactions than digests, then we don't care.
// Implementation relies on the fact that for each valid transaction, the associated digest is removed
// from the digest map and at the end of the function, the map should be empty.
func validateTransactions(ctx context.Context, transactions []types.Transaction, digestsMap map[string]struct{}) ([]types.Transaction, error) {
	validTransactions := make([]types.Transaction, 0, len(transactions))
	for _, tx := range transactions {
		txDigest, err := getDigestFromTransaction(tx)
		if err != nil {
			return nil, fmt.Errorf("getting digest from transaction: %w", err)
		}

		txId, err := tx.ID()
		if err != nil {
			return nil, fmt.Errorf("getting tx hash: %w", err)
		}

		hexDigest := hex.EncodeToString(txDigest[:])
		if _, ok := digestsMap[hexDigest]; !ok {
			return nil, fmt.Errorf("transaction [%s] not found in digests map: %w", txId, err)
		}

		txDataBytes, err := tx.MarshallBinary()
		if err != nil {
			return nil, fmt.Errorf("marshalling transaction data: %w", err)
		}

		constructedDigest, err := utils.K12Hash(txDataBytes[:len(txDataBytes)-64])
		if err != nil {
			return nil, fmt.Errorf("calculating digest from transaction: %w", err)
		}

		err = utils.SchnorrqVerify(ctx, tx.SourcePublicKey, constructedDigest, tx.Signature)
		if err != nil {
			return nil, fmt.Errorf("verifying transaction signature: %w", err)
		}
		validTransactions = append(validTransactions, tx)
		delete(digestsMap, hexDigest)
	}

	if len(digestsMap) > 0 {
		return nil, errors.Errorf("not all digests were matched, remaining: %v", digestsMap)
	}

	return validTransactions, nil
}

func getDigestFromTransaction(tx types.Transaction) ([32]byte, error) {
	txDataMarshalledBytes, err := tx.MarshallBinary()
	if err != nil {
		return [32]byte{}, fmt.Errorf("marshalling transaction: %w", err)
	}

	digest, err := utils.K12Hash(txDataMarshalledBytes)
	if err != nil {
		return [32]byte{}, fmt.Errorf("hashing transaction: %w", err)
	}

	return digest, nil
}

func createTxDigestsMap(tickData types.TickData) map[string]struct{} {
	digestsMap := make(map[string]struct{})

	for _, digest := range tickData.TransactionDigests {
		if digest == emptyTxDigest {
			continue
		}

		hexDigest := hex.EncodeToString(digest[:])
		digestsMap[hexDigest] = struct{}{}
	}

	return digestsMap
}

func Store(ctx context.Context, store *db.PebbleStore, _ uint32, transactions types.Transactions) error {
	err := storeTickTransactions(ctx, store, transactions)
	if err != nil {
		return fmt.Errorf("storing transactions: %w", err)
	}

	return nil
}

func storeTickTransactions(ctx context.Context, store *db.PebbleStore, transactions types.Transactions) error {
	protoModel, err := qubicToProto(transactions)
	if err != nil {
		return fmt.Errorf("converting transactions to proto: %w", err)
	}

	err = store.SetTransactions(ctx, protoModel)
	if err != nil {
		return fmt.Errorf("saving data: %w", err)
	}

	return nil
}
