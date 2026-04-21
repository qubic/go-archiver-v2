package bob

import (
	"encoding/hex"
	"fmt"

	"github.com/qubic/go-node-connector/types"
)

// qubicHashToBytes32 converts a Qubic hash (60-char lowercase identity encoding)
// to a [32]byte public key.
func qubicHashToBytes32(qubicHash string) ([32]byte, error) {
	if len(qubicHash) == 0 {
		return [32]byte{}, nil
	}
	id := types.Identity(qubicHash)
	pubKey, err := id.ToPubKey(true) // true = lowercase
	if err != nil {
		return [32]byte{}, fmt.Errorf("converting qubic hash %q to bytes: %w", qubicHash, err)
	}
	return pubKey, nil
}

// identityToBytes32 converts a Qubic identity (60-char uppercase) to [32]byte.
func identityToBytes32(identity string) ([32]byte, error) {
	if len(identity) == 0 {
		return [32]byte{}, nil
	}
	id := types.Identity(identity)
	pubKey, err := id.ToPubKey(false) // false = uppercase
	if err != nil {
		return [32]byte{}, fmt.Errorf("converting identity %q to bytes: %w", identity, err)
	}
	return pubKey, nil
}

// hexToBytes32 converts a hex-encoded string to [32]byte.
func hexToBytes32(hexStr string) ([32]byte, error) {
	var result [32]byte
	if len(hexStr) == 0 {
		return result, nil
	}
	decoded, err := hex.DecodeString(hexStr)
	if err != nil {
		return result, fmt.Errorf("decoding hex: %w", err)
	}
	if len(decoded) != 32 {
		return result, fmt.Errorf("expected 32 bytes, got %d", len(decoded))
	}
	copy(result[:], decoded)
	return result, nil
}

// hexToBytes64 converts a hex-encoded string to [64]byte (for signatures).
func hexToBytes64(hexStr string) ([64]byte, error) {
	var result [64]byte
	if len(hexStr) == 0 {
		return result, nil
	}
	decoded, err := hex.DecodeString(hexStr)
	if err != nil {
		return result, fmt.Errorf("decoding hex: %w", err)
	}
	if len(decoded) != 64 {
		return result, fmt.Errorf("expected 64 bytes, got %d", len(decoded))
	}
	copy(result[:], decoded)
	return result, nil
}

// hexToBytes converts a hex-encoded string to a byte slice.
func hexToBytes(hexStr string) ([]byte, error) {
	if len(hexStr) == 0 {
		return nil, nil
	}
	return hex.DecodeString(hexStr)
}

// convertVote converts a bob vote JSON to types.QuorumTickVote.
func convertVote(v bobVote) (types.QuorumTickVote, error) {
	prevSpectrumDigest, err := qubicHashToBytes32(v.PrevSpectrumDigest)
	if err != nil {
		return types.QuorumTickVote{}, fmt.Errorf("prevSpectrumDigest: %w", err)
	}
	prevUniverseDigest, err := qubicHashToBytes32(v.PrevUniverseDigest)
	if err != nil {
		return types.QuorumTickVote{}, fmt.Errorf("prevUniverseDigest: %w", err)
	}
	prevComputerDigest, err := qubicHashToBytes32(v.PrevComputerDigest)
	if err != nil {
		return types.QuorumTickVote{}, fmt.Errorf("prevComputerDigest: %w", err)
	}
	saltedSpectrumDigest, err := qubicHashToBytes32(v.SaltedSpectrumDigest)
	if err != nil {
		return types.QuorumTickVote{}, fmt.Errorf("saltedSpectrumDigest: %w", err)
	}
	saltedUniverseDigest, err := qubicHashToBytes32(v.SaltedUniverseDigest)
	if err != nil {
		return types.QuorumTickVote{}, fmt.Errorf("saltedUniverseDigest: %w", err)
	}
	saltedComputerDigest, err := qubicHashToBytes32(v.SaltedComputerDigest)
	if err != nil {
		return types.QuorumTickVote{}, fmt.Errorf("saltedComputerDigest: %w", err)
	}
	txDigest, err := qubicHashToBytes32(v.TransactionDigest)
	if err != nil {
		return types.QuorumTickVote{}, fmt.Errorf("transactionDigest: %w", err)
	}
	expectedNextTickTxDigest, err := qubicHashToBytes32(v.ExpectedNextTickTransactionDigest)
	if err != nil {
		return types.QuorumTickVote{}, fmt.Errorf("expectedNextTickTransactionDigest: %w", err)
	}
	signature, err := hexToBytes64(v.Signature)
	if err != nil {
		return types.QuorumTickVote{}, fmt.Errorf("signature: %w", err)
	}

	return types.QuorumTickVote{
		ComputorIndex:                 v.ComputorIndex,
		Epoch:                         v.Epoch,
		Tick:                          v.Tick,
		Millisecond:                   v.Millisecond,
		Second:                        v.Second,
		Minute:                        v.Minute,
		Hour:                          v.Hour,
		Day:                           v.Day,
		Month:                         v.Month,
		Year:                          v.Year,
		PreviousResourceTestingDigest: v.PrevResourceTestingDigest,
		SaltedResourceTestingDigest:   v.SaltedResourceTestingDigest,
		PreviousTransactionBodyDigest: v.PrevTransactionBodyDigest,
		SaltedTransactionBodyDigest:   v.SaltedTransactionBodyDigest,
		PreviousSpectrumDigest:        prevSpectrumDigest,
		PreviousUniverseDigest:        prevUniverseDigest,
		PreviousComputerDigest:        prevComputerDigest,
		SaltedSpectrumDigest:          saltedSpectrumDigest,
		SaltedUniverseDigest:          saltedUniverseDigest,
		SaltedComputerDigest:          saltedComputerDigest,
		TxDigest:                      txDigest,
		ExpectedNextTickTxDigest:      expectedNextTickTxDigest,
		Signature:                     signature,
	}, nil
}

// convertTickData converts bob tick data JSON to types.TickData.
func convertTickData(td bobTickData) (types.TickData, error) {
	timelock, err := qubicHashToBytes32(td.Timelock)
	if err != nil {
		return types.TickData{}, fmt.Errorf("timelock: %w", err)
	}

	var transactionDigests [types.NumberOfTransactionsPerTick][32]byte
	for i, digest := range td.TransactionDigests {
		if i >= types.NumberOfTransactionsPerTick {
			break
		}
		d, err := qubicHashToBytes32(digest)
		if err != nil {
			return types.TickData{}, fmt.Errorf("transactionDigest[%d]: %w", i, err)
		}
		transactionDigests[i] = d
	}

	var contractFees [1024]int64
	for i, fee := range td.ContractFees {
		if i >= 1024 {
			break
		}
		contractFees[i] = fee
	}

	signature, err := hexToBytes64(td.Signature)
	if err != nil {
		return types.TickData{}, fmt.Errorf("signature: %w", err)
	}

	return types.TickData{
		ComputorIndex:      td.ComputorIndex,
		Epoch:              td.Epoch,
		Tick:               td.Tick,
		Millisecond:        td.Millisecond,
		Second:             td.Second,
		Minute:             td.Minute,
		Hour:               td.Hour,
		Day:                td.Day,
		Month:              td.Month,
		Year:               td.Year,
		Timelock:           timelock,
		TransactionDigests: transactionDigests,
		ContractFees:       contractFees,
		Signature:          signature,
	}, nil
}

// convertRPCTransaction converts a bob RPC transaction to types.Transaction.
func convertRPCTransaction(tx bobRPCTransaction) (types.Transaction, error) {
	sourcePubKey, err := identityToBytes32(tx.From)
	if err != nil {
		return types.Transaction{}, fmt.Errorf("source identity: %w", err)
	}
	destPubKey, err := identityToBytes32(tx.To)
	if err != nil {
		return types.Transaction{}, fmt.Errorf("dest identity: %w", err)
	}
	signature, err := hexToBytes64(tx.Signature)
	if err != nil {
		return types.Transaction{}, fmt.Errorf("signature: %w", err)
	}
	input, err := hexToBytes(tx.InputData)
	if err != nil {
		return types.Transaction{}, fmt.Errorf("input data: %w", err)
	}

	return types.Transaction{
		SourcePublicKey:      sourcePubKey,
		DestinationPublicKey: destPubKey,
		Amount:               tx.Amount,
		Tick:                 0, // tick is not in the per-tx object from qubic_getTickByNumber
		InputType:            tx.InputType,
		InputSize:            tx.InputSize,
		Input:                input,
		Signature:            signature,
	}, nil
}

// convertComputors converts bob's computor identity list to types.Computors.
func convertComputors(identities []string, epoch uint16) (types.Computors, error) {
	var comps types.Computors
	comps.Epoch = epoch

	for i, identity := range identities {
		if i >= types.NumberOfComputors {
			break
		}
		pubKey, err := identityToBytes32(identity)
		if err != nil {
			return types.Computors{}, fmt.Errorf("computor[%d] identity: %w", i, err)
		}
		comps.PubKeys[i] = pubKey
	}

	// Bob doesn't expose the computor list signature
	// Signature stays as zero value [64]byte{}

	return comps, nil
}
