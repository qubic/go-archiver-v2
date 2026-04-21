package bob

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"

	"github.com/qubic/go-node-connector/types"
)

const quTransferLogType = 0

// firstQUTransfer holds the fields from the first QU_TRANSFER event for a transaction.
type firstQUTransfer struct {
	Source      string // uppercase identity
	Destination string // uppercase identity
	Amount      int64
	Tick        uint32
}

// computeMoneyFlew fetches logs for the given tick and computes the moneyFlew
// status for each transaction by comparing the transaction's fields against its
// first QU_TRANSFER log event.
//
// Algorithm (from bob maintainer):
//
//	moneyFlew = (tx.amount == first_event.amount)
//	         && (tx.src == first_event.src)
//	         && (tx.dst == first_event.dst)
//	         && (tx.tick == first_event.tick)
//
// Only transactions with amount > 0 are candidates; others default to false.
func computeMoneyFlew(ctx context.Context, client *Client, tickResp *bobTickResponse, txs types.Transactions, tickNumber uint32) (types.TransactionStatus, error) {
	epoch := tickResp.TickData.Epoch
	logIdStart := tickResp.TickData.LogIdStart
	logIdEnd := tickResp.TickData.LogIdEnd

	// Build transaction lookup: txHash -> Transaction
	txByHash := make(map[string]*types.Transaction, len(txs))
	for i := range txs {
		txID, err := txs[i].ID()
		if err != nil {
			return types.TransactionStatus{}, fmt.Errorf("computing tx ID for index %d: %w", i, err)
		}
		txByHash[txID] = &txs[i]
	}

	// Collect non-zero transaction digests and build the digest list
	var digestList [][32]byte
	for i, digestStr := range tickResp.TickData.TransactionDigests {
		if digestStr == "" {
			continue
		}
		digest, err := qubicHashToBytes32(digestStr)
		if err != nil {
			return types.TransactionStatus{}, fmt.Errorf("converting digest[%d]: %w", i, err)
		}
		digestList = append(digestList, digest)
	}

	// Build the MoneyFlew bit array
	var moneyFlew [128]byte

	// If there are no logs, return with all-zero moneyFlew
	if logIdStart < 0 || logIdEnd < logIdStart {
		return types.TransactionStatus{
			CurrentTickOfNode:  tickNumber,
			Tick:               tickNumber,
			TxCount:            uint32(len(digestList)),
			MoneyFlew:          moneyFlew,
			TransactionDigests: digestList,
		}, nil
	}

	// Fetch all logs for this tick
	firstTransfers, err := fetchFirstQUTransfers(ctx, client, epoch, logIdStart, logIdEnd)
	if err != nil {
		return types.TransactionStatus{}, fmt.Errorf("fetching logs for tick %d: %w", tickNumber, err)
	}

	// For each transaction digest, determine moneyFlew
	for i, digest := range digestList {
		// Convert digest to txHash (60-char lowercase)
		var id types.Identity
		id, err := id.FromPubKey(digest, true)
		if err != nil {
			log.Printf("[WARN] failed to convert digest[%d] to identity: %v", i, err)
			continue
		}
		txHash := id.String()

		// Look up the actual transaction
		tx, hasTx := txByHash[txHash]
		if !hasTx || tx.Amount <= 0 {
			continue // moneyFlew stays false
		}

		// Look up the first QU_TRANSFER for this tx
		transfer, hasLog := firstTransfers[txHash]
		if !hasLog {
			continue // no QU_TRANSFER event -> moneyFlew = false
		}

		// Convert tx public keys to uppercase identities for comparison
		var srcID types.Identity
		srcID, err = srcID.FromPubKey(tx.SourcePublicKey, false)
		if err != nil {
			log.Printf("[WARN] failed to convert source pubkey for tx %s: %v", txHash, err)
			continue
		}
		var dstID types.Identity
		dstID, err = dstID.FromPubKey(tx.DestinationPublicKey, false)
		if err != nil {
			log.Printf("[WARN] failed to convert dest pubkey for tx %s: %v", txHash, err)
			continue
		}

		// Apply the moneyFlew algorithm
		if tx.Amount == transfer.Amount &&
			string(srcID) == transfer.Source &&
			string(dstID) == transfer.Destination &&
			tx.Tick == transfer.Tick {
			setMoneyFlewBit(&moneyFlew, i)
		}
	}

	return types.TransactionStatus{
		CurrentTickOfNode:  tickNumber,
		Tick:               tickNumber,
		TxCount:            uint32(len(digestList)),
		MoneyFlew:          moneyFlew,
		TransactionDigests: digestList,
	}, nil
}

// fetchFirstQUTransfers fetches all logs for the tick and returns a map of
// txHash -> first QU_TRANSFER event for that transaction.
func fetchFirstQUTransfers(ctx context.Context, client *Client, epoch uint16, logIdStart, logIdEnd int64) (map[string]firstQUTransfer, error) {
	path := fmt.Sprintf("/log/%d/%d/%d", epoch, logIdStart, logIdEnd)
	body, err := client.RESTGet(ctx, path)
	if err != nil {
		return nil, fmt.Errorf("GET %s: %w", path, err)
	}

	var logs []bobLogEvent
	if err := json.Unmarshal(body, &logs); err != nil {
		return nil, fmt.Errorf("unmarshalling logs: %w", err)
	}

	result := make(map[string]firstQUTransfer)
	for _, logEvent := range logs {
		if !logEvent.OK {
			continue
		}
		if logEvent.Type != quTransferLogType {
			continue
		}
		txHash := strings.ToLower(logEvent.TxHash)
		if txHash == "" {
			continue
		}
		// Only keep the first QU_TRANSFER per transaction
		if _, exists := result[txHash]; exists {
			continue
		}
		result[txHash] = firstQUTransfer{
			Source:      logEvent.Body.From,
			Destination: logEvent.Body.To,
			Amount:      logEvent.Body.Amount,
			Tick:        logEvent.Tick,
		}
	}

	return result, nil
}

// setMoneyFlewBit sets bit at position index in the MoneyFlew byte array.
func setMoneyFlewBit(moneyFlew *[128]byte, index int) {
	bytePos := index / 8
	bitPos := index % 8
	moneyFlew[bytePos] |= 1 << bitPos
}
