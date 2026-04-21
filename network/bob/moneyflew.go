package bob

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	"github.com/qubic/go-node-connector/types"
)

// bobTxInfoResponse represents the fields we need from bob's GET /tx/{hash} endpoint.
type bobTxInfoResponse struct {
	Executed *bool  `json:"executed,omitempty"` // nil when hasIndexedInfo is false
	Error    string `json:"error,omitempty"`
}

// computeMoneyFlew determines the moneyFlew status for each transaction in a tick
// by querying bob's GET /tx/{hash} endpoint and reading the authoritative `executed` flag.
func computeMoneyFlew(ctx context.Context, client *Client, tickResp *bobTickResponse, tickNumber uint32) (types.TransactionStatus, error) {
	// Collect non-zero transaction digests
	var digestList [][32]byte
	var txHashes []string

	for i, digestStr := range tickResp.TickData.TransactionDigests {
		if digestStr == "" {
			continue
		}
		digest, err := qubicHashToBytes32(digestStr)
		if err != nil {
			return types.TransactionStatus{}, fmt.Errorf("converting digest[%d]: %w", i, err)
		}
		digestList = append(digestList, digest)
		txHashes = append(txHashes, digestStr) // already lowercase qubic hash
	}

	var moneyFlew [128]byte

	for i, txHash := range txHashes {
		executed, err := fetchTxExecuted(ctx, client, txHash)
		if err != nil {
			log.Printf("[WARN] failed to fetch executed status for tx %s: %v", txHash, err)
			continue
		}
		if executed {
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

// fetchTxExecuted queries bob's GET /tx/{hash} and returns the executed flag.
func fetchTxExecuted(ctx context.Context, client *Client, txHash string) (bool, error) {
	body, err := client.RESTGet(ctx, fmt.Sprintf("/tx/%s", txHash))
	if err != nil {
		return false, fmt.Errorf("GET /tx/%s: %w", txHash, err)
	}

	var info bobTxInfoResponse
	if err := json.Unmarshal(body, &info); err != nil {
		return false, fmt.Errorf("unmarshalling tx info: %w", err)
	}

	if info.Error != "" {
		return false, nil // tx not found or error — treat as not executed
	}

	if info.Executed == nil {
		return false, nil // not indexed yet — treat as not executed
	}

	return *info.Executed, nil
}

// setMoneyFlewBit sets bit at position index in the MoneyFlew byte array.
func setMoneyFlewBit(moneyFlew *[128]byte, index int) {
	bytePos := index / 8
	bitPos := index % 8
	moneyFlew[bytePos] |= 1 << bitPos
}
