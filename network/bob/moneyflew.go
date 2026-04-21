package bob

import (
	"fmt"

	"github.com/qubic/go-node-connector/types"
)

// computeMoneyFlew builds the MoneyFlew bit array from the executed status map.
// The executedMap is populated from bob's qubic_getTickByNumber response (bob 1.4.0+),
// where each transaction carries an authoritative `executed` flag.
func computeMoneyFlew(tickResp *bobTickResponse, executedMap map[string]bool, tickNumber uint32) (types.TransactionStatus, error) {
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

	var moneyFlew [128]byte

	for i, digestStr := range tickResp.TickData.TransactionDigests {
		if digestStr == "" {
			continue
		}
		if executedMap[digestStr] {
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

// setMoneyFlewBit sets bit at position index in the MoneyFlew byte array.
func setMoneyFlewBit(moneyFlew *[128]byte, index int) {
	bytePos := index / 8
	bitPos := index % 8
	moneyFlew[bytePos] |= 1 << bitPos
}
