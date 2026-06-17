package bob

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/qubic/go-node-connector/v2/types"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

// GetMoneyFlew fetches the tick from bob via qubic_getTickByNumber, verifies bob's
// transaction set matches the node-validated validTxs, and returns a
// types.TransactionStatus whose MoneyFlew bit array is populated from bob's
// authoritative `executed` flag.
//
// On any disagreement (count mismatch, missing digest, or a still-pending tx that
// the node already validated) it returns an error so the caller can fail the tick
// and let the processor retry.
func GetMoneyFlew(ctx context.Context, client *Client, tickNumber uint32, validTxs []types.Transaction) (status types.TransactionStatus, err error) {
	ctx, span := client.tracer.Start(ctx, "bob.GetMoneyFlew",
		trace.WithAttributes(
			attribute.Int64("qubic.tick_number", int64(tickNumber)),
			attribute.Int("bob.valid_tx_count", len(validTxs)),
		))
	defer func() {
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
		}
		span.End()
	}()

	if len(validTxs) == 0 {
		return types.TransactionStatus{
			CurrentTickOfNode: tickNumber,
			Tick:              tickNumber,
			TxCount:           0,
		}, nil
	}

	raw, err := client.RPCCall(ctx, "qubic_getTickByNumber", []interface{}{tickNumber, true})
	if err != nil {
		return types.TransactionStatus{}, fmt.Errorf("bob qubic_getTickByNumber tick=%d: %w", tickNumber, err)
	}

	var tickResp bobRPCTickResponse
	if err := json.Unmarshal(raw, &tickResp); err != nil {
		return types.TransactionStatus{}, fmt.Errorf("unmarshalling bob tick response tick=%d: %w", tickNumber, err)
	}

	bobExecuted := make(map[string]bool, len(tickResp.Transactions))
	for _, btx := range tickResp.Transactions {
		if btx.Executed == nil {
			return types.TransactionStatus{}, fmt.Errorf("bob tx [%s] at tick %d is pending (executed=nil); retrying tick", btx.Hash, tickNumber)
		}
		bobExecuted[strings.ToLower(btx.Hash)] = *btx.Executed
	}

	if len(bobExecuted) != len(validTxs) {
		return types.TransactionStatus{}, fmt.Errorf("bob/node tx count mismatch at tick %d: bob=%d node=%d", tickNumber, len(bobExecuted), len(validTxs))
	}

	var moneyFlew [(types.NumberOfTransactionsPerTick + 7) / 8]byte
	digests := make([][32]byte, 0, len(validTxs))

	for i, vtx := range validTxs {
		digest, err := vtx.Digest()
		if err != nil {
			return types.TransactionStatus{}, fmt.Errorf("computing digest for validTxs[%d] at tick %d: %w", i, tickNumber, err)
		}
		digests = append(digests, digest)

		var id types.Identity
		hashID, err := id.FromPubKey(digest, true)
		if err != nil {
			return types.TransactionStatus{}, fmt.Errorf("encoding digest for validTxs[%d] at tick %d: %w", i, tickNumber, err)
		}
		hashStr := strings.ToLower(hashID.String())

		executed, ok := bobExecuted[hashStr]
		if !ok {
			return types.TransactionStatus{}, fmt.Errorf("validTxs[%d] digest [%s] missing from bob at tick %d", i, hashStr, tickNumber)
		}
		if executed {
			setMoneyFlewBit(&moneyFlew, i)
		}
	}

	return types.TransactionStatus{
		CurrentTickOfNode:  tickNumber,
		Tick:               tickNumber,
		TxCount:            uint32(len(validTxs)),
		MoneyFlew:          moneyFlew,
		TransactionDigests: digests,
	}, nil
}

func setMoneyFlewBit(moneyFlew *[(types.NumberOfTransactionsPerTick + 7) / 8]byte, index int) {
	moneyFlew[index/8] |= 1 << (index % 8)
}
