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

// TickData is bob's view of a tick: the per-transaction `executed` flag keyed by
// lowercase tx hash. It is produced by FetchTick (network) and consumed by
// ComputeMoneyFlew (CPU), so the bob round-trip can run in parallel with the
// node fetches while the cross-check waits for the node-validated validTxs.
type TickData struct {
	executed map[string]bool
}

// FetchTick fetches the tick from bob via qubic_getTickByNumber and returns bob's
// executed map. It only needs the tick number, so it can be fetched concurrently
// with the node's tick data / transactions. A still-pending tx (executed=nil) is
// an error so the caller fails the tick and retries.
func FetchTick(ctx context.Context, client *Client, tickNumber uint32) (TickData, error) {
	ctx, span := client.tracer.Start(ctx, "bob.FetchTick",
		trace.WithAttributes(attribute.Int64("qubic.tick_number", int64(tickNumber))))
	defer span.End()

	raw, err := client.RPCCall(ctx, "qubic_getTickByNumber", []interface{}{tickNumber, true})
	if err != nil {
		err = fmt.Errorf("bob qubic_getTickByNumber tick=%d: %w", tickNumber, err)
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return TickData{}, err
	}

	var tickResp bobRPCTickResponse
	if err := json.Unmarshal(raw, &tickResp); err != nil {
		return TickData{}, fmt.Errorf("unmarshalling bob tick response tick=%d: %w", tickNumber, err)
	}

	bobExecuted := make(map[string]bool, len(tickResp.Transactions))
	for _, btx := range tickResp.Transactions {
		if btx.Executed == nil {
			return TickData{}, fmt.Errorf("bob tx [%s] at tick %d is pending (executed=nil); retrying tick", btx.Hash, tickNumber)
		}
		bobExecuted[strings.ToLower(btx.Hash)] = *btx.Executed
	}

	return TickData{executed: bobExecuted}, nil
}

// ComputeMoneyFlew cross-checks bob's executed set (from FetchTick) against the
// node-validated validTxs and returns a types.TransactionStatus whose MoneyFlew
// bit array is populated from bob's authoritative `executed` flag.
//
// On any disagreement (count mismatch or missing digest) it returns an error so
// the caller can fail the tick and let the processor retry.
func ComputeMoneyFlew(bobTick TickData, tickNumber uint32, validTxs []types.Transaction) (types.TransactionStatus, error) {
	if len(validTxs) == 0 {
		return types.TransactionStatus{
			CurrentTickOfNode: tickNumber,
			Tick:              tickNumber,
			TxCount:           0,
		}, nil
	}

	bobExecuted := bobTick.executed
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

// GetMoneyFlew fetches the bob tick and cross-checks it against validTxs in one
// call (FetchTick + ComputeMoneyFlew). It short-circuits with no RPC when there
// are no valid transactions. The hot path prefers fetching via FetchTick in
// parallel and calling ComputeMoneyFlew afterwards; this wrapper is kept for the
// simple serial case.
func GetMoneyFlew(ctx context.Context, client *Client, tickNumber uint32, validTxs []types.Transaction) (types.TransactionStatus, error) {
	if len(validTxs) == 0 {
		return types.TransactionStatus{
			CurrentTickOfNode: tickNumber,
			Tick:              tickNumber,
			TxCount:           0,
		}, nil
	}

	bobTick, err := FetchTick(ctx, client, tickNumber)
	if err != nil {
		return types.TransactionStatus{}, err
	}
	return ComputeMoneyFlew(bobTick, tickNumber, validTxs)
}

func setMoneyFlewBit(moneyFlew *[(types.NumberOfTransactionsPerTick + 7) / 8]byte, index int) {
	moneyFlew[index/8] |= 1 << (index % 8)
}
