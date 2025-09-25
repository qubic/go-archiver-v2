package tick

import (
	"encoding/hex"
	"fmt"
	"github.com/qubic/go-archiver-v2/protobuf"
	"github.com/qubic/go-node-connector/types"
	"time"
)

func qubicToProto(tickData types.TickData) (*protobuf.TickData, error) {
	if tickData.IsEmpty() {
		return nil, nil
	}

	date := time.Date(2000+int(tickData.Year), time.Month(tickData.Month), int(tickData.Day), int(tickData.Hour), int(tickData.Minute), int(tickData.Second), 0, time.UTC)
	timestamp := date.UnixMilli() + int64(tickData.Millisecond)
	transactionIds, err := digestsToIdentities(tickData.TransactionDigests)
	if err != nil {
		return nil, fmt.Errorf("getting transaction hashes from digests: %w", err)
	}
	return &protobuf.TickData{
		ComputorIndex:  uint32(tickData.ComputorIndex),
		Epoch:          uint32(tickData.Epoch),
		TickNumber:     tickData.Tick,
		Timestamp:      uint64(timestamp),
		TimeLock:       tickData.Timelock[:],
		TransactionIds: transactionIds,
		ContractFees:   contractFeesToProto(tickData.ContractFees),
		SignatureHex:   hex.EncodeToString(tickData.Signature[:]),
	}, nil
}

func digestsToIdentities(digests [types.NumberOfTransactionsPerTick][32]byte) ([]string, error) {
	identities := make([]string, 0)
	for _, digest := range digests {
		if digest == [32]byte{} {
			continue
		}
		var id types.Identity
		id, err := id.FromPubKey(digest, true)
		if err != nil {
			return nil, fmt.Errorf("getting identity from digest hex %s: %w", hex.EncodeToString(digest[:]), err)
		}
		identities = append(identities, id.String())
	}

	return identities, nil
}

func contractFeesToProto(contractFees [1024]int64) []int64 {
	protoContractFees := make([]int64, 0, len(contractFees))
	for _, fee := range contractFees {
		if fee == 0 {
			continue
		}
		protoContractFees = append(protoContractFees, fee)
	}
	return protoContractFees
}

//func WasSkippedByArchive(tick uint32, processedTicksIntervalPerEpoch []*protobuf.ProcessedTickIntervalsPerEpoch) (bool, uint32) {
//	if len(processedTicksIntervalPerEpoch) == 0 {
//		return false, 0
//	}
//	for _, epochInterval := range processedTicksIntervalPerEpoch {
//		for _, interval := range epochInterval.Intervals {
//			if tick < interval.InitialProcessedTick {
//				return true, interval.InitialProcessedTick
//			}
//			if tick >= interval.InitialProcessedTick && tick <= interval.LastProcessedTick {
//				return false, 0
//			}
//		}
//	}
//	return false, 0
//}

//func GetTickEpoch(tickNumber uint32, intervals []*protobuf.ProcessedTickIntervalsPerEpoch) (uint32, error) {
//	if len(intervals) == 0 {
//		return 0, errors.New("processed tick interval list is empty")
//	}
//
//	for _, epochInterval := range intervals {
//		for _, interval := range epochInterval.Intervals {
//			if tickNumber >= interval.InitialProcessedTick && tickNumber <= interval.LastProcessedTick {
//				return epochInterval.Epoch, nil
//			}
//		}
//	}
//
//	return 0, errors.Errorf("unable to find the epoch for tick %d", tickNumber)
//}

//func GetProcessedTickIntervalsForEpoch(epoch uint32, intervals []*protobuf.ProcessedTickIntervalsPerEpoch) (*protobuf.ProcessedTickIntervalsPerEpoch, error) {
//	for _, interval := range intervals {
//		if interval.Epoch != epoch {
//			continue
//		}
//		return interval, nil
//	}
//
//	return nil, errors.Errorf("unable to find processed tick intervals for epoch %d", epoch)
//}

//func IsTickLastInAnyEpochInterval(tickNumber uint32, epoch uint32, intervals []*protobuf.ProcessedTickIntervalsPerEpoch) (bool, int, error) {
//	epochIntervals, err := GetProcessedTickIntervalsForEpoch(epoch, intervals)
//	if err != nil {
//		return false, -1, fmt.Errorf("getting processed tick intervals for epoch %d: %w", epoch, err)
//	}
//
//	for index, interval := range epochIntervals.Intervals {
//		if interval.LastProcessedTick == tickNumber {
//			return true, index, nil
//		}
//	}
//
//	return false, -1, nil
//}
