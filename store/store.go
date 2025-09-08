package store

import (
	"context"
	"encoding/binary"
	"github.com/cockroachdb/pebble"
	"github.com/pkg/errors"
	"github.com/qubic/go-archiver/protobuf"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
	"strconv"
)

const maxTickNumber = ^uint64(0)

var ErrNotFound = errors.New("store resource not found")

type PebbleStore struct {
	db     *pebble.DB
	logger *zap.Logger
}

func NewPebbleStore(db *pebble.DB, logger *zap.Logger) *PebbleStore {
	return &PebbleStore{db: db, logger: logger}
}

func (s *PebbleStore) GetTickData(_ context.Context, tickNumber uint32) (*protobuf.TickData, error) {
	key := tickDataKey(tickNumber)
	value, closer, err := s.db.Get(key)
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return nil, ErrNotFound
		}

		return nil, errors.Wrap(err, "getting tick data")
	}
	defer closer.Close()

	var td protobuf.TickData
	if err := proto.Unmarshal(value, &td); err != nil {
		return nil, errors.Wrap(err, "unmarshalling tick data to protobuf type")
	}

	return &td, err
}

func (s *PebbleStore) SetTickData(_ context.Context, tickNumber uint32, td *protobuf.TickData) error {
	key := tickDataKey(tickNumber)
	serialized, err := proto.Marshal(td)
	if err != nil {
		return errors.Wrap(err, "serializing td proto")
	}

	err = s.db.Set(key, serialized, pebble.Sync)
	if err != nil {
		return errors.Wrap(err, "setting tick data")
	}

	return nil
}

func (s *PebbleStore) SetQuorumTickData(_ context.Context, tickNumber uint32, qtd *protobuf.QuorumTickDataStored) error {
	key := quorumTickDataKey(tickNumber)
	serialized, err := proto.Marshal(qtd)
	if err != nil {
		return errors.Wrap(err, "serializing qtdV2 proto")
	}

	err = s.db.Set(key, serialized, pebble.Sync)
	if err != nil {
		return errors.Wrap(err, "setting quorum tick data")
	}

	return nil
}

func (s *PebbleStore) GetQuorumTickData(_ context.Context, tickNumber uint32) (*protobuf.QuorumTickDataStored, error) {
	key := quorumTickDataKey(tickNumber)
	value, closer, err := s.db.Get(key)
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return nil, ErrNotFound
		}

		return nil, errors.Wrap(err, "getting quorum tick data")
	}
	defer closer.Close()

	var qtd protobuf.QuorumTickDataStored
	if err := proto.Unmarshal(value, &qtd); err != nil {
		return nil, errors.Wrap(err, "unmarshalling qtdV2 to protobuf type")
	}

	return &qtd, err
}

func (s *PebbleStore) GetComputors(_ context.Context, epoch uint32) (*protobuf.Computors, error) {
	key := computorsKey(epoch)

	value, closer, err := s.db.Get(key)
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return nil, ErrNotFound
		}

		return nil, errors.Wrap(err, "getting quorum tick data")
	}
	defer closer.Close()

	var computors protobuf.Computors
	if err := proto.Unmarshal(value, &computors); err != nil {
		return nil, errors.Wrap(err, "unmarshalling computors to protobuf type")
	}

	return &computors, nil
}

func (s *PebbleStore) SetComputors(_ context.Context, epoch uint32, computors *protobuf.Computors) error {
	key := computorsKey(epoch)

	serialized, err := proto.Marshal(computors)
	if err != nil {
		return errors.Wrap(err, "serializing computors proto")
	}

	err = s.db.Set(key, serialized, pebble.Sync)
	if err != nil {
		return errors.Wrap(err, "setting computors")
	}

	return nil
}

func (s *PebbleStore) SetTransactions(_ context.Context, txs []*protobuf.Transaction) error {
	batch := s.db.NewBatchWithSize(len(txs))
	defer batch.Close()

	for _, tx := range txs {
		key, err := tickTxKey(tx.TxId)
		if err != nil {
			return errors.Wrapf(err, "creating tx key for id: %s", tx.TxId)
		}

		serialized, err := proto.Marshal(tx)
		if err != nil {
			return errors.Wrap(err, "serializing tx proto")
		}

		err = batch.Set(key, serialized, nil)
		if err != nil {
			return errors.Wrap(err, "getting tick data")
		}
	}

	if err := batch.Commit(pebble.Sync); err != nil {
		return errors.Wrap(err, "committing batch")
	}

	return nil
}

func (s *PebbleStore) GetTickTransactions(ctx context.Context, tickNumber uint32) ([]*protobuf.Transaction, error) {
	td, err := s.GetTickData(ctx, tickNumber)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return nil, ErrNotFound
		}

		return nil, errors.Wrap(err, "getting tick data")
	}

	txs := make([]*protobuf.Transaction, 0, len(td.TransactionIds))
	for _, txID := range td.TransactionIds {
		tx, err := s.GetTransaction(ctx, txID)
		if err != nil {
			if errors.Is(err, ErrNotFound) {
				return nil, ErrNotFound
			}

			return nil, errors.Wrapf(err, "getting tx for id: %s", txID)
		}

		txs = append(txs, tx)
	}

	return txs, nil
}

func (s *PebbleStore) GetTickTransferTransactions(ctx context.Context, tickNumber uint32) ([]*protobuf.Transaction, error) {
	td, err := s.GetTickData(ctx, tickNumber)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return nil, ErrNotFound
		}

		return nil, errors.Wrap(err, "getting tick data")
	}

	txs := make([]*protobuf.Transaction, 0, len(td.TransactionIds))
	for _, txID := range td.TransactionIds {
		tx, err := s.GetTransaction(ctx, txID)
		if err != nil {
			if errors.Is(err, ErrNotFound) {
				return nil, ErrNotFound
			}

			return nil, errors.Wrapf(err, "getting tx for id: %s", txID)
		}
		if tx.Amount <= 0 {
			continue
		}

		txs = append(txs, tx)
	}

	return txs, nil
}

func (s *PebbleStore) GetTransaction(_ context.Context, txID string) (*protobuf.Transaction, error) {
	key, err := tickTxKey(txID)
	if err != nil {
		return nil, errors.Wrap(err, "getting tx key")
	}

	value, closer, err := s.db.Get(key)
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return nil, ErrNotFound
		}

		return nil, errors.Wrap(err, "getting tx")
	}
	defer closer.Close()

	var tx protobuf.Transaction
	if err := proto.Unmarshal(value, &tx); err != nil {
		return nil, errors.Wrap(err, "unmarshalling tx to protobuf type")
	}

	return &tx, nil
}

func (s *PebbleStore) SetLastProcessedTick(ctx context.Context, lastProcessedTick *protobuf.ProcessedTick) error {
	batch := s.db.NewBatch()
	defer batch.Close()

	key := lastProcessedTickKeyPerEpoch(lastProcessedTick.Epoch)
	value := make([]byte, 4)
	binary.LittleEndian.PutUint32(value, lastProcessedTick.TickNumber)

	err := batch.Set(key, value, pebble.Sync)
	if err != nil {
		return errors.Wrap(err, "setting last processed tick")
	}

	key = lastProcessedTickKey()
	serialized, err := proto.Marshal(lastProcessedTick)
	if err != nil {
		return errors.Wrap(err, "serializing skipped tick proto")
	}

	err = batch.Set(key, serialized, pebble.Sync)
	if err != nil {
		return errors.Wrap(err, "setting last processed tick")
	}

	err = batch.Commit(pebble.Sync)
	if err != nil {
		return errors.Wrap(err, "committing batch")
	}

	ptie, err := s.getProcessedTickIntervalsPerEpoch(ctx, lastProcessedTick.Epoch)
	if err != nil {
		return errors.Wrap(err, "getting ptie")
	}

	if len(ptie.Intervals) == 0 {
		ptie = &protobuf.ProcessedTickIntervalsPerEpoch{Epoch: lastProcessedTick.Epoch, Intervals: []*protobuf.ProcessedTickInterval{{InitialProcessedTick: lastProcessedTick.TickNumber, LastProcessedTick: lastProcessedTick.TickNumber}}}
	} else {
		ptie.Intervals[len(ptie.Intervals)-1].LastProcessedTick = lastProcessedTick.TickNumber
	}

	err = s.SetProcessedTickIntervalPerEpoch(ctx, lastProcessedTick.Epoch, ptie)
	if err != nil {
		return errors.Wrap(err, "setting ptie")
	}

	return nil
}

func (s *PebbleStore) GetLastProcessedTick(_ context.Context) (*protobuf.ProcessedTick, error) {
	key := lastProcessedTickKey()
	value, closer, err := s.db.Get(key)
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return nil, ErrNotFound
		}

		return nil, errors.Wrap(err, "getting last processed tick")
	}
	defer closer.Close()

	var lpt protobuf.ProcessedTick
	if err := proto.Unmarshal(value, &lpt); err != nil {
		return nil, errors.Wrap(err, "unmarshalling lpt to protobuf type")
	}

	return &lpt, nil
}

func (s *PebbleStore) GetLastProcessedTicksPerEpoch(_ context.Context) (map[uint32]uint32, error) {
	upperBound := append([]byte{LastProcessedTickPerEpoch}, []byte(strconv.FormatUint(maxTickNumber, 10))...)
	iter, err := s.db.NewIter(&pebble.IterOptions{
		LowerBound: []byte{LastProcessedTickPerEpoch},
		UpperBound: upperBound,
	})
	if err != nil {
		return nil, errors.Wrap(err, "creating iter")
	}
	defer iter.Close()

	ticksPerEpoch := make(map[uint32]uint32)
	for iter.First(); iter.Valid(); iter.Next() {
		key := iter.Key()

		value, err := iter.ValueAndErr()
		if err != nil {
			return nil, errors.Wrap(err, "getting value from iter")
		}

		epochNumber := binary.BigEndian.Uint32(key[1:])
		tickNumber := binary.LittleEndian.Uint32(value)
		ticksPerEpoch[epochNumber] = tickNumber
	}

	return ticksPerEpoch, nil
}

func (s *PebbleStore) SetSkippedTicksInterval(ctx context.Context, skippedTick *protobuf.SkippedTicksInterval) error {
	newList := protobuf.SkippedTicksIntervalList{}
	current, err := s.GetSkippedTicksInterval(ctx)
	if err != nil {
		if !errors.Is(err, ErrNotFound) {
			return errors.Wrap(err, "getting skipped tick interval")
		}
	} else {
		newList.SkippedTicks = current.SkippedTicks
	}

	newList.SkippedTicks = append(newList.SkippedTicks, skippedTick)

	key := skippedTicksIntervalKey()
	serialized, err := proto.Marshal(&newList)
	if err != nil {
		return errors.Wrap(err, "serializing skipped tick proto")
	}

	err = s.db.Set(key, serialized, pebble.Sync)
	if err != nil {
		return errors.Wrap(err, "setting skipped tick interval")
	}

	return nil
}

func (s *PebbleStore) GetSkippedTicksInterval(_ context.Context) (*protobuf.SkippedTicksIntervalList, error) {
	key := skippedTicksIntervalKey()
	value, closer, err := s.db.Get(key)
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return nil, ErrNotFound
		}

		return nil, errors.Wrap(err, "getting skipped tick interval")
	}
	defer closer.Close()

	var skipped protobuf.SkippedTicksIntervalList
	if err := proto.Unmarshal(value, &skipped); err != nil {
		return nil, errors.Wrap(err, "unmarshalling skipped tick interval to protobuf type")
	}

	return &skipped, nil
}

func (s *PebbleStore) PutTransferTransactionsPerTick(_ context.Context, identity string, tickNumber uint32, txs *protobuf.TransferTransactionsPerTick) error {
	key := identityTransferTransactionsPerTickKey(identity, tickNumber)

	serialized, err := proto.Marshal(txs)
	if err != nil {
		return errors.Wrap(err, "serializing tx proto")
	}

	err = s.db.Set(key, serialized, pebble.Sync)
	if err != nil {
		return errors.Wrap(err, "setting transfer tx")
	}

	return nil
}

type Pageable struct {
	Page, Size uint32
}

type Sortable struct {
	Descending bool
}

type Filterable struct {
	ScOnly bool
}

func (s *PebbleStore) GetTransactionsForEntity(ctx context.Context, identity string, startTick, endTick uint64) ([]*protobuf.TransferTransactionsPerTick, error) {
	const limitForRequestWithoutPaging = 1000 // old implementation was unlimited.
	transfers, _, err := s.GetTransactionsForEntityPaged(ctx, identity, startTick, endTick,
		Pageable{Size: limitForRequestWithoutPaging},
		Sortable{},
		Filterable{},
	)
	return transfers, err
}

func (s *PebbleStore) GetTransactionsForEntityPaged(_ context.Context, identity string, startTick, endTick uint64, page Pageable, sort Sortable, filter Filterable) ([]*protobuf.TransferTransactionsPerTick, int, error) {

	var index, start, end int
	start = int(page.Page) * int(page.Size)
	end = start + int(page.Size)

	var transferTxs []*protobuf.TransferTransactionsPerTick
	transferTxs = make([]*protobuf.TransferTransactionsPerTick, 0, min(page.Size, 1000))

	partialKey := identityTransferTransactions(identity)
	iter, err := s.db.NewIter(&pebble.IterOptions{
		LowerBound: binary.BigEndian.AppendUint64(partialKey, startTick),
		UpperBound: binary.BigEndian.AppendUint64(partialKey, endTick+1),
	})
	if err != nil {
		return nil, -1, errors.Wrap(err, "creating iterator")
	}
	defer iter.Close()

	if sort.Descending {
		for iter.Last(); iter.Valid(); iter.Prev() {
			index, transferTxs, err = getTransfersPage(iter, index, transferTxs, start, end, filter)
		}
	} else {
		for iter.First(); iter.Valid(); iter.Next() { // per tick
			index, transferTxs, err = getTransfersPage(iter, index, transferTxs, start, end, filter)
		}
	}
	if err != nil {
		return nil, -1, errors.Wrap(err, "getting transfers page")
	}

	return transferTxs, index, nil
}

func getTransfersPage(iter *pebble.Iterator, index int, transferTxs []*protobuf.TransferTransactionsPerTick, pageStart int, pageEnd int, filter Filterable) (int, []*protobuf.TransferTransactionsPerTick, error) {
	value, err := iter.ValueAndErr()
	if err != nil {
		return -1, nil, errors.Wrap(err, "getting value from iter")
	}

	var perTick protobuf.TransferTransactionsPerTick
	var toBeAdded *protobuf.TransferTransactionsPerTick

	err = proto.Unmarshal(value, &perTick)
	if err != nil {
		return -1, nil, errors.Wrap(err, "unmarshalling transfer tx per tick to protobuf type")
	}

	transactions := filterTransactions(filter, &perTick)

	count := len(transactions)
	if count > 0 && index+count >= pageStart && index < pageEnd {

		startIndex := max(pageStart-index, 0) // if index < pageStart we need to skip first items
		endIndex := min(pageEnd-index, count)

		if index+count >= pageStart && endIndex > startIndex { // covers case index >= pageStart and index+count >= pageStart
			toBeAdded = &protobuf.TransferTransactionsPerTick{
				TickNumber:   perTick.GetTickNumber(),
				Identity:     perTick.GetIdentity(),
				Transactions: transactions[startIndex:endIndex],
			}
			transferTxs = append(transferTxs, toBeAdded)
		}
	}
	index += count
	return index, transferTxs, nil
}

func filterTransactions(filter Filterable, perTick *protobuf.TransferTransactionsPerTick) []*protobuf.Transaction {
	var transactions []*protobuf.Transaction
	if filter.ScOnly { // filter if necessary
		transactions = make([]*protobuf.Transaction, 0)
		for _, tx := range perTick.GetTransactions() {
			if tx.InputType != 0 {
				transactions = append(transactions, tx)
			}
		}
	} else {
		transactions = perTick.GetTransactions()
	}
	return transactions
}

func (s *PebbleStore) PutChainDigest(_ context.Context, tickNumber uint32, digest []byte) error {
	key := chainDigestKey(tickNumber)

	err := s.db.Set(key, digest, pebble.Sync)
	if err != nil {
		return errors.Wrap(err, "setting chain digest")
	}

	return nil
}

func (s *PebbleStore) GetChainDigest(_ context.Context, tickNumber uint32) ([]byte, error) {
	key := chainDigestKey(tickNumber)
	value, closer, err := s.db.Get(key)
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return nil, ErrNotFound
		}

		return nil, errors.Wrap(err, "getting chain digest")
	}
	defer closer.Close()

	return value, nil
}

func (s *PebbleStore) PutStoreDigest(_ context.Context, tickNumber uint32, digest []byte) error {
	key := storeDigestKey(tickNumber)

	err := s.db.Set(key, digest, pebble.Sync)
	if err != nil {
		return errors.Wrap(err, "setting chain digest")
	}

	return nil
}

func (s *PebbleStore) GetStoreDigest(_ context.Context, tickNumber uint32) ([]byte, error) {
	key := storeDigestKey(tickNumber)
	value, closer, err := s.db.Get(key)
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return nil, ErrNotFound
		}

		return nil, errors.Wrap(err, "getting chain digest")
	}
	defer closer.Close()

	return value, nil
}

func (s *PebbleStore) GetTickTransactionsStatus(_ context.Context, tickNumber uint64) (*protobuf.TickTransactionsStatus, error) {
	key := tickTxStatusKey(tickNumber)
	value, closer, err := s.db.Get(key)
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return nil, ErrNotFound
		}

		return nil, errors.Wrap(err, "getting transactions status")
	}
	defer closer.Close()

	var tts protobuf.TickTransactionsStatus
	if err := proto.Unmarshal(value, &tts); err != nil {
		return nil, errors.Wrap(err, "unmarshalling tick transactions status")
	}

	return &tts, err
}

func (s *PebbleStore) GetTransactionStatus(_ context.Context, txID string) (*protobuf.TransactionStatus, error) {
	key := txStatusKey(txID)
	value, closer, err := s.db.Get(key)
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return nil, ErrNotFound
		}

		return nil, errors.Wrap(err, "getting transaction status")
	}
	defer closer.Close()

	var ts protobuf.TransactionStatus
	if err := proto.Unmarshal(value, &ts); err != nil {
		return nil, errors.Wrap(err, "unmarshalling transaction status")
	}

	return &ts, err
}

func (s *PebbleStore) SetTickTransactionsStatus(_ context.Context, tickNumber uint64, tts *protobuf.TickTransactionsStatus) error {
	key := tickTxStatusKey(tickNumber)
	batch := s.db.NewBatchWithSize(len(tts.Transactions) + 1)
	defer batch.Close()

	serialized, err := proto.Marshal(tts)
	if err != nil {
		return errors.Wrap(err, "serializing tts proto")
	}

	err = batch.Set(key, serialized, pebble.Sync)
	if err != nil {
		return errors.Wrap(err, "setting tts data")
	}

	for _, tx := range tts.Transactions {
		key := txStatusKey(tx.TxId)

		serialized, err := proto.Marshal(tx)
		if err != nil {
			return errors.Wrap(err, "serializing tx status proto")
		}

		err = batch.Set(key, serialized, nil)
		if err != nil {
			return errors.Wrap(err, "setting tx status data")
		}
	}

	err = batch.Commit(pebble.Sync)
	if err != nil {
		return errors.Wrap(err, "committing batch")
	}

	return nil
}

func (s *PebbleStore) getProcessedTickIntervalsPerEpoch(_ context.Context, epoch uint32) (*protobuf.ProcessedTickIntervalsPerEpoch, error) {
	key := processedTickIntervalsPerEpochKey(epoch)
	value, closer, err := s.db.Get(key)
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return &protobuf.ProcessedTickIntervalsPerEpoch{Intervals: make([]*protobuf.ProcessedTickInterval, 0), Epoch: epoch}, nil
		}

		return nil, errors.Wrap(err, "getting processed tick intervals per epoch from store")
	}
	defer closer.Close()

	var ptie protobuf.ProcessedTickIntervalsPerEpoch
	if err := proto.Unmarshal(value, &ptie); err != nil {
		return nil, errors.Wrap(err, "unmarshalling processed tick intervals per epoch")
	}

	return &ptie, nil
}

func (s *PebbleStore) SetProcessedTickIntervalPerEpoch(_ context.Context, epoch uint32, ptie *protobuf.ProcessedTickIntervalsPerEpoch) error {
	key := processedTickIntervalsPerEpochKey(epoch)
	serialized, err := proto.Marshal(ptie)
	if err != nil {
		return errors.Wrap(err, "serializing ptie proto")
	}

	err = s.db.Set(key, serialized, pebble.Sync)
	if err != nil {
		return errors.Wrap(err, "setting ptie")
	}

	return nil
}

func (s *PebbleStore) AppendProcessedTickInterval(ctx context.Context, epoch uint32, pti *protobuf.ProcessedTickInterval) error {
	existing, err := s.getProcessedTickIntervalsPerEpoch(ctx, epoch)
	if err != nil {
		return errors.Wrap(err, "getting existing processed tick intervals")
	}

	existing.Intervals = append(existing.Intervals, pti)

	err = s.SetProcessedTickIntervalPerEpoch(ctx, epoch, existing)
	if err != nil {
		return errors.Wrap(err, "setting ptie")
	}

	return nil
}

func (s *PebbleStore) GetProcessedTickIntervals(_ context.Context) ([]*protobuf.ProcessedTickIntervalsPerEpoch, error) {
	upperBound := append([]byte{ProcessedTickIntervals}, []byte(strconv.FormatUint(maxTickNumber, 10))...)
	iter, err := s.db.NewIter(&pebble.IterOptions{
		LowerBound: []byte{ProcessedTickIntervals},
		UpperBound: upperBound,
	})
	if err != nil {
		return nil, errors.Wrap(err, "creating iter")
	}
	defer iter.Close()

	processedTickIntervals := make([]*protobuf.ProcessedTickIntervalsPerEpoch, 0)
	for iter.First(); iter.Valid(); iter.Next() {
		value, err := iter.ValueAndErr()
		if err != nil {
			return nil, errors.Wrap(err, "getting value from iter")
		}

		var ptie protobuf.ProcessedTickIntervalsPerEpoch
		err = proto.Unmarshal(value, &ptie)
		if err != nil {
			return nil, errors.Wrap(err, "unmarshalling iter ptie")
		}
		processedTickIntervals = append(processedTickIntervals, &ptie)
	}

	return processedTickIntervals, nil
}

func (s *PebbleStore) SetEmptyTicksForEpoch(epoch uint32, emptyTicksCount uint32) error {
	key := emptyTicksPerEpochKey(epoch)

	value := make([]byte, 4)
	binary.LittleEndian.PutUint32(value, emptyTicksCount)

	err := s.db.Set(key, value, pebble.Sync)
	if err != nil {
		return errors.Wrapf(err, "saving emptyTickCount for epoch %d", epoch)
	}
	return nil
}

func (s *PebbleStore) GetEmptyTicksForEpoch(epoch uint32) (uint32, error) {
	key := emptyTicksPerEpochKey(epoch)

	value, closer, err := s.db.Get(key)
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return 0, err
		}

		return 0, errors.Wrapf(err, "getting emptyTickCount for epoch %d", epoch)
	}
	defer closer.Close()

	emptyTicksCount := binary.LittleEndian.Uint32(value)

	return emptyTicksCount, nil
}

func (s *PebbleStore) GetEmptyTicksForEpochs(firstEpoch, lastEpoch uint32) (map[uint32]uint32, error) {

	iter, err := s.db.NewIter(&pebble.IterOptions{
		LowerBound: emptyTicksPerEpochKey(firstEpoch),
		UpperBound: emptyTicksPerEpochKey(lastEpoch + 1), // Increment as upper bound is exclusive
	})
	if err != nil {
		return nil, errors.Wrap(err, "creating iter")
	}
	defer iter.Close()

	emptyTickMap := make(map[uint32]uint32)

	for iter.First(); iter.Valid(); iter.Next() {

		value, err := iter.ValueAndErr()
		if err != nil {
			return nil, errors.Wrap(err, "getting value from iter")
		}

		key := iter.Key()
		epochNumber := binary.BigEndian.Uint64(key[1:])
		emptyTicksCount := binary.LittleEndian.Uint32(value)

		emptyTickMap[uint32(epochNumber)] = emptyTicksCount
	}

	return emptyTickMap, nil
}

func (s *PebbleStore) DeleteEmptyTicksKeyForEpoch(epoch uint32) error {
	key := emptyTicksPerEpochKey(epoch)

	err := s.db.Delete(key, pebble.Sync)
	if err != nil {
		return errors.Wrapf(err, "deleting empty ticks key for epoch %d", epoch)
	}
	return nil
}

func (s *PebbleStore) SetLastTickQuorumDataPerEpochIntervals(epoch uint32, lastQuorumDataPerEpochIntervals *protobuf.LastTickQuorumDataPerEpochIntervals) error {

	key := lastTickQuorumDataPerEpochIntervalKey(epoch)

	value, err := proto.Marshal(lastQuorumDataPerEpochIntervals)
	if err != nil {
		return errors.Wrapf(err, "serializing last quorum data per epoch intervals for epoch %d", epoch)
	}

	err = s.db.Set(key, value, pebble.Sync)
	if err != nil {
		return errors.Wrapf(err, "setting last quorum data per epoch intervals for epoch %d", epoch)
	}
	return nil
}

func (s *PebbleStore) GetLastTickQuorumDataListPerEpochInterval(epoch uint32) (*protobuf.LastTickQuorumDataPerEpochIntervals, error) {
	key := lastTickQuorumDataPerEpochIntervalKey(epoch)

	value, closer, err := s.db.Get(key)
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return &protobuf.LastTickQuorumDataPerEpochIntervals{
				QuorumDataPerInterval: make(map[int32]*protobuf.QuorumTickData),
			}, nil
		}
		return nil, errors.Wrapf(err, "getting quorum data list for the intervals of epoch %d", epoch)
	}
	defer closer.Close()

	var lastQuorumDataPerEpochIntervals protobuf.LastTickQuorumDataPerEpochIntervals
	err = proto.Unmarshal(value, &lastQuorumDataPerEpochIntervals)
	if err != nil {
		return nil, errors.Wrapf(err, "de-serializing last quorum data per epoch intervals for epoch %d", epoch)
	}

	return &lastQuorumDataPerEpochIntervals, err
}

func (s *PebbleStore) SetQuorumDataForCurrentEpochInterval(epoch uint32, quorumData *protobuf.QuorumTickData) error {

	processedIntervals, err := s.getProcessedTickIntervalsPerEpoch(nil, epoch)
	if err != nil {
		return errors.Wrapf(err, "getting processed tick intervals for epoch %d", epoch)
	}

	intervalIndex := len(processedIntervals.Intervals) - 1
	if intervalIndex < 0 {
		intervalIndex = 0
	}

	quorumDataPerIntervals, err := s.GetLastTickQuorumDataListPerEpochInterval(epoch)
	if err != nil {
		return errors.Wrap(err, "getting last quorum data list for epoch intervals")
	}

	quorumDataPerIntervals.QuorumDataPerInterval[int32(intervalIndex)] = quorumData

	err = s.SetLastTickQuorumDataPerEpochIntervals(epoch, quorumDataPerIntervals)
	if err != nil {
		return errors.Wrap(err, "setting last quorum data list for epoch intervals")
	}

	return nil
}

func (s *PebbleStore) SetEmptyTickListPerEpoch(epoch uint32, emptyTicks []uint32) error {
	key := emptyTickListPerEpochKey(epoch)

	value := make([]byte, len(emptyTicks)*4)
	for index, tickNumber := range emptyTicks {
		binary.LittleEndian.PutUint32(value[index*4:index*4+4], tickNumber)
	}

	err := s.db.Set(key, value, pebble.Sync)
	if err != nil {
		return errors.Wrapf(err, "saving empty tick list for epoch %d", epoch)
	}
	return nil
}

func (s *PebbleStore) GetEmptyTickListPerEpoch(epoch uint32) ([]uint32, error) {
	key := emptyTickListPerEpochKey(epoch)

	value, closer, err := s.db.Get(key)
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return nil, err
		}

		return nil, errors.Wrapf(err, "getting empty tick list for epoch %d", epoch)
	}
	defer closer.Close()

	if len(value)%4 != 0 {
		return nil, errors.Errorf("corrupted empty tick list for epoch %d. array length mod 4 != 0. length: %d", epoch, len(value))
	}

	var emptyTicks []uint32

	for index := 0; index < (len(value) / 4); index++ {
		tickNumber := binary.LittleEndian.Uint32(value[index*4 : index*4+4])
		emptyTicks = append(emptyTicks, tickNumber)
	}

	return emptyTicks, nil

}

func (s *PebbleStore) AppendEmptyTickToEmptyTickListPerEpoch(epoch uint32, tickNumber uint32) error {

	emptyTicks, err := s.GetEmptyTickListPerEpoch(epoch)
	if err != nil {
		return errors.Wrapf(err, "getting empty tick list for epoch %d", epoch)
	}

	emptyTicks = append(emptyTicks, tickNumber)

	err = s.SetEmptyTickListPerEpoch(epoch, emptyTicks)
	if err != nil {
		return errors.Wrapf(err, "saving appended empty tick list")
	}

	return nil
}

func (s *PebbleStore) DeleteEmptyTickListKeyForEpoch(epoch uint32) error {
	key := emptyTickListPerEpochKey(epoch)

	err := s.db.Delete(key, pebble.Sync)
	if err != nil {
		return errors.Wrapf(err, "deleting empty tick list key for epoch %d", epoch)
	}
	return nil
}

func (s *PebbleStore) SetTargetTickVoteSignature(epoch, value uint32) error {
	key := targetTickVoteSignatureKey(epoch)

	data := make([]byte, 4)
	binary.LittleEndian.PutUint32(data, value)

	err := s.db.Set(key, data, pebble.Sync)
	if err != nil {
		return errors.Wrapf(err, "saving target tick vote signature for epoch %d", epoch)
	}
	return nil
}

func (s *PebbleStore) GetTargetTickVoteSignature(epoch uint32) (uint32, error) {
	key := targetTickVoteSignatureKey(epoch)

	value, closer, err := s.db.Get(key)
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return 0, ErrNotFound
		}
		return 0, errors.Wrap(err, "getting target tick vote signature")
	}
	defer closer.Close()

	return binary.LittleEndian.Uint32(value), nil
}
