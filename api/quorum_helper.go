package api

import (
	"fmt"
	"github.com/qubic/go-archiver-v2/db"
	"github.com/qubic/go-archiver-v2/protobuf"
)

// tryToGetLastTickQuorumData last quorum data for every interval is stored separately and 'uncompressed'. Safe access except for current epoch.
func tryToGetLastTickQuorumData(database *db.PebbleStore, intervalIndex int, tick uint32, epoch uint16) (*protobuf.QuorumTickData, error) {
	quorumDataList, err := database.GetLastTickQuorumDataListPerEpochInterval(uint32(epoch))
	if err != nil {
		return nil, fmt.Errorf("getting last tick quorum data of epoch [%d]: %v", epoch, err)
	}
	if len(quorumDataList.QuorumDataPerInterval) > intervalIndex {
		lastDataForCurrentInterval := quorumDataList.QuorumDataPerInterval[int32(intervalIndex)]
		if lastDataForCurrentInterval == nil || lastDataForCurrentInterval.GetQuorumTickStructure() == nil {
			return nil, fmt.Errorf("no quorum data for epoch [%d], index [%d]", epoch, intervalIndex)
		}

		quorumDataTick := lastDataForCurrentInterval.GetQuorumTickStructure().GetTickNumber()
		if quorumDataTick != tick { // for the current epoch it could be that the last stored quorum data already changed
			return nil, fmt.Errorf("quorum data tick does not match. wanted [%d], got [%d]", tick, quorumDataTick)
		}

		return lastDataForCurrentInterval, nil
	}
	return nil, fmt.Errorf("index [%d] out of range for epoch [%d] intervals", intervalIndex, epoch)
}
