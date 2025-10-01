package api

import (
	"context"
	"github.com/qubic/go-archiver-v2/db"
	"github.com/qubic/go-archiver-v2/processor"
	"github.com/qubic/go-archiver-v2/protobuf"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/emptypb"
	"testing"
)

func TestArchiveServer_GetStatus(t *testing.T) {

	testDir := t.TempDir()
	databases, err := db.NewDatabasePool(testDir, 5)
	require.NoError(t, err)

	endpoints := NewArchiveServer(databases, &processor.TickStatus{}, "0.0.0.0:6666", "0.0.0.0:6667", 10)

	database, err := databases.GetOrCreateDbForEpoch(42)
	require.NoError(t, err)
	err = database.AppendProcessedTickInterval(context.Background(), 42, &protobuf.ProcessedTickInterval{
		InitialProcessedTick: 123,
		LastProcessedTick:    789,
	})
	require.NoError(t, err)

	response, err := endpoints.GetStatus(context.Background(), &emptypb.Empty{})
	require.NoError(t, err)

	require.Len(t, response.GetProcessedTickIntervalsPerEpoch(), 1)
	require.Equal(t, 42, int(response.GetProcessedTickIntervalsPerEpoch()[0].GetEpoch()))
	require.Len(t, response.GetProcessedTickIntervalsPerEpoch()[0].GetIntervals(), 1)
	require.Equal(t, 123, int(response.GetProcessedTickIntervalsPerEpoch()[0].GetIntervals()[0].GetInitialProcessedTick()))
	require.Equal(t, 789, int(response.GetProcessedTickIntervalsPerEpoch()[0].GetIntervals()[0].GetLastProcessedTick()))

}

func TestArchiveServer_GetStatus_WhenNoEpochs_ReturnEmpty(t *testing.T) {
	testDir := t.TempDir()
	databases, err := db.NewDatabasePool(testDir, 5)
	require.NoError(t, err)

	endpoints := NewArchiveServer(databases, &processor.TickStatus{}, "0.0.0.0:6666", "0.0.0.0:6667", 10)
	response, err := endpoints.GetStatus(context.Background(), &emptypb.Empty{})
	require.NoError(t, err)
	require.NotNil(t, response)
	require.Empty(t, response.GetProcessedTickIntervalsPerEpoch())
}

func TestArchiveServer_GetStatus_WhenNoIntervals_ReturnEmpty(t *testing.T) {
	testDir := t.TempDir()
	databases, err := db.NewDatabasePool(testDir, 5)
	require.NoError(t, err)

	endpoints := NewArchiveServer(databases, &processor.TickStatus{}, "0.0.0.0:6666", "0.0.0.0:6667", 10)

	_, err = databases.GetOrCreateDbForEpoch(666) // new epoch but no intervals
	require.NoError(t, err)

	response, err := endpoints.GetStatus(context.Background(), &emptypb.Empty{})
	require.NoError(t, err)
	require.NotNil(t, response)
	require.Empty(t, response.GetProcessedTickIntervalsPerEpoch())
}

func TestArchiveServer_GetStatus_WhenNoIntervalsInNewEpoch_ReturnPrevious(t *testing.T) {
	testDir := t.TempDir()
	databases, err := db.NewDatabasePool(testDir, 5)
	require.NoError(t, err)

	endpoints := NewArchiveServer(databases, &processor.TickStatus{}, "0.0.0.0:6666", "0.0.0.0:6667", 10)

	database, err := databases.GetOrCreateDbForEpoch(42)
	require.NoError(t, err)

	err = database.AppendProcessedTickInterval(context.Background(), 42, &protobuf.ProcessedTickInterval{
		InitialProcessedTick: 123,
		LastProcessedTick:    789,
	})
	require.NoError(t, err)

	_, err = databases.GetOrCreateDbForEpoch(666) // new epoch but no intervals
	require.NoError(t, err)

	response, err := endpoints.GetStatus(context.Background(), &emptypb.Empty{})
	require.NoError(t, err)
	require.Len(t, response.GetProcessedTickIntervalsPerEpoch(), 1)
	require.Equal(t, 42, int(response.GetProcessedTickIntervalsPerEpoch()[0].GetEpoch()))
}
