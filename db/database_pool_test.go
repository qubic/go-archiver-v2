package db

import (
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDatabasePool_NewDatabasePool_GivenEmptyDir_DoNotFail(t *testing.T) {
	testDir := t.TempDir()
	databases, err := NewDatabasePool(testDir, 5)
	require.NoError(t, err)
	assert.Len(t, databases.stores, 0)
}

func TestDatabasePool_loadFromDisk_GivenNewDir_CreateIt(t *testing.T) {
	testDir := fmt.Sprintf("%s/%s", t.TempDir(), "foo")
	databases, err := loadFromDisk(testDir, 5)
	require.NoError(t, err)
	assert.Len(t, databases, 0)
	require.DirExists(t, testDir)
}

func TestDatabasePool_loadFromDisk_GivenEmpty_DoNotFail(t *testing.T) {
	testDir := t.TempDir()
	databases, err := loadFromDisk(testDir, 5)
	require.NoError(t, err)
	assert.Len(t, databases, 0)
}

func TestDatabasePool_loadFromDisk_GivenDatabases_LoadDatabases(t *testing.T) {
	testDir := t.TempDir()
	err := os.Mkdir(fmt.Sprintf("%s/%s", testDir, "4711"), 0755)
	require.NoError(t, err)
	err = os.Mkdir(fmt.Sprintf("%s/%s", testDir, "42"), 0755)
	require.NoError(t, err)
	err = os.Mkdir(fmt.Sprintf("%s/%s", testDir, "65535"), 0755)
	require.NoError(t, err)
	err = os.Mkdir(fmt.Sprintf("%s/%s", testDir, "0815"), 0755) // will be ignored (starting with zero)
	require.NoError(t, err)
	err = os.Mkdir(fmt.Sprintf("%s/%s", testDir, "47110815"), 0755) // will be ignored (number too big)
	require.NoError(t, err)
	err = os.Mkdir(fmt.Sprintf("%s/%s", testDir, "1"), 0755) // will be ignored because it's the fourth db
	require.NoError(t, err)

	databases, err := loadFromDisk(testDir, 3)
	require.NoError(t, err)
	assert.Len(t, databases, 3)
	require.DirExists(t, fmt.Sprintf("%s/%s", testDir, "4711"))
	require.DirExists(t, fmt.Sprintf("%s/%s", testDir, "42"))
	require.DirExists(t, fmt.Sprintf("%s/%s", testDir, "65535"))
}

func TestDatabasePool_GetAvailableEpochs(t *testing.T) {
	testDir := t.TempDir()
	err := os.Mkdir(fmt.Sprintf("%s/%s", testDir, "42"), 0755)
	require.NoError(t, err)
	err = os.Mkdir(fmt.Sprintf("%s/%s", testDir, "43"), 0755)
	require.NoError(t, err)
	err = os.Mkdir(fmt.Sprintf("%s/%s", testDir, "45"), 0755)
	require.NoError(t, err)
	databases, err := NewDatabasePool(testDir, 5)
	require.NoError(t, err)

	epochs := databases.GetAvailableEpochsDescending()
	require.Len(t, epochs, 3)
	require.Equal(t, 45, int(epochs[0]))
	require.Equal(t, 43, int(epochs[1]))
	require.Equal(t, 42, int(epochs[2]))
}

func TestDatabasePool_GetDbForEpoch_ReturnsDbForEpoch(t *testing.T) {
	testDir := t.TempDir()
	err := os.Mkdir(fmt.Sprintf("%s/%s", testDir, "1"), 0755)
	require.NoError(t, err)

	dbPool, err := NewDatabasePool(testDir, 5)
	require.NoError(t, err)

	db1, err := dbPool.GetDbForEpoch(1)
	require.NoError(t, err)
	require.NotNil(t, db1)

	_, err = dbPool.GetDbForEpoch(2)
	require.Error(t, err)
}

func TestDatabasePool_GetOrCreateDbForEpoch_ReturnsDbForEpoch(t *testing.T) {
	testDir := t.TempDir()
	err := os.Mkdir(fmt.Sprintf("%s/%s", testDir, "1"), 0755)
	require.NoError(t, err)

	dbPool, err := NewDatabasePool(testDir, 5)
	require.NoError(t, err)

	require.DirExists(t, fmt.Sprintf("%s/%s", testDir, "1"))
	db1, err := dbPool.GetOrCreateDbForEpoch(1)
	require.NoError(t, err)
	require.NotNil(t, db1)

	require.NoDirExists(t, fmt.Sprintf("%s/%s", testDir, "2"))
	db2, err := dbPool.GetOrCreateDbForEpoch(2)
	require.NoError(t, err)
	require.NotNil(t, db2)
	require.DirExists(t, fmt.Sprintf("%s/%s", testDir, "2")) // created

	require.NotEqual(t, db1, db2)
}
