package db

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"os"
	"testing"
)

func Test_NewDatabasePool_GivenEmptyDir_DoNotFail(t *testing.T) {
	testDir := t.TempDir()
	databases, err := NewDatabasePool(testDir)
	require.NoError(t, err)
	assert.Len(t, databases.stores, 0)
}

func Test_loadFromDisk_GivenNewDir_CreateIt(t *testing.T) {
	testDir := fmt.Sprintf("%s/%s", t.TempDir(), "foo")
	databases, err := loadFromDisk(testDir)
	require.NoError(t, err)
	assert.Len(t, databases, 0)
	require.DirExists(t, testDir)
}

func Test_loadFromDisk_GivenEmpty_DoNotFail(t *testing.T) {
	testDir := t.TempDir()
	databases, err := loadFromDisk(testDir)
	require.NoError(t, err)
	assert.Len(t, databases, 0)
}

func Test_loadFromDisk_GivenDatabases_LoadDatabases(t *testing.T) {
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

	databases, err := loadFromDisk(testDir)
	require.NoError(t, err)
	assert.Len(t, databases, 3)
	require.DirExists(t, fmt.Sprintf("%s/%s", testDir, "4711"))
	require.DirExists(t, fmt.Sprintf("%s/%s", testDir, "42"))
	require.DirExists(t, fmt.Sprintf("%s/%s", testDir, "65535"))
}

func Test_GetDbForEpoch_ReturnsDbForEpoch(t *testing.T) {
	testDir := t.TempDir()
	err := os.Mkdir(fmt.Sprintf("%s/%s", testDir, "1"), 0755)
	require.NoError(t, err)

	dbPool, err := NewDatabasePool(testDir)
	require.NoError(t, err)

	db1, err := dbPool.GetDbForEpoch(1)
	require.NoError(t, err)
	require.NotNil(t, db1)

	_, err = dbPool.GetDbForEpoch(2)
	require.Error(t, err)
}

func Test_GetOrCreateDbForEpoch_ReturnsDbForEpoch(t *testing.T) {
	testDir := t.TempDir()
	err := os.Mkdir(fmt.Sprintf("%s/%s", testDir, "1"), 0755)
	require.NoError(t, err)

	dbPool, err := NewDatabasePool(testDir)
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
