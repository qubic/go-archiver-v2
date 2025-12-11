package db

import (
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"

	"github.com/cockroachdb/pebble/v2"
)

type DatabasePool struct {
	storeDir  string
	stores    map[uint16]*PebbleStore
	maxEpochs int
	mu        sync.RWMutex
}

func NewDatabasePool(storageFolder string, epochCount int) (*DatabasePool, error) {
	maxEpochs := max(1, epochCount)
	stores, err := loadFromDisk(storageFolder, maxEpochs)
	if err != nil {
		return nil, fmt.Errorf("loading from disk: %w", err)
	}
	return &DatabasePool{
		storeDir:  storageFolder,
		stores:    stores,
		maxEpochs: maxEpochs,
	}, nil
}

func (dp *DatabasePool) HasDbForEpoch(epoch uint16) bool {
	dp.mu.RLock()
	defer dp.mu.RUnlock()
	_, ok := dp.stores[epoch]

	return ok
}

func (dp *DatabasePool) GetDbForEpoch(epoch uint16) (*PebbleStore, error) {
	dp.mu.RLock()
	defer dp.mu.RUnlock()

	store, ok := dp.stores[epoch]
	if !ok {
		return nil, fmt.Errorf("db for epoch %d not found", epoch)
	}

	return store, nil
}

// getEpochs returns a slice of epoch numbers for which databases are available.
func (dp *DatabasePool) getEpochs() []uint16 {
	dp.mu.RLock()
	defer dp.mu.RUnlock()

	keys := make([]uint16, len(dp.stores))
	pos := 0
	for key := range dp.stores {
		keys[pos] = key
		pos++
	}

	return keys
}

func (dp *DatabasePool) setDbForEpoch(epoch uint16, store *PebbleStore) {
	dp.mu.Lock()
	defer dp.mu.Unlock()

	dp.stores[epoch] = store
}

func (dp *DatabasePool) getDbCount() int {
	dp.mu.RLock()
	defer dp.mu.RUnlock()

	return len(dp.stores)
}

func (dp *DatabasePool) removeDbForEpoch(epoch uint16) {
	dp.mu.Lock()
	defer dp.mu.Unlock()

	store, ok := dp.stores[epoch]
	if !ok {
		return
	}
	closeEpochStore(store, epoch)
	delete(dp.stores, epoch)
}

// GetOrCreateDbForEpoch gets or creates the database for the specified epoch.
// It creates a new database, if necessary and closes old databases, if the maximum number of open dbs is exceeded.
func (dp *DatabasePool) GetOrCreateDbForEpoch(epoch uint16) (*PebbleStore, error) {
	db, err := dp.GetDbForEpoch(epoch)
	if err == nil {
		return db, nil
	}

	log.Printf("Creating database for epoch [%d].", epoch)
	newStore, err := CreateStore(dp.storeDir, epoch, true)
	if err != nil {
		return nil, fmt.Errorf("creating data store for epoch [%d]: %w", epoch, err)
	}
	dp.setDbForEpoch(epoch, newStore)

	dp.closeOldEpochStoresIfNecessary() // keep only a certain number of stores open
	return newStore, nil
}

func (dp *DatabasePool) closeOldEpochStoresIfNecessary() {
	if dp.getDbCount() <= dp.maxEpochs {
		return
	}

	epochs := dp.GetAvailableEpochsDescending() // close oldest one
	for i, epoch := range epochs {
		if i >= dp.maxEpochs {
			dp.removeDbForEpoch(epoch)
		}
	}
}

func (dp *DatabasePool) GetAvailableEpochsAscending() []uint16 {
	epochs := dp.getEpochs()
	slices.Sort(epochs)
	return epochs
}

func (dp *DatabasePool) GetAvailableEpochsDescending() []uint16 {
	epochs := dp.GetAvailableEpochsAscending()
	slices.Reverse(epochs)
	return epochs
}

func (dp *DatabasePool) Close() {
	for epoch, store := range dp.stores {
		if store != nil {
			closeEpochStore(store, epoch)
		}
	}
}

func closeEpochStore(store *PebbleStore, epoch uint16) {
	if store != nil {
		err := store.Close()
		if err != nil {
			log.Printf("[ERROR] closing data store for epoch [%d]: %v", epoch, err)
		} else {
			log.Printf("[INFO] Closed data store for epoch [%d].", epoch)
		}
	}
}

func loadFromDisk(storageDirectory string, maxEpochs int) (map[uint16]*PebbleStore, error) {
	// open database from folder (epoch name numbers only)
	libRegEx, err := regexp.Compile("^\\d{1,5}$")
	if err != nil {
		return nil, fmt.Errorf("compiling regexp: %w", err)
	}

	err = createDirIfNotExists(storageDirectory)
	if err != nil {
		return nil, fmt.Errorf("checking directory: %w", err)
	}

	files, err := os.ReadDir(storageDirectory)
	if err != nil {
		return nil, fmt.Errorf("reading directory: %w", err)
	}

	databases := make(map[uint16]*PebbleStore, len(files))

	var epochs []uint16
	for _, f := range files {

		if f.IsDir() && libRegEx.MatchString(f.Name()) && !strings.HasPrefix(f.Name(), "0") {
			log.Printf("Detected database for epoch [%s].", f.Name())
			epoch, err := strconv.ParseUint(f.Name(), 10, 16)
			if err != nil {
				return nil, fmt.Errorf("parsing epoch: %w", err)
			}
			epochs = append(epochs, uint16(epoch))
		}
	}

	slices.Sort(epochs)    // sort in ascending order
	slices.Reverse(epochs) // only use the most recent dbs
	for i, epoch := range epochs {
		if i < maxEpochs { // only open x newest epochs
			log.Printf("Loading database for epoch [%d].", epoch)
			store, err := CreateStore(storageDirectory, epoch, true)
			if err != nil {
				return nil, fmt.Errorf("creating data store for epoch [%d]: %w", epoch, err)
			}
			databases[epoch] = store
		} else {
			log.Printf("Skipping database for epoch [%d].", epoch)
		}
	}

	return databases, nil
}

func CreateStore(directory string, epoch uint16, enableListener bool) (*PebbleStore, error) {
	db, err := openDatabase(directory, epoch, enableListener)
	if err != nil {
		return nil, err
	}
	return NewPebbleStore(db), nil
}

func openDatabase(directory string, name uint16, enableListener bool) (*pebble.DB, error) {
	dbDir := filepath.Join(directory, fmt.Sprintf("%d", name))
	log.Printf("Opening database [%s].", dbDir)
	options := getDefaultPebbleOptions(enableListener)
	db, err := pebble.Open(dbDir, options)
	if err != nil {
		return nil, fmt.Errorf("opening database [%s]: %w", dbDir, err)
	}
	return db, nil
}

func createDirIfNotExists(path string) error {
	_, err := os.Stat(path)
	if errors.Is(err, os.ErrNotExist) {
		log.Printf("Directory [%s] not found. Trying to create it...", path)
		err = os.Mkdir(path, 0755)
		if err != nil {
			return err
		}
	}
	if err != nil {
		return err
	}
	return nil
}
