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
	storeDir        string
	stores          map[uint16]*PebbleStore
	maxEpochs       int
	createStoreLock sync.Mutex
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
	store := dp.stores[epoch]
	return store != nil
}

func (dp *DatabasePool) GetDbForEpoch(epoch uint16) (*PebbleStore, error) {
	store := dp.stores[epoch]
	if store == nil {
		return nil, fmt.Errorf("getting data store for epoch [%d]", epoch)
	}
	return store, nil
}

// GetOrCreateDbForEpoch gets or creates the database for the specified epoch.
// It creates a new database, if necessary and closes old databases, if the maximum number of open dbs is exceeded.
func (dp *DatabasePool) GetOrCreateDbForEpoch(epoch uint16) (*PebbleStore, error) {
	dp.createStoreLock.Lock()
	defer dp.createStoreLock.Unlock()
	store := dp.stores[epoch]
	if store == nil { // attention: this is not thread safe
		log.Printf("Creating database for epoch [%d].", epoch)
		newStore, err := CreateStore(dp.storeDir, epoch, true)
		if err != nil {
			return nil, fmt.Errorf("creating data store for epoch [%d]: %w", epoch, err)
		}
		dp.stores[epoch] = newStore
		defer dp.closeOldEpochStores() // keep only a certain number of stores open
		return newStore, nil
	}
	return store, nil
}

func (dp *DatabasePool) closeOldEpochStores() {
	if len(dp.stores) > dp.maxEpochs {
		epochs := dp.GetAvailableEpochsDescending() // close oldest one
		for i, epoch := range epochs {
			if i >= dp.maxEpochs {
				closeEpochStore(dp.stores[epoch], epoch)
				delete(dp.stores, epoch)
			}
		}
	}
}

func (dp *DatabasePool) GetAvailableEpochsAscending() []uint16 {
	keys := make([]uint16, len(dp.stores))
	pos := 0
	for key := range dp.stores {
		keys[pos] = key
		pos++
	}
	slices.Sort(keys)
	return keys
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

	slices.Sort(epochs) // sort in ascending order
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
