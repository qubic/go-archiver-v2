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

	"github.com/cockroachdb/pebble"
)

type DatabasePool struct {
	storeDir  string
	stores    map[uint16]*PebbleStore
	maxEpochs int
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

func (dp *DatabasePool) GetDbForEpoch(epoch uint16) (*PebbleStore, error) {
	store := dp.stores[epoch]
	if store == nil {
		return nil, fmt.Errorf("getting data store for epoch [%d]", epoch)
	}
	return store, nil
}

func (dp *DatabasePool) GetOrCreateDbForEpoch(epoch uint16) (*PebbleStore, error) {
	store := dp.stores[epoch]
	if store == nil {
		log.Printf("Creating database for epoch [%d].", epoch)
		newStore, err := CreateStore(dp.storeDir, epoch)
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
	epochs := dp.GetAvailableEpochsDescending()
	for i, epoch := range epochs {
		if i >= dp.maxEpochs {
			closeEpochStore(dp.stores[epoch], epoch)
			dp.stores[epoch] = nil
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
			log.Printf("[INFO] closed data store for epoch [%d]", epoch)
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

			log.Printf("Loading database for epoch [%s]", f.Name())
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
			store, err := CreateStore(storageDirectory, epoch)
			if err != nil {
				return nil, fmt.Errorf("creating data store for epoch [%d]: %w", epoch, err)
			}
			databases[epoch] = store
		}
	}

	return databases, nil
}

func CreateStore(directory string, epoch uint16) (*PebbleStore, error) {
	db, err := openDatabase(directory, epoch)
	if err != nil {
		return nil, err
	}
	return NewPebbleStore(db), nil
}

func openDatabase(directory string, name uint16) (*pebble.DB, error) {
	dbDir := filepath.Join(directory, fmt.Sprintf("%d", name))
	log.Printf("Opening database [%s].", dbDir)
	db, err := pebble.Open(dbDir, getDefaultPebbleOptions())
	if err != nil {
		return nil, fmt.Errorf("opening database [%s]: %w", dbDir, err)
	}
	return db, nil
}

//func (dp *DatabasePool) getDbKeys() []uint16 {
//	keys := make([]uint16, len(dp.stores))
//	i := 0
//	for k, v := range dp.stores {
//		if v == nil {
//			log.Printf("[WARN] database for epoch [%d] not found.", k)
//		}
//		keys[i] = k
//		i++
//	}
//
//	return keys
//}

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
