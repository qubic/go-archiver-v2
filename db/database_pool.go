package db

import (
	"errors"
	"fmt"
	"github.com/cockroachdb/pebble"
	"log"
	"os"
	"regexp"
	"strconv"
	"strings"
)

type DatabasePool struct {
	storeDir string
	stores   map[uint16]*PebbleStore
}

func NewDatabasePool(storageFolder string) (*DatabasePool, error) {
	stores, err := loadFromDisk(storageFolder)
	if err != nil {
		return nil, fmt.Errorf("loading from disk: %w", err)
	}
	return &DatabasePool{
		storeDir: storageFolder,
		stores:   stores,
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
		newStore, err := createStore(dp.storeDir, epoch)
		if err != nil {
			return nil, fmt.Errorf("creating data store for epoch [%d]: %w", epoch, err)
		}
		dp.stores[epoch] = newStore
		return newStore, nil
	}
	return store, nil
}

func (dp *DatabasePool) Close() {
	for epoch, store := range dp.stores {
		if store != nil {
			err := store.Close()
			if err != nil {
				log.Printf("[ERROR] closing data store for epoch [%d]: %v", epoch, err)
			}
		}
	}
}

func loadFromDisk(directory string) (map[uint16]*PebbleStore, error) {
	// open database from folder (epoch name numbers only)
	libRegEx, err := regexp.Compile("^\\d{1,5}$")
	if err != nil {
		return nil, fmt.Errorf("compiling regexp: %w", err)
	}

	err = createDirIfNotExists(directory)
	if err != nil {
		return nil, fmt.Errorf("checking directory: %w", err)
	}

	files, err := os.ReadDir(directory)
	if err != nil {
		return nil, fmt.Errorf("reading directory: %w", err)
	}

	databases := make(map[uint16]*PebbleStore, len(files))

	for _, f := range files {

		if f.IsDir() && libRegEx.MatchString(f.Name()) && !strings.HasPrefix(f.Name(), "0") {

			log.Printf("Loading database for epoch [%s]", f.Name())
			epoch, err := strconv.ParseUint(f.Name(), 10, 16)
			if err != nil {
				return nil, fmt.Errorf("parsing epoch: %w", err)
			}

			store, err := createStore(directory, uint16(epoch))
			if err != nil {
				return nil, fmt.Errorf("creating data store for epoch [%d]: %w", epoch, err)
			}
			databases[uint16(epoch)] = store
		}

	}

	return databases, nil
}

func createStore(directory string, epoch uint16) (*PebbleStore, error) {
	db, err := openDatabase(directory, epoch)
	if err != nil {
		return nil, err
	}
	return NewPebbleStore(db), nil
}

func openDatabase(directory string, name uint16) (*pebble.DB, error) {
	dbDir := fmt.Sprintf("%s/%d", directory, name)
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
