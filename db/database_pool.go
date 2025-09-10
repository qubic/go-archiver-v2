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
	stores   map[uint32]*pebble.DB // FIXME replace with pebble store
}

func NewDatabasePool(storageFolder string) (*DatabasePool, error) {
	stores, err := loadFromDisk(storageFolder)
	if err != nil {
		return nil, err
	}
	return &DatabasePool{
		storeDir: storageFolder,
		stores:   stores,
	}, nil
}

func (dp *DatabasePool) GetDbForEpoch(epoch uint32) (*pebble.DB, error) {
	store := dp.stores[epoch]
	if store == nil {
		return nil, fmt.Errorf("there is no database for epoch [%d]", epoch)
	}
	return store, nil
}

func (dp *DatabasePool) GetOrCreateDbForEpoch(epoch uint32) (*pebble.DB, error) {
	store := dp.stores[epoch]
	if store == nil {
		log.Printf("creating database for epoch [%d]", epoch)
		newStore, err := openDatabase(dp.storeDir, epoch)
		if err != nil {
			return nil, fmt.Errorf("failed to create database for epoch [%d]: %w", epoch, err)
		}
		dp.stores[epoch] = newStore
		return newStore, nil
	}
	return store, nil
}

func (dp *DatabasePool) Close() {
	for epoch, db := range dp.stores {
		if db != nil {
			err := db.Close()
			if err != nil {
				log.Printf("[ERROR] failed to close database [%d]: %v", epoch, err)
			}
		}
	}
}

func openDatabase(directory string, name uint32) (*pebble.DB, error) {
	dbDir := fmt.Sprintf("%s/%d", directory, name)
	log.Printf("opening database [%s]", dbDir)
	db, err := pebble.Open(dbDir, getDefaultPebbleOptions())
	if err != nil {
		return nil, fmt.Errorf("failed to open database [%s]: %w", dbDir, err)
	}
	return db, nil
}

func loadFromDisk(directory string) (map[uint32]*pebble.DB, error) {
	// open database from folder (epoch name numbers only)
	libRegEx, err := regexp.Compile("^\\d{1,5}$")
	if err != nil {
		return nil, fmt.Errorf("failed to compile regexp: %w", err)
	}

	err = createDirIfNotExists(directory)
	if err != nil {
		return nil, fmt.Errorf("failed to check directory: %w", err)
	}

	files, err := os.ReadDir(directory)
	if err != nil {
		return nil, fmt.Errorf("failed to read directory: %w", err)
	}

	databases := make(map[uint32]*pebble.DB, len(files))

	for _, f := range files {

		if f.IsDir() && libRegEx.MatchString(f.Name()) && !strings.HasPrefix(f.Name(), "0") {

			log.Printf("loading database for epoch [%s]", f.Name())
			epoch, err := strconv.ParseUint(f.Name(), 10, 32)
			if err != nil {
				return nil, fmt.Errorf("failed to parse epoch: %w", err)
			}

			db, err := openDatabase(directory, uint32(epoch))
			databases[uint32(epoch)] = db
		}

	}

	return databases, nil
}

//func (dp *DatabasePool) getDbKeys() []uint32 {
//	keys := make([]uint32, len(dp.stores))
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
