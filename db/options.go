package db

import (
	"github.com/cockroachdb/pebble"
	"runtime"
)

const memTableSize = 268435456 // 256MB

func getDefaultPebbleOptions() *pebble.Options {

	l1Options := pebble.LevelOptions{
		BlockRestartInterval: 16,
		BlockSize:            4096,
		BlockSizeThreshold:   90,
		Compression:          pebble.NoCompression,
		FilterPolicy:         nil,
		FilterType:           pebble.TableFilter,
		IndexBlockSize:       4096,
		TargetFileSize:       memTableSize, // 256 MB
	}

	l2Options := pebble.LevelOptions{
		BlockRestartInterval: 16,
		BlockSize:            4096,
		BlockSizeThreshold:   90,
		Compression:          pebble.ZstdCompression,
		FilterPolicy:         nil,
		FilterType:           pebble.TableFilter,
		IndexBlockSize:       4096,
		TargetFileSize:       l1Options.TargetFileSize * 10, // 2.5 GB
	}

	l3Options := pebble.LevelOptions{
		BlockRestartInterval: 16,
		BlockSize:            4096,
		BlockSizeThreshold:   90,
		Compression:          pebble.ZstdCompression,
		FilterPolicy:         nil,
		FilterType:           pebble.TableFilter,
		IndexBlockSize:       4096,
		TargetFileSize:       l2Options.TargetFileSize * 10, // 25 GB
	}

	pebbleOptions := pebble.Options{
		Levels: []pebble.LevelOptions{l1Options, l2Options, l3Options},
		MaxConcurrentCompactions: func() int {
			return runtime.NumCPU()
		},
		MemTableSize:  memTableSize,
		EventListener: NewPebbleEventListener(),
	}

	return &pebbleOptions

}
