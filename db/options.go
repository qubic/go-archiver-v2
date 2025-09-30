package db

import (
	"github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/sstable/block"
	"runtime"
)

const size256mb = 268435456 // 256MB

func getDefaultPebbleOptions() *pebble.Options {
	options := pebble.DefaultOptions()
	options.WithFSDefaults()
	options.TargetFileSizes = [7]int64{
		size256mb,
		size256mb * 4,   // 1 GB
		size256mb * 8,   // 2 GB
		size256mb * 16,  // 4 GB
		size256mb * 32,  // 8 GB
		size256mb * 64,  // 16 GB
		size256mb * 128, // 32 GB
	}
	options.ApplyCompressionSettings(func() pebble.DBCompressionSettings {
		cs := pebble.DBCompressionSettings{Name: "QubicEpochData"}
		cs.Levels[0] = block.NoCompression
		cs.Levels[1] = block.FastestCompression
		cs.Levels[2] = block.FastestCompression
		cs.Levels[3] = block.FastestCompression
		cs.Levels[4] = block.FastestCompression
		cs.Levels[5] = block.FastCompression
		cs.Levels[6] = block.BalancedCompression
		return cs
	})
	options.CompactionConcurrencyRange = func() (lower, upper int) {
		return 1, runtime.NumCPU() - 1
	}
	options.AddEventListener(NewPebbleEventListener())
	return options
}
