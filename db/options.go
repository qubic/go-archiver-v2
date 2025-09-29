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
		size256mb * 10,  // 2.5 GB
		size256mb * 20,  // 5 GB
		size256mb * 40,  // 10 GB
		size256mb * 100, // 25 GB
		size256mb * 200, // 50 GB
		size256mb * 400, // 100 GB
	}
	options.ApplyCompressionSettings(func() pebble.DBCompressionSettings {
		cs := pebble.DBCompressionSettings{Name: "QubicEpochData"}
		cs.Levels[0] = block.NoCompression
		cs.Levels[1] = block.FastestCompression
		cs.Levels[2] = block.FastestCompression
		cs.Levels[3] = block.FastCompression
		cs.Levels[4] = block.FastCompression
		cs.Levels[5] = block.BalancedCompression
		cs.Levels[6] = block.BalancedCompression
		return cs
	})
	options.CompactionConcurrencyRange = func() (lower, upper int) {
		return 1, runtime.NumCPU() - 1
	}
	options.AddEventListener(NewPebbleEventListener())
	return options
}
