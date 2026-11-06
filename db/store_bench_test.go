package db

import (
	"fmt"
	"os"
	"testing"

	"github.com/cockroachdb/pebble/v2/vfs"
)

// BenchmarkPebbleStore_storeTick measures the store phase of one processed tick (all the
// writes the processor issues, ending with the processed-tick commit), reporting WAL
// fsyncs per tick next to the time.
//
// fsync is what dominates, so run it against a real disk: by default the data lives in
// b.TempDir(), which is often tmpfs where fsync is free. Point ARCHIVER_BENCH_DIR at a
// directory on the disk you want to measure, e.g.
//
//	ARCHIVER_BENCH_DIR=$HOME/.cache/archiver-bench go test ./db -run '^$' -bench storeTick -benchtime 300x
func BenchmarkPebbleStore_storeTick(b *testing.B) {
	for _, nrTxs := range []int{0, 10, 100} {
		b.Run(fmt.Sprintf("txs=%d", nrTxs), func(b *testing.B) {
			dir := b.TempDir()
			if base := os.Getenv("ARCHIVER_BENCH_DIR"); base != "" {
				var err error
				if err = os.MkdirAll(base, 0755); err == nil {
					dir, err = os.MkdirTemp(base, "bench-")
				}
				if err != nil {
					b.Fatal(err)
				}
				b.Cleanup(func() { _ = os.RemoveAll(dir) })
			}

			fs := &walSyncCountingFS{FS: vfs.Default}
			store := openStoreWithFS(b, fs, dir)
			defer closeDb(store)

			const epoch = 42
			tick := uint32(1_000_000)
			storeTick(b, store, epoch, tick, nrTxs, true) // warm-up
			before := fs.syncs.Load()

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				tick++
				storeTick(b, store, epoch, tick, nrTxs, true)
			}
			b.StopTimer()
			b.ReportMetric(float64(fs.syncs.Load()-before)/float64(b.N), "fsyncs/tick")
		})
	}
}
