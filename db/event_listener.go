package db

import (
	"log"

	"github.com/cockroachdb/pebble/v2"
)

type PebbleEventListener struct {
	pebble.EventListener
}

func NewPebbleEventListener() pebble.EventListener {

	listener := pebble.EventListener{}
	listener.BackgroundError = backgroundError
	listener.CompactionBegin = compactionBegin
	listener.CompactionEnd = compactionEnd
	listener.FlushBegin = flushBegin
	listener.FlushEnd = flushEnd
	listener.WriteStallBegin = writeStallBegin
	listener.WriteStallEnd = writeStallEnd

	return listener
}

func backgroundError(err error) {

	log.Printf("[PEBBLE]: Encountered background error: %v", err)

}

func compactionBegin(info pebble.CompactionInfo) {

	log.Printf("[PEBBLE]: Compaction triggered. JobID: %d. Reason: %s. ", info.JobID, info.Reason)
	for _, level := range info.Input {
		log.Printf("  From Level %d - %s", level.Level, level.String())
	}
	log.Printf("  To level %d %ss", info.Output.Level, info.Output.String())

}

func compactionEnd(info pebble.CompactionInfo) {
	log.Printf("[PEBBLE]: Compaction with JobID %d ended. Took %v.", info.JobID, info.TotalDuration)
}

func flushBegin(info pebble.FlushInfo) {
	log.Printf("[PEBBLE]: Flush triggered. JobID: %d. Reason: %s.", info.JobID, info.Reason)
}

func flushEnd(info pebble.FlushInfo) {
	log.Printf("[PEBBLE]: Flush with JobID %d ended. Took %v.", info.JobID, info.TotalDuration)
}

func writeStallBegin(info pebble.WriteStallBeginInfo) {
	log.Printf("[PEBBLE]: Writes stalled. Reason: %s.", info.Reason)
}

func writeStallEnd() {
	log.Printf("[PEBBLE]: Writes resumed.")
}
