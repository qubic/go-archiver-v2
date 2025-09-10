package processor

import (
	"github.com/qubic/go-archiver/db"
	qubic "github.com/qubic/go-node-connector"
	"log"
	"time"
)

type Processor struct {
	nodePool           *qubic.Pool
	dbPool             *db.DatabasePool
	arbitratorPubKey   [32]byte
	processTickTimeout time.Duration
	disableStatusAddon bool
}

func NewProcessor(nodePool *qubic.Pool, dbPool *db.DatabasePool, processTickTimeout time.Duration, arbitratorPubKey [32]byte, disableStatusAddon bool) *Processor {
	return &Processor{
		nodePool:           nodePool,
		dbPool:             dbPool,
		processTickTimeout: processTickTimeout,
		arbitratorPubKey:   arbitratorPubKey,
		disableStatusAddon: disableStatusAddon,
	}
}

func (p *Processor) Start() error {
	for {
		log.Println("Processing ...")
		time.Sleep(10 * time.Second)
	}
}
