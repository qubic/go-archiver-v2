package processor

import (
	"github.com/qubic/go-archiver/store"
	qubic "github.com/qubic/go-node-connector"
	"log"
	"time"
)

type Processor struct {
	pool               *qubic.Pool
	ps                 *store.PebbleStore
	arbitratorPubKey   [32]byte
	processTickTimeout time.Duration
	disableStatusAddon bool
}

func NewProcessor(p *qubic.Pool, ps *store.PebbleStore, processTickTimeout time.Duration, arbitratorPubKey [32]byte, disableStatusAddon bool) *Processor {
	return &Processor{
		pool:               p,
		ps:                 ps,
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
