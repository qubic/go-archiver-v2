package network

import (
	"fmt"

	qubic "github.com/qubic/go-node-connector"
)

type QubicClientPool interface {
	Get() (QubicClient, error)
	Put(c QubicClient) error
	Close(c QubicClient) error
}

// NodeConnectorPool QubicClientPool implementation that wraps the go-node-connector implementation.
type NodeConnectorPool struct {
	pool *qubic.Pool
}

// NewNodeConnectorPool Creates a new client pool using the go-node-connector library.
func NewNodeConnectorPool(config qubic.PoolConfig) (*NodeConnectorPool, error) {
	clientPool, err := qubic.NewPoolConnection(config)
	if err != nil {
		return nil, fmt.Errorf("creating new pool connection: %w", err)
	}
	return &NodeConnectorPool{pool: clientPool}, nil
}

func (n *NodeConnectorPool) Get() (QubicClient, error) {
	return n.pool.Get()
}

func (n *NodeConnectorPool) Put(client QubicClient) error {
	switch c := client.(type) {
	case *qubic.Client:
		return n.pool.Put(c)
	default:
		return fmt.Errorf("invalid client type: %T", c)
	}
}

func (n *NodeConnectorPool) Close(client QubicClient) error {
	switch c := client.(type) {
	case *qubic.Client:
		return n.pool.Close(c)
	default:
		return fmt.Errorf("invalid client type: %T", c)
	}
}
