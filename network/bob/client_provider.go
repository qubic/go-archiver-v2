package bob

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net/http"
	"sync"
	"time"
)

type ClientProvider struct {
	httpClient *http.Client

	fetcherUrl string

	bobHttpProtocol string
	bobHttpPort     string

	bobAddresses      []string
	bobAddressesMutex sync.RWMutex

	updateInterval time.Duration
}

func NewProvider(fetcherUrl, bobHttpProtocol, bobHttpPort string, updateInterval time.Duration) *ClientProvider {

	return &ClientProvider{
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
		},
		fetcherUrl:        fetcherUrl,
		bobHttpProtocol:   bobHttpProtocol,
		bobHttpPort:       bobHttpPort,
		bobAddresses:      make([]string, 0),
		bobAddressesMutex: sync.RWMutex{},
		updateInterval:    updateInterval,
	}
}

func (p *ClientProvider) Start(stop chan interface{}) {
	err := p.updateBobAddresses()
	if err != nil {
		log.Printf("Failed to find bob instances: %s", err)
	}

	go func() {
		ticker := time.NewTicker(p.updateInterval)
		defer ticker.Stop()
		for {
			select {
			case <-stop:
				log.Printf("Stopping bob client provider.")
				return
			case <-ticker.C:
				err := p.updateBobAddresses()
				if err != nil {
					log.Printf("Failed to find bob instances: %s", err)
				}
			}
		}
	}()
}

func (p *ClientProvider) fetchReliableBobNodes() (bobFetcherStatusResponse, error) {

	req, err := http.NewRequest(http.MethodGet, p.fetcherUrl, nil)
	if err != nil {
		return bobFetcherStatusResponse{}, fmt.Errorf("creating request: %w", err)
	}

	res, err := p.httpClient.Do(req)
	if err != nil {
		return bobFetcherStatusResponse{}, fmt.Errorf("performing request: %w", err)
	}
	defer res.Body.Close()

	body, err := io.ReadAll(res.Body)
	if err != nil {
		return bobFetcherStatusResponse{}, fmt.Errorf("reading response: %w", err)
	}

	if res.StatusCode != http.StatusOK {
		return bobFetcherStatusResponse{}, fmt.Errorf("response status not OK (%d): %s", res.StatusCode, body)
	}

	var bobFetcherResponse bobFetcherStatusResponse
	err = json.Unmarshal(body, &bobFetcherResponse)
	if err != nil {
		return bobFetcherStatusResponse{}, fmt.Errorf("unmarshalling response: %w", err)
	}

	return bobFetcherResponse, nil
}

func (p *ClientProvider) updateBobAddresses() error {

	reliableBobNodesStatus, err := p.fetchReliableBobNodes()
	if err != nil {
		p.bobAddressesMutex.Lock()
		p.bobAddresses = make([]string, 0)
		p.bobAddressesMutex.Unlock()
		return fmt.Errorf("fetching reliable bob nodes: %w", err)
	}

	addresses := make([]string, 0, len(reliableBobNodesStatus.ReliableNodes))
	for _, bobNode := range reliableBobNodesStatus.ReliableNodes {
		addresses = append(addresses, bobNode.Address)
	}

	p.bobAddressesMutex.Lock()
	defer p.bobAddressesMutex.Unlock()

	p.bobAddresses = addresses
	return nil
}

func (p *ClientProvider) GetClient() (*Client, error) {
	var addresses []string
	p.bobAddressesMutex.RLock()
	addresses = append(addresses, p.bobAddresses...)
	p.bobAddressesMutex.RUnlock()

	if len(addresses) == 0 {
		return nil, fmt.Errorf("no available bob instances")
	}

	address := addresses[rand.Intn(len(addresses))]
	bobClient := NewClient(p.httpClient, fmt.Sprintf("%s://%s:%s", p.bobHttpProtocol, address, p.bobHttpPort))

	return bobClient, nil
}

type reliableNodeDef struct {
	Address string `json:"address"`
}

type bobFetcherStatusResponse struct {
	MaxTick                 uint32            `json:"max_tick"`
	LastUpdate              int64             `json:"last_update"`
	NumberOfConfiguredNodes int               `json:"number_of_configured_nodes"`
	ReliableNodes           []reliableNodeDef `json:"reliable_nodes"`
}
