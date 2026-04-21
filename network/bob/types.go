package bob

import "encoding/json"

// bobTickResponse represents the full response from GET /tick/{tick_number}.
type bobTickResponse struct {
	Tick     uint32       `json:"tick"`
	TickData bobTickData  `json:"tickdata"`
	Votes    []bobVote    `json:"votes"`
}

// bobTickData represents tick data from bob's REST API.
type bobTickData struct {
	ComputorIndex      uint16   `json:"computorIndex"`
	Epoch              uint16   `json:"epoch"`
	Tick               uint32   `json:"tick"`
	IsSkipped          bool     `json:"isSkipped"`
	Millisecond        uint16   `json:"millisecond"`
	Second             uint8    `json:"second"`
	Minute             uint8    `json:"minute"`
	Hour               uint8    `json:"hour"`
	Day                uint8    `json:"day"`
	Month              uint8    `json:"month"`
	Year               uint8    `json:"year"`
	Timelock           string   `json:"timelock"`           // qubic hash encoded (60-char lowercase)
	TransactionDigests []string `json:"transactionDigests"` // qubic hash encoded
	ContractFees       jsonFees `json:"contractFees"`       // can be 0 (number) or array of int64
	Signature          string   `json:"signature"`          // hex encoded (128 chars)
	LogIdStart         int64    `json:"logIdStart"`
	LogIdEnd           int64    `json:"logIdEnd"`
}

// jsonFees handles bob's contractFees field which can be either 0 (number) or an array.
type jsonFees []int64

func (f *jsonFees) UnmarshalJSON(data []byte) error {
	// Try as array first
	var arr []int64
	if err := json.Unmarshal(data, &arr); err == nil {
		*f = arr
		return nil
	}
	// If it's a single number (0), treat as empty
	var n int64
	if err := json.Unmarshal(data, &n); err == nil {
		*f = nil
		return nil
	}
	return nil
}

// bobVote represents a quorum vote from bob's REST API.
type bobVote struct {
	ComputorIndex                    uint16 `json:"computorIndex"`
	Epoch                            uint16 `json:"epoch"`
	Tick                             uint32 `json:"tick"`
	Millisecond                      uint16 `json:"millisecond"`
	Second                           uint8  `json:"second"`
	Minute                           uint8  `json:"minute"`
	Hour                             uint8  `json:"hour"`
	Day                              uint8  `json:"day"`
	Month                            uint8  `json:"month"`
	Year                             uint8  `json:"year"`
	PrevResourceTestingDigest        uint32 `json:"prevResourceTestingDigest"`
	SaltedResourceTestingDigest      uint32 `json:"saltedResourceTestingDigest"`
	PrevTransactionBodyDigest        uint32 `json:"prevTransactionBodyDigest"`
	SaltedTransactionBodyDigest      uint32 `json:"saltedTransactionBodyDigest"`
	PrevSpectrumDigest               string `json:"prevSpectrumDigest"`               // qubic hash
	PrevUniverseDigest               string `json:"prevUniverseDigest"`               // qubic hash
	PrevComputerDigest               string `json:"prevComputerDigest"`               // qubic hash
	SaltedSpectrumDigest             string `json:"saltedSpectrumDigest"`             // qubic hash
	SaltedUniverseDigest             string `json:"saltedUniverseDigest"`             // qubic hash
	SaltedComputerDigest             string `json:"saltedComputerDigest"`             // qubic hash
	TransactionDigest                string `json:"transactionDigest"`                // qubic hash
	ExpectedNextTickTransactionDigest string `json:"expectedNextTickTransactionDigest"` // qubic hash
	Signature                        string `json:"signature"`                        // hex encoded
}

// bobSyncStatus represents the response from GET /status.
type bobSyncStatus struct {
	Epoch                      uint16  `json:"epoch"`
	InitialTick                uint32  `json:"initialTick"`
	CurrentFetchingTick        uint32  `json:"currentFetchingTick"`
	CurrentFetchingLogTick     uint32  `json:"currentFetchingLogTick"`
	CurrentVerifyLoggingTick   uint32  `json:"currentVerifyLoggingTick"`
	CurrentIndexingTick        uint32  `json:"currentIndexingTick"`
	LastSeenNetworkTick        uint32  `json:"lastSeenNetworkTick"`
	IsSyncing                  bool    `json:"isSyncing"`
	Progress                   float64 `json:"progress"`
}

// bobComputorsResponse represents the response from qubic_getComputors.
type bobComputorsResponse struct {
	Epoch      uint16   `json:"epoch"`
	Computors  []string `json:"computors"` // 60-char uppercase identity strings
}

// bobRPCTransaction represents a transaction from qubic_getTickByNumber (RPC format).
// As of bob 1.4.0, includes the executed field (tri-state: true/false/null for pending).
type bobRPCTransaction struct {
	Hash      string `json:"hash"`      // 60-char lowercase
	From      string `json:"from"`      // 60-char uppercase identity
	To        string `json:"to"`        // 60-char uppercase identity
	Amount    int64  `json:"amount"`
	InputType uint16 `json:"inputType"`
	InputSize uint16 `json:"inputSize"`
	InputData string `json:"inputData"` // hex encoded
	Signature string `json:"signature"` // hex encoded (128 chars)
	Executed  *bool  `json:"executed"`  // nil = pending, true = success, false = failed
}

// bobRPCTickResponse represents the response from qubic_getTickByNumber with includeTransactions=true.
type bobRPCTickResponse struct {
	TickNumber       uint32              `json:"tickNumber"`
	Epoch            uint16              `json:"epoch"`
	ComputorIndex    uint16              `json:"computorIndex"`
	Signature        string              `json:"signature"`     // hex
	TickHash         string              `json:"tickHash"`      // hex
	Timestamp        uint64              `json:"timestamp"`
	Millisecond      uint16              `json:"millisecond"`
	Timelock         string              `json:"timelock"`      // hex
	TransactionCount int                 `json:"transactionCount"`
	Transactions     []bobRPCTransaction `json:"transactions"`
}
