package bob

// bobRPCTransaction represents a transaction in a qubic_getTickByNumber response.
// `executed` is tri-state: true (success), false (failed), null (pending; bob has not
// finalized this tick yet).
type bobRPCTransaction struct {
	Hash     string `json:"hash"`     // 60-char lowercase qubic-hash encoding of the tx digest
	Executed *bool  `json:"executed"` // nil = pending
}

// bobRPCTickResponse is the result payload from qubic_getTickByNumber(tick, true).
type bobRPCTickResponse struct {
	TickNumber   uint32              `json:"tickNumber"`
	Transactions []bobRPCTransaction `json:"transactions"`
}
