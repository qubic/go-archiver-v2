# Bob Connector - Validation Gaps & Requests for Bob Maintainers

This document tracks what data integrity validations are **skipped or weakened**
when using bob as the data source, compared to the direct Qubic node connector.
Each item describes what bob would need to expose to achieve full validation parity.

---

## Critical Missing Functionality

### 0. Epoch transition handling

**What's missing**: The archiver has no way to discover which epochs bob has stored,
or to know the `endTick` of each epoch for sequential processing.

**Current behavior**: `GetTickStatus` returns the **current** epoch from bob's `/status`.
The processor uses `InitialTick` to detect epoch boundaries. During catchup, if the
archiver is processing epoch 207 but bob reports epoch 209 as current, it will skip
epoch 208 entirely — jumping from wherever it is to epoch 209's `InitialTick`.

**Impact**: Critical. Epochs get skipped during catchup, resulting in missing data.
Also, if bob transitions to a new epoch while the archiver is still syncing the
previous one, the archiver would abandon the old epoch mid-way.

**What bob has today**:
- `qubic_getEpochInfo(epoch)` — returns `initialTick`, `endTick` for a specific epoch
  (`initialTick: 0` when the epoch doesn't exist in bob's database)
- `qubic_getCurrentEpoch` — returns current epoch info
- NO endpoint to list all available epochs

**Request**:
- Expose a list of available epochs (e.g. `qubic_getAvailableEpochs` returning `[207, 208, 209]`)
- Alternatively, the archiver can probe sequentially using `qubic_getEpochInfo(N)` and
  check for `initialTick > 0`, but a listing endpoint would be more efficient

**Archiver changes needed**: Refactor processor to iterate epochs sequentially using
`endTick` boundaries, rather than relying solely on the live epoch from `/status`.

---

## Currently Skipped Validations

### 1. Quorum vote score filtering (TargetTickVoteSignature)

**What's missing from bob**: `SystemInfo.TargetTickVoteSignature` — a uint32 threshold
used to filter quorum votes by their signature score.

**What we skip**: The score check in `quorum.verifyTickVoteSignature()`. We still
verify each vote's cryptographic Schnorr signature against the computor's public key,
but we don't filter by score threshold.

**Impact**: Minor. The score check is an optimization/difficulty filter. Cryptographic
signature verification (which we DO perform) is the security-critical validation.

**Request**: Expose `TargetTickVoteSignature` in bob's `/status` or `GET /tick/{tick}`
response, or as a field in `qubic_syncing`.

---

### 2. Mid-epoch computor list change detection (ComputorPacketSignature)

**What's missing from bob**: `SystemInfo.ComputorPacketSignature` — a uint64 that
changes when the computor list is updated mid-epoch.

**What we skip**: The archiver fetches the computor list once at epoch start and
reuses it for the entire epoch. It does NOT detect mid-epoch computor list changes.

**Impact**: Low. If computors change mid-epoch, the archiver may validate quorum
vote signatures against stale public keys. This could cause valid ticks to fail
validation or (worse) accept votes signed by former computors.

**Request**: Expose `ComputorPacketSignature` in bob's status endpoint. Alternatively,
expose a computor-list-version or change-tick so the archiver can detect when to refetch.

---

### 3. Arbitrator signature on computor list

**What's missing from bob**: The raw 64-byte arbitrator signature on the computor
list. Bob's `qubic_getComputors` returns only identity strings (60-char), not the
signed computor packet.

**What we skip**: Arbitrator signature validation (`computors.Validate()`). The
archiver trusts bob's computor list and marks it as pre-validated.

**Impact**: Medium. The arbitrator signature proves the computor list was authorized
by the network arbitrator. Without it, a compromised bob could serve a fake computor
list, which would then be used to validate quorum votes — effectively bypassing all
validation.

**Request**: Expose the raw computor list signature (64 bytes, hex-encoded) in the
`qubic_getComputors` response. This is the `Signature` field from the `Computors`
struct in the Qubic protocol.

---

### 4. TargetTickVoteSignature storage per epoch

**What's missing from bob**: Same as item #1 (`TargetTickVoteSignature`).

**What we skip**: The archiver does not store `TargetTickVoteSignature` history
per epoch (`quorum.StoreTargetTickVoteSignature` is skipped). This data is used
by the archiver API but is non-critical for processing.

**Request**: Same as item #1.

---

## Resolved Items

### Transaction status (moneyFlew) — RESOLVED in bob 1.4.0

Bob 1.4.0 added the `executed` field (tri-state: `true`/`false`/`null`) to each
transaction in the `qubic_getTickByNumber` response. The archiver reads this directly
from the RPC call already made in `GetTickTransactions` — zero extra HTTP calls.

See: `network/bob/moneyflew.go`

---

### Transaction fetching (signatures) — RESOLVED

Bob's `qubic_getTickByNumber` with `includeTransactions=true` returns full transaction
data including signatures. Two HTTP calls per tick (REST for votes/tick data, RPC for
transactions with signatures) but functionally complete.

---

## Summary: What Bob Maintainers Need to Add

| Priority | Field | Where to expose | Purpose |
|----------|-------|-----------------|---------|
| **High** | Available epochs list | New `qubic_getAvailableEpochs` endpoint | Sequential epoch processing during catchup |
| **High** | Computor list signature (64 bytes) | `qubic_getComputors` response | Arbitrator trust chain validation |
| Medium | `ComputorPacketSignature` (uint64) | `/status` or `qubic_syncing` | Detect mid-epoch computor changes |
| Low | `TargetTickVoteSignature` (uint32) | `/status` or `GET /tick/{tick}` | Vote score filtering + storage |
