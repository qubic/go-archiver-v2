# Bob Connector - Missing Features & Simplifications

This document tracks what's missing or simplified when using bob as the data source
for the archiver, compared to the direct Qubic node connector. These are candidates
for future sprint work.

## 1. TargetTickVoteSignature (quorum vote score filtering)

**What**: Bob doesn't expose `SystemInfo.TargetTickVoteSignature`.

**Current behavior**: The archiver skips the vote signature score check
(`skipScoreCheck=true`) when using bob. Cryptographic signature verification
of individual votes still runs.

**Impact**: Minor. The score check is an optimization/filtering heuristic, not a
security-critical validation. Bob has already validated the votes.

**Future fix**: Expose `TargetTickVoteSignature` in bob's `/status` or `/tick` endpoint.

---

## 2. ComputorPacketSignature (mid-epoch computor change detection)

**What**: Bob doesn't expose `SystemInfo.ComputorPacketSignature`.

**Current behavior**: The archiver fetches the computor list once per epoch and
caches it. It does NOT detect mid-epoch computor list changes.

**Impact**: Low. Computor list changes mid-epoch are rare. If they occur, the
archiver may validate ticks against stale computor keys until the next epoch.

**Future fix**: Expose `ComputorPacketSignature` in bob's status endpoint, OR
add a computor-change notification mechanism.

---

## 3. Arbitrator signature on computor list

**What**: Bob returns computor identities (60-char strings) via `qubic_getComputors`,
but does NOT return the raw 64-byte arbitrator signature on the computor list.

**Current behavior**: The archiver skips arbitrator signature validation for
bob-sourced computors and marks them as pre-validated (`Validated: true`).

**Impact**: Low. Bob has already validated the computor list. The arbitrator
signature is a trust chain verification that's redundant when trusting bob.

**Future fix**: Expose the raw computor list signature in bob's `qubic_getComputors`
response.

---

## 4. Transaction status (moneyFlew)

**What**: Bob has per-transaction `executed` status from log events, but does NOT
expose the `MoneyFlew` bit array that the node connector provides.

**Current behavior**: The archiver returns empty transaction status (all moneyFlew
bits set to false) when using bob. The `txstatus` validation is effectively skipped.

**Impact**: Medium. The `moneyFlew` field is used by API consumers to determine if
a transaction's side effects were executed. Currently all transactions appear as
"not executed" when using bob.

**Future fix**: Use `qubic_getTransactionReceipt` to fetch per-transaction execution
status, then reconstruct the MoneyFlew bit array from the `executed` field.

---

## 5. Transaction fetching efficiency

**What**: Bob's `GET /tick/{tick}` returns transaction digests but not full transaction
bodies. The archiver must make an additional RPC call (`qubic_getTickByNumber` with
`includeTransactions=true`) to get full transactions with signatures.

**Impact**: Performance. Two HTTP calls per tick instead of one.

**Future fix**: Add full transaction bodies (with signatures) to the REST
`GET /tick/{tick}` response, or add a query parameter to include them.

---

## 6. SystemInfo.InitialTick in GetSystemMetadata

**What**: `GetSystemMetadata` for bob currently returns `InitialTick: 0`. The
`InitialTick` is instead obtained from `GetTickStatus` which reads `/status`.

**Current behavior**: The validator gets `InitialTick` from `GetTickStatus` and
passes it to computor validation. However, `GetSystemMetadata` returns 0 for
`InitialTick` which means the computor's initial `TickNumber` will be set to 0
on first fetch.

**Impact**: Very low. The computor TickNumber is metadata only, not used in
validation logic.

**Future fix**: Populate `InitialTick` from the cached status response in
`GetSystemMetadata`.
