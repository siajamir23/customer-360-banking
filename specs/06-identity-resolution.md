# 06 — Customer Identity Resolution (Implementation)

**Phase:** 6 (Identity Resolution) · **Status:** Approved (2026-11-24; review amendments RA1–RA15 applied)  
**Depends on:** `02-customer-identity-resolution.md` (amended by this document — see section 9), `05-silver-standardization.md` (production-executed for batch_001 at HEAD ba8aa00), `02-data-model.md`, `02-source-data-contracts.md`; Phase 6 Steps 1–2 investigations (2026-11-24, read-only)

## Objective

Resolve every customer reference from the six source systems into the unified enterprise `CUST-######` domain: produce `silver.customer_xref` (the crosswalk every Silver product table joins on), `quarantine.rejected_resolutions`, and identity-enriched views. Phase 5 tables, code, tests, and audit are untouched.

## 1. Decisions (all approved 2026-11-24)

| # | Decision | Basis |
|---|----------|-------|
| D1 | **Conflict semantics: the quarantine reading governs.** A product ref whose valid `global_customer_ref` resolves to customer B while any distinct email resolves to customer A ≠ B is quarantined (`rejected_resolutions`, reason `EMAIL_REF_CONFLICT`; xref `IN_REVIEW`; `unified_customer_id` NULL) — never auto-merged. Resolves the identity-spec §2-cascade vs §4/Amendment-1 tension in favor of the quarantine rule, which appears in three approved documents. | Step 2 Part 1: 1,414 refs affected (405 pure + 1,009 mixed) |
| D2 | **Mixed refs quarantine alongside pure:** any conflicting email evidence diverts the ref, even when other emails of the same ref agree with the R1 customer. No silent choosing. | Step 2 Part 1 |
| D3 | **Orphan disposition (both mechanisms):** every orphan ref (no valid gref, no email resolving to any customer) gets an xref row with `match_rule = UNRESOLVED` **and** a `rejected_resolutions` row, reason `ORPHAN_NO_R1_R2`, carrying the identity evidence. Original Silver rows are never dropped (joinable by native ref). | Contracts: "quarantine + reconciliation reporting"; Step 1/2: 2,325 refs |
| D4 | **R4 never auto-adopts.** A record without a global ref whose `name_key`+DOB matches one **or more** already-resolved customers → `IN_REVIEW`, reason `NAME_DOB_REVIEW` (subsumes identity §5 rule 2). Rationale: all higher-precedence keys are unique in the population and all 12,000 DOBs share a single month-day (observed artifact), so name+DOB is weak evidence. | Step 2 Part 3: CIS-11603, CIS-10717 |
| D5 | **R5 allocation:** candidates = CIS records confirmed R5 in the run, ordered ascending `cis_customer_id`; IDs contiguous from `CUST-100001`; allocated **only on confirmed R5 outcome** (IN_REVIEW persons consume no ID; a later "register new" review disposition takes the then-next ID); resumption = max assigned R5 ID + 1; never reused. | Step 2 Part 4 |
| D6 | **Enrichment architecture: identity-enriched views (Option C).** One view per entity (`silver.<entity>_resolved`) = Silver table LEFT JOIN `customer_xref` on the native ref. Fallback: separate materialized tables (Option B). Option A (ALTER/UPDATE of Phase 5 tables) rejected. Data-model §3 amended accordingly (section 9). | Step 2 Part 5 |
| D7 | **R2 match set = the accumulated identity set's emails** (CIS records; product-source emails are attributes, never identity keys). A no-gref ref whose emails identify ≥2 distinct customers → `IN_REVIEW`, reason `EMAIL_EMAIL_CONFLICT` (cannot occur in batch_001 — observed 0). Emails map 1:1 to customers (10,991 distinct over 10,991 customers). | Step 1/2 |
| D8 | **Acceptance baselines = exact production counts** (section 7); deterministic pipeline, exact targets, Option-A style. | Step 1 profiling |

## 2. Resolution Algorithm (deterministic)

**Grain:** one row per (`source_system`, `source_customer_ref`) — the xref grain. Native refs never NULL; ref→gref fan-out is exactly 1 (verified batch_001). **Seed order (fixed):** CIS → CORE_BANKING → CREDIT_CARD → LOANS → DIGITAL → SUPPORT; each later system matches the accumulated identity set. Candidate evaluation is **exhaustive** per ref (R1 and all R2 email candidates computed before outcome selection); the cascade then selects the outcome, and conflicting evidence diverts (D1/D2).

### 2.1 CIS — identity establishment (independent candidate evaluation)

CIS records are processed in ascending `cis_customer_id` order (deterministic). For each record, all available R1/R2/R3 evidence is evaluated **independently before** an identity is assigned — cascade precedence never hides contradictory evidence:

* **R1 candidate**: valid `global_customer_ref` resolved against the accumulated registry.
* **R2 candidate**: normalized email resolved against the accumulated identity set's emails.
* **R3 candidate**: normalized phone resolved against the accumulated identity set's phones.

Outcome selection:

1. **Candidates present, all identifying the same unified ID** → adopt that ID under the highest-precedence available rule (`R1_GLOBAL_REF`/EXACT, else `R2_EMAIL`/EXACT, else `R3_PHONE`/HIGH). *(batch_001: 10,000 via R1; R2/R3 fire 0 times)*
2. **Candidates identifying different unified IDs** → `IN_REVIEW`, reason `CIS_IDENTIFIER_CONFLICT` — covering **R1-vs-R2** (gref vs email), **R1-vs-R3** (gref vs phone), and **R2-vs-R3** (email vs phone) disagreement. *(batch_001: 0 — every customer email and phone is unique per customer, so cross-customer disagreement cannot occur; rule active for future batches)*
3. **No R1–R3 evidence resolved the record** → evaluate **R4** (`name_key`+DOB against already-resolved customers). Any match — one or more — → `IN_REVIEW`, reason `NAME_DOB_REVIEW`; never auto-adopt (D4). Records already diverted by outcome 2 do not cascade further. *(batch_001: 2 — CIS-11603, CIS-10717)*
4. **Nothing matched** → **R5**: new ID per section 3. *(batch_001: 1,998)*

Expected batch_001 CIS outcomes: R1 10,000 · R2 0 · R3 0 · R5 1,998 · IN_REVIEW 2 (both `NAME_DOB_REVIEW`; zero `CIS_IDENTIFIER_CONFLICT`).

### 2.2 Product sources — reference resolution

Per ref: R1 candidate B = the ref's gref resolved against the accumulated registry; R2 candidates = **every distinct non-null email** on the ref's rows resolved against the accumulated identity set's emails. Outcomes, in order:

1. R1 present AND any R2 candidate ≠ B → `IN_REVIEW`, `EMAIL_REF_CONFLICT` (D1/D2). *(1,414)*
2. No R1 AND ≥2 R2 candidates → `IN_REVIEW`, `EMAIL_EMAIL_CONFLICT` (D7). *(0 in batch_001)*
3. No R1 AND exactly 1 R2 candidate → `R2_EMAIL` adopt, EXACT. *(6,024 — includes matches to R5-registered new persons)*
4. No R1 AND no R2 candidate → `UNRESOLVED` (D3). *(2,325)*
5. R1 present AND (all R2 candidates = B or no R2 candidate) → `R1_GLOBAL_REF` adopt, EXACT. *(25,225 = 20,132 agreeing + 5,093 ordinary email differences)*

**Defensive rule (documented):** a valid gref not present in the accumulated registry → `IN_REVIEW`, reason `UNRESOLVED_GLOBAL_REF`. Cannot occur in batch_001 (verified: every carried gref exists in CIS).

### 2.3 Hard boundaries

No changes to `src/silver`, Phase 5 tables/tests/audit; Bronze immutable; `fraud_detection` read-only. **R5 registrations come only from CIS records** — product refs never register new customers (they resolve via R1/R2 or divert to review/orphan). No Gold; no SCD2.

## 3. R5 Allocation, Serialization & Recovery (D5 detail; RA13 ledger subsystem)

1. **Within-batch order:** R5 candidates of the current run are sorted ascending `cis_customer_id` — this ordering governs allocation **within a batch**.
2. **IDs:** contiguous from `max(existing R5 IDs in the allocation ledger) + 1` (first ever: `CUST-100001`); batch_001 assigns `CUST-100001`–`CUST-100998` to the 1,998 confirmed persons (the 2 IN_REVIEW persons consume nothing).
3. **Determinism scope (explicit):** global ID assignment is deterministic **for a fixed serialized batch-processing order**. Within a batch, IDs depend only on (existing ledger state, ascending `cis_customer_id`). If batches were processed in a different order, the same persons could receive different IDs — therefore batches are resolved strictly one at a time, in arrival order, by the single orchestrator, and the processing order of each run is documented.
4. **Collision safety:** fraud domain ends at `CUST-010000` (verified); seeded grefs ≤ `CUST-010000`; `100001 > 010000` in the zero-padded 6-digit domain — no collision by construction.

### 3.1 Allocation ledger and control row (RA13)

**`silver.r5_allocation_ledger`** — the durable allocation history, written **before** any downstream Phase 6 write:

* **Grain:** one row per (`batch_id`, `cis_customer_id`) — the allocation unit. batch_001 = 1,998 rows.
* **Schema:** `batch_id STRING`, `cis_customer_id STRING`, `unified_customer_id STRING` (the reserved R5 ID), `allocation_seq INT` (position in ascending `cis_customer_id` order within the batch), `state STRING` (`RESERVED` → `COMMITTED` | `ABANDONING` → `ABANDONED`), `reserved_at TIMESTAMP`, `finalized_at TIMESTAMP`.
* **Uniqueness constraints:** (`batch_id`, `cis_customer_id`) unique; (`batch_id`, `unified_customer_id`) unique; `unified_customer_id` **globally unique across the entire table — all states, all batches**: an ID is reserved exactly once, ever. These are declared as **informational** Delta constraints on the table (see §3.5); actual write-time enforcement is the allocator's pre-reservation validation.
* **Lifecycle:** `RESERVED` at allocation (one atomic append of the complete batch mapping); `COMMITTED` when the batch's xref write and reconciliation succeed; `ABANDONING` when abandonment intent is recorded (§3.3.4 step 1); `ABANDONED` when abandonment completes (§3.3.4 step 2). **Rows are never deleted** — `ABANDONED` rows are retained forever.
* **Recovery behavior:** the `RESERVED` mapping is the authoritative person→ID assignment for the batch; all downstream writes read it by join (never re-allocate). Resumption and abandonment are defined in §3.3.

**`silver.identity_resolution_control`** — the enforceable single-writer gate:

* **Grain:** exactly one row (singleton), created `IDLE` by the first run.
* **Schema:** `control_state STRING` (`IDLE` | `BUSY`), `active_batch_id STRING`, `active_batch_state STRING` (`RUNNING` | `ABANDONING` | `ABANDONED` — abandonment progress marker, §3.3.4), `updated_at TIMESTAMP`, `updated_by STRING`.
* **Acquisition is an atomic check-and-set:** a run's first write is a single conditional transaction flipping `IDLE` → `BUSY(batch_id)` that fails (and aborts the run) if the row is not `IDLE`. Delta's transactional commit guarantees that among any number of concurrent attempts exactly one succeeds and the rest conflict-abort. This is the **enforceable** concurrency protection: R5 allocation happens only while holding the control row — serialization holds at the data layer, not by assumption.
* **Release:** `BUSY` → `IDLE` (with `active_batch_id` cleared) only as the final step of a successful run or of a **completed** recovery. A `BUSY` row whose run did not finish is itself the persistent signal of an **incomplete batch**: every new run aborts at acquisition until that batch is resumed or abandoned — **no subsequent batch can allocate R5 IDs while an earlier batch's allocation is incomplete or its recovery is unresolved**.

**Consistency with `customer_xref` — the authoritative resolved identity mapping (unchanged):** `customer_xref` remains the single source of truth for the resolved identity mapping; it is what the `*_resolved` views join on, and identity consumers never query the ledger. The ledger is the authoritative **allocation history** — the pre-write reservation of R5 IDs — not a crosswalk. Reconciled by invariant (audited at the end of every run and in the Phase 9 weekly audit): every xref `R5_NEW` row corresponds 1:1 to a `COMMITTED` ledger row (same `batch_id`, same `unified_customer_id`); every `COMMITTED` ledger row has exactly one xref `R5_NEW` row; no `RESERVED`-only, `ABANDONING`, or `ABANDONED` ledger ID appears in xref; and the allocation frontier `max(all ledger IDs) + 1` ≥ `max(xref R5_NEW IDs) + 1`.

### 3.2 Run sequence (durable mapping before downstream writes)

1. **Acquire control:** atomic check-and-set `IDLE` → `BUSY(batch_id)`; abort if not `IDLE` (enforceable single writer).
2. **Batch guard:** an already-resolved batch aborts (mirrors Phase 5), with control released.
3. **Allocate:** compute the complete R5 mapping — ascending `cis_customer_id`, contiguous from `max(all ledger IDs) + 1`.
4. **Durable reservation:** append **all** ledger rows for the batch in **one atomic Delta append** (single commit), state `RESERVED`. After this commit the complete person→ID mapping is durable — **no downstream Phase 6 write has occurred yet**.
5. **Downstream writes:** `customer_xref` R5 rows are written **by joining the ledger** on (`batch_id`, `cis_customer_id`) — the allocator is never re-run for a batch that has ledger rows. Quarantine and view writes follow.
6. **Finalize:** when all writes and the §6 reconciliation succeed, mark the batch's ledger rows `COMMITTED` (one transactional update), then release control `BUSY` → `IDLE`.

### 3.3 Partial-failure recovery sequence

A failure leaves: control `BUSY(batch_id)`, a complete `RESERVED` ledger mapping (unless the run failed before step 4), and an arbitrary prefix of the downstream writes.

1. **Detect:** the next run aborts at control acquisition (`BUSY`); the incomplete batch is identified by `active_batch_id`; no other batch can proceed.
2. **Resume (default):** re-run the orchestrator for the same batch under an explicit recovery flag. It reuses the **exact original assignments** — downstream writes are re-derived from the `RESERVED` ledger rows by join, never re-allocated. xref insertion is insert-new-only (§5), so the already-written prefix is skipped and the remainder completes. The batch then finalizes per §3.2 step 6. If the run failed *before* reservation, no mapping exists; recovery simply re-runs steps 3–4 — deterministic, and safe because no IDs were consumed.
3. **Delete-and-replay (inputs were wrong):** permitted only when the failed batch is the **latest** batch — **enforced on the ledger**: no ledger rows exist from any batch allocated after the failed one. Recovery deletes the batch's ledger, xref, and quarantine rows (one batch-scoped scope by `_batch_id`) and re-runs from scratch; the replay re-allocates identically (`max + 1` excludes only the deleted mapping's own IDs).
4. **Abandonment (precise definition — idempotent, resumable multi-step protocol; RA14):** withdrawal of a batch that cannot or should not be completed — e.g., an unrecoverable input defect, or withdrawal requested after later batches have already allocated. Abandonment is **not** one atomic transaction across the ledger, xref, quarantine, and control tables — Delta cannot atomically commit a multi-table write. It is an **ordered protocol of individually atomic, idempotent steps**, each a single Delta transaction on one table, resumable from any interruption. **The control row remains `BUSY(batch_id)` throughout the protocol.** Exact ordered operations:

   1. **Record abandonment intent** (one transactional update): the batch's ledger rows `RESERVED` → `ABANDONING`, and the control row's `active_batch_state` → `ABANDONING`. From this point resume (§3.3.2) is permanently forbidden — the only forward path is completing abandonment.
   2. **Mark allocation ledger rows `ABANDONED`** (one transactional update): `ABANDONING` → `ABANDONED` — **retained forever, never deleted, IDs never freed and never reassigned** (the frontier is computed from the ledger across all states).
   3. **Remove partial xref and quarantine rows** (batch-scoped `DELETE` by `_batch_id`, one transaction per table): safe *because* the mapping survives in the ledger.
   4. **Verify cleanup and ledger/xref consistency** (read-only): zero `customer_xref` / `rejected_resolutions` rows remain for the batch; no `ABANDONING`/`ABANDONED` ledger ID appears in `customer_xref`; the global `COMMITTED`↔xref 1:1 reconciliation (§3.1) still holds. Any check that fails → repeat steps 3–4.
   5. **Mark abandonment complete** (one transactional update): control row `active_batch_state` → `ABANDONED`.
   6. **Release the control row:** `BUSY` → `IDLE`, with `active_batch_id` and `active_batch_state` cleared.

   **Failure after any individual step:** the control row is still `BUSY` with `active_batch_state = ABANDONING`, so **every new run aborts at acquisition — a new batch can never acquire the control row while abandonment is incomplete**. Re-running the protocol resumes from wherever it stopped: every step is idempotent (rows already `ABANDONING`/`ABANDONED` are skipped by the state predicates; deletes re-delete nothing; verification re-checks), so the protocol converges to the same end state regardless of how many times it is interrupted. Abandoned persons hold no resolved identity; if their data returns in a later batch they are new candidates and receive fresh IDs from the frontier.

### 3.4 Invariants preserved (RA9, strengthened by RA13)

An R5 ID is never assigned to a different person — reservation is single-assignment (global ledger uniqueness) and `ABANDONED` IDs are never freed. Existing person→ID assignments survive every recovery path — resume reuses the durable mapping; abandonment preserves it in the ledger. A previously allocated ID never changes because an earlier batch is replayed — replay is limited to the latest batch (§3.3.3). A batch containing R5 assignments is rolled back after later allocations **only via abandonment**, whose ledger retention preserves the original allocation mapping — the RA9 escape clause ("unless its original allocation mapping is preserved"), now realized by the ledger rather than by a prohibition on deletion. R5 IDs are never reused and never re-pointed (xref immutability, §5, unchanged for committed assignments). Global serialization is enforced by the control row, not assumed.

### 3.5 Enforced uniqueness (RA15)

The uniqueness declarations on `r5_allocation_ledger` (`PRIMARY KEY` / `UNIQUE` style) are **informational Delta constraints**: in current Databricks they document intent and may assist query optimization, but they are **not enforced on write**. The §3.1 uniqueness invariants are therefore **enforced by the serialized allocator** — while holding the control row (single writer, §3.1) and **before** the durable reservation append (§3.2 step 4) — by validating the complete candidate mapping:

1. **No duplicate `unified_customer_id` across any ledger rows, any state, any batch** — including `ABANDONED` rows: abandoned IDs are never reissued.
2. **No duplicate allocation key (`batch_id`, `cis_customer_id`)** — every candidate is allocated exactly once; and no ledger rows already exist for the batch (re-preparation is impossible under the control protocol).
3. **No duplicate `allocation_seq` within the batch** — ascending `cis_customer_id` order yields a strict permutation of `1..N`.
4. **No collision with existing `customer_xref` assignments** — no candidate ID equals any `unified_customer_id` already present in `customer_xref` (the frontier `max(all ledger IDs) + 1` makes collision impossible by construction; it is validated, not assumed).

**Fail-closed:** any validation failure aborts the run **before the durable ledger append** — zero rows written, zero IDs consumed, `customer_xref`/`rejected_resolutions` untouched, control released `BUSY` → `IDLE` — and the failure reason is reported. Because no state was written, a re-attempt after the defect is corrected is identical to a first attempt. The single-writer control protocol (§3.1) and the durable reservation design (§3.2) are unchanged.

## 4. Output Schemas

### 4.1 `silver.customer_xref`

Grain/PK: (`source_system`, `source_customer_ref`); batch_001 = **46,988 rows** (12,000 CIS + 34,988 product refs). Columns per identity §6: `source_system`, `source_customer_ref`, `global_customer_ref_raw`, `unified_customer_id` (NULL if IN_REVIEW/UNRESOLVED), `match_rule` (enum extended per D3: `R1_GLOBAL_REF` / `R2_EMAIL` / `R3_PHONE` / `R4_NAME_DOB` / `R5_NEW` / `IN_REVIEW` / `UNRESOLVED`), `match_confidence` (`EXACT` / `HIGH` / `MEDIUM` / `NEW` / `IN_REVIEW` / `UNRESOLVED`), `match_timestamp`, `_batch_id`.

Invariants: (1) each (`source_system`, `source_customer_ref`) maps to **at most one** `unified_customer_id`; (2) a source reference never maps to two different unified IDs; (3) **multiple source references may map to the same unified customer** — this many-to-one mapping is expected and is the purpose of resolution (the same natural person referenced by several systems); it is not a uniqueness violation; (4) an R5 ID is never assigned to two distinct persons and never reused; (5) `unified_customer_id` on every product row comes only from this table (single source of truth); `R4_NAME_DOB` rows cannot occur (D4 diverts to `IN_REVIEW`).

### 4.2 `quarantine.rejected_resolutions`

Grain: one row per (`source_system`, `source_customer_ref`, `reason_code`, `_batch_id`) — `_batch_id` in the grain keeps drift events for the same reference across different batches independently auditable (each batch's evaluation of a reference yields at most one row per reason). batch_001 = **3,741 rows**. Columns: `source_system`, `source_customer_ref`, `reason_code`, `candidate_evidence`, `global_customer_ref_raw`, `_batch_id`, `_quarantine_timestamp`.

`reason_code` enum: `EMAIL_REF_CONFLICT` (product: gref→B vs email→A) · `EMAIL_EMAIL_CONFLICT` (product: no gref, ≥2 emails→different customers) · `CIS_IDENTIFIER_CONFLICT` (CIS: any R1/R2/R3 pairwise disagreement) · `NAME_DOB_REVIEW` (R4 match; never auto-adopt) · `ORPHAN_NO_R1_R2` (no resolvable evidence — **unresolved identity**) · `UNRESOLVED_GLOBAL_REF` (defensive: valid gref absent from the registry) · `IDENTITY_DRIFT` (later batch: contradictory evidence against an existing assignment).

**Review vs unresolved:** `EMAIL_REF_CONFLICT`, `EMAIL_EMAIL_CONFLICT`, `CIS_IDENTIFIER_CONFLICT`, `NAME_DOB_REVIEW` (and `UNRESOLVED_GLOBAL_REF`, `IDENTITY_DRIFT`) are **review cases** — a human disposition is possible and pending. `ORPHAN_NO_R1_R2` is **unresolved identity** — no candidate exists. Both classes are stored here but are **reconciled and reported separately** (sections 6–7), mirroring the xref `IN_REVIEW` vs `UNRESOLVED` split.

`candidate_evidence ARRAY&lt;STRUCT&lt;evidence_type STRING, evidence_value STRING, candidate_unified_id STRING, match_rule STRING&gt;&gt;` — the authoritative record of **which evidence value resolved to which candidate ID** (the former parallel arrays could not express this pairing):

* One entry per **distinct evidence value** evaluated: R1 → `evidence_type = 'GLOBAL_REF'`; R2 → `'EMAIL'` (one per distinct non-null normalized email on the reference); R3 → `'PHONE'`; R4 → `'NAME_DOB'` (value serialized as `name_key|YYYY-MM-DD`). `IDENTITY_DRIFT` records additionally carry one distinguished `EXISTING_ASSIGNMENT` entry (defined below).
* `candidate_unified_id` = the unified ID that evidence resolves to; **NULL when the evidence resolves to no customer** (retained so orphans show exactly what was tried).
* **Duplicate candidates preserved:** multiple evidence values resolving to the same unified ID appear as multiple entries sharing that `candidate_unified_id` (e.g., a gref and its agreeing email → two entries, same ID).
* **Deterministic ordering:** entries sorted ascending by (`evidence_type`, `evidence_value`).
* `match_rule` = the rule the evidence feeds (`R1_GLOBAL_REF` / `R2_EMAIL` / `R3_PHONE` / `R4_NAME_DOB`).

**`IDENTITY_DRIFT` — full definition.** A drift record is created only for a source reference that **already has an xref row**, when a later batch's evidence for that reference contradicts the existing assignment:

* The **existing assigned `unified_customer_id` is preserved as evidence**: the record's `candidate_evidence` contains a distinguished entry with `evidence_type = 'EXISTING_ASSIGNMENT'`, `evidence_value` = the currently assigned unified ID, `candidate_unified_id` = the same assigned unified ID, and `match_rule` = the original `match_rule` of the existing xref row.
* The **newly evaluated candidate evidence is recorded separately** as standard entries (`GLOBAL_REF` / `EMAIL` / `PHONE` / `NAME_DOB`) with their own candidates — structurally distinguished from the existing-assignment entry by `evidence_type`.
* Deterministic ordering applies unchanged (ascending `evidence_type`, `evidence_value`).
* Because the grain includes `_batch_id`, **multiple drift events for the same source reference across different batches are separate, independently auditable rows**.
* Drift records **never create a duplicate xref row and never modify the existing assignment** (section 5) — the xref row is untouched.
* Reconciliation reports identity drift as its **own category**, separate from initial identity conflicts and orphans (section 6; section 7 criterion 3).

Original source rows remain in Silver untouched — preserved through their (`source_system`, native reference) relationship and recoverable by it (documented deviation from "full original row" storage: multi-row references make single-row storage arbitrary; nothing is dropped or duplicated).

### 4.3 Identity-enriched views (D6)

`silver.customers_resolved`, `accounts_resolved`, `cards_resolved`, `loans_resolved`, `digital_logins_resolved`, `support_tickets_resolved`: `SELECT silver.*, x.unified_customer_id, x.match_rule, x.match_confidence FROM silver.<entity> LEFT JOIN silver.customer_xref x ON x.source_system = '<SYSTEM>' AND x.source_customer_ref = <native_ref_col>`. No batch predicate on the xref side (xref is the single, immutable truth per ref). Views expose `unified_customer_id`; Phase 5 tables themselves never carry the column (D4 boundary of spec 05 holds; guard test unchanged).

**Join contract per view** (native reference columns verified against the actual Phase 5 Silver schemas and source contracts):

| View | xref `source_system` | Join condition (`x.source_customer_ref =`) |
|---|---|---|
| `silver.customers_resolved` | `CIS` | `cis_customer_id` |
| `silver.accounts_resolved` | `CORE_BANKING` | `cb_customer_ref` |
| `silver.cards_resolved` | `CREDIT_CARD` | `cc_customer_ref` |
| `silver.loans_resolved` | `LOANS` | `ln_customer_ref` |
| `silver.digital_logins_resolved` | `DIGITAL` | `db_customer_ref` |
| `silver.support_tickets_resolved` | `SUPPORT` | `cs_customer_ref` |

**Consumer contract:** rows with `unified_customer_id` NULL are distinguished by `match_rule`: `IN_REVIEW` = review case (disposition pending — reason is one of `EMAIL_REF_CONFLICT` / `EMAIL_EMAIL_CONFLICT` / `CIS_IDENTIFIER_CONFLICT` / `NAME_DOB_REVIEW`); `UNRESOLVED` = orphan (`ORPHAN_NO_R1_R2`). The specific reason is not carried in the view — consumers join `quarantine.rejected_resolutions` on (`source_system`, `source_customer_ref`) for it. Gold excludes both classes.

### 4.4 Allocation ledger & control table (RA13)

`silver.r5_allocation_ledger` and `silver.identity_resolution_control` (grain, schema, uniqueness, lifecycle, recovery: §3.1; enforced uniqueness: §3.5) are Phase 6 outputs created by the orchestrator. They are internal allocation machinery, **not** consumer-facing: identity consumers use `customer_xref` exclusively. The ledger is append/update-only and its rows are never deleted; the control row is a singleton updated in place by the atomic check-and-set. batch_001: 1,998 ledger rows, all `COMMITTED` at finalization; control row `IDLE` at end of run.

## 5. Idempotency, Incremental Behavior & Existing References

* **Batch guard:** resolving an already-resolved batch aborts before any write (mirrors Phase 5 `batch_already_present`). Reprocessing the same batch is always blocked.
* **xref immutability:** insert new (`source_system`, `source_customer_ref`) pairs only; existing rows are never updated.
* **Existing references in later batches** — a source ref already present in xref is **never rewritten**. The new batch's evidence for that ref is evaluated against the **existing assignment**:
  * Consistent evidence (or none) → the reference is **verified**: no new xref row, no change.
  * Evidence identifying a **different** unified ID → **identity drift**: a new `rejected_resolutions` row (this batch's `_batch_id`, reason `IDENTITY_DRIFT`); the full record definition — existing assignment preserved as a distinguished `EXISTING_ASSIGNMENT` evidence entry, new evidence recorded separately, cross-batch auditability via the grain — is in section 4.2. The xref row is untouched. Drift is surfaced for review — never silently ignored, never used to rewrite xref. Re-pointing an existing assignment remains possible only through the manual review flow (identity spec §7), never automatically.
  * **Deterministic policy for repeated source references:** first resolution wins; every later appearance of the same reference is a verification pass (consistent → verified; contradictory → drift review).
* **New references** (not in xref): new xref row via the standard algorithm, matching against the accumulated identity set (which includes every prior R5 registration).
* **Reconciliation** reports separately: previously-resolved references re-observed (verified vs drift), and new references by outcome (section 6).
* **R5 resumption:** per section 3 — resume reads the durable `RESERVED` ledger mapping and never re-allocates; the allocation frontier is `max(all ledger IDs) + 1`.
* **Views:** `CREATE OR REPLACE VIEW` — idempotent, always consistent with Silver + xref.

**Review disposition boundary (Phase 6):** Phase 6 creates the `IN_REVIEW` xref row with `unified_customer_id` NULL, allocates **no** R5 ID to it, implements **no manual review dispositions**, and never silently updates the immutable xref row. A **future review-resolution phase** must define how an approved disposition (accept A / accept B / register new) is recorded and how the original xref assignment is represented or superseded (an amended row, a superseding row, or a disposition table — undecided). That workflow is intentionally not designed or implemented in Phase 6.

* Weekly full invariant re-audit remains Phase 9.

## 6. Reconciliation Identity (verified before writes)

Per source: Silver refs = xref rows for that source = sum of outcome buckets; view rows with NULL `unified_customer_id` counted by reason (IN_REVIEW / UNRESOLVED); every non-NULL `unified_customer_id` in a view traceable to exactly one xref row. Nothing silently dropped. **Review and unresolved counts are reconciled and reported separately** (per reason code), never merged into one number. In incremental batches, reconciliation reports **four separate categories** — initial review conflicts, unresolved orphans, verified existing references, and **identity drift** — drift is never merged into the initial-conflict or orphan counts (section 4.2).

## 7. Acceptance Criteria (batch_001, exact counts — D8)

1. **xref:** 46,988 rows; PK unique; distribution exactly:

| Source | R1 | R2 | R5 | IN_REVIEW | UNRESOLVED | Total |
|---|---|---|---|---|---|---|
| CIS | 10,000 | — | 1,998 | 2 | — | 12,000 |
| accounts | 7,358 | 1,851 | — | 224 | 447 | 9,880 |
| cards | 4,316 | 878 | — | 118 | 220 | 5,532 |
| loans | 1,845 | 686 | — | 36 | 129 | 2,696 |
| digital_logins | 6,637 | 1,346 | — | 889 | 1,228 | 10,100 |
| support_tickets | 5,069 | 1,263 | — | 147 | 301 | 6,780 |
| **Product subtotal** | **25,225** | **6,024** | — | **1,414** | **2,325** | **34,988** |
| **Grand total** | **35,225** | **6,024** | **1,998** | **1,416** | **2,325** | **46,988** |

(RA1: cards R1 = 3,181 agreeing + 1,135 ordinary email differences = **4,316** — previously misstated as 4,315; the row now sums to 5,532. Product R1 = 20,132 agreeing + 5,093 ordinary = **25,225**, unchanged. All rows sum to their totals: product subtotal R1 = 7,358 + 4,316 + 1,845 + 6,637 + 5,069 = 25,225; grand total = 35,225 + 6,024 + 1,998 + 1,416 + 2,325 = 46,988.)

2. **R5:** 1,998 IDs, `CUST-100001`–`CUST-100998`, ascending `cis_customer_id` order, all `^CUST-[0-9]{6}$` and > `CUST-010000`; none reused; the 2 IN_REVIEW persons hold no ID.
3. **rejected_resolutions:** exactly 3,741 rows = 1,414 `EMAIL_REF_CONFLICT` + 2,325 `ORPHAN_NO_R1_R2` + 2 `NAME_DOB_REVIEW`; zero `EMAIL_EMAIL_CONFLICT`, `CIS_IDENTIFIER_CONFLICT`, `UNRESOLVED_GLOBAL_REF`, `IDENTITY_DRIFT`. **Review cases (1,416 = 1,414 + 2) and unresolved cases (2,325) reconciled and reported separately; identity drift is a third, separately reported category (0 rows in batch_001).** The (`source_system`, `source_customer_ref`, `reason_code`, `_batch_id`) grain keeps drift events for the same reference independently auditable across batches.
4. **Coverage:** 100% of Silver product rows whose ref resolved R1/R2 carry `unified_customer_id` in the views; rows whose ref is IN_REVIEW/UNRESOLVED carry NULL and are counted; excluded from Gold.
5. **Zero double-assignments (corrected invariants):** no (`source_system`, `source_customer_ref`) maps to two unified IDs; no R5 ID is assigned to two distinct persons; **multiple references mapping to the same unified customer are expected** (many-to-one — the purpose of resolution) and are not violations; product R1 unified IDs equal the ref's own gref.
6. **Views:** all six exist with the documented schema; row counts equal the underlying tables; Phase 5 tables byte-identical (no writes to them).
7. **Idempotency:** re-run aborts; zero new assignments; no xref or view changes.
8. **Regression:** Phase 4/5 suites green (54/54 + 8/8); D4 guard (`test_no_unified_customer_id_no_identity_resolution`) unchanged and passing.
9. **Allocation ledger & control (RA13):** exactly 1,998 `COMMITTED` ledger rows = the xref `R5_NEW` rows 1:1 (same `batch_id`, same `unified_customer_id`, `CUST-100001`–`CUST-100998`, `allocation_seq` 1–1,998 in ascending `cis_customer_id` order); zero `RESERVED`/`ABANDONING`/`ABANDONED` rows; `unified_customer_id` globally unique across the ledger; the control row exists and is `IDLE` with `active_batch_id` NULL at end of run.

## 8. Test Plan

* `tests/test_identity_resolution.py`: pure-function unit tests on synthetic mini-populations — every rule outcome and reason code, including CIS independent candidate evaluation and all three pairwise disagreements (R1-vs-R2, R1-vs-R3, R2-vs-R3 → `CIS_IDENTIFIER_CONFLICT`), mixed-evidence diversion, R4 single/multi-match → `NAME_DOB_REVIEW` (never auto-adopt), R5 ordering/resumption/no-ID-for-in-review, orphan `ORPHAN_NO_R1_R2`, `IDENTITY_DRIFT` on existing references (consistent → verified; contradictory → drift review; xref untouched), `candidate_evidence` struct construction + deterministic ordering, batch guard, allocation-ledger lifecycle (`RESERVED`→`COMMITTED`/`ABANDONED`; global ID uniqueness; frontier = max over all ledger states incl. `ABANDONED`), control-row exclusive acquisition (a concurrent check-and-set fails), resume reuses the exact reserved mapping (no re-allocation), abandonment as a resumable multi-step protocol (control `BUSY` throughout; each step idempotent; abandoned IDs never freed), allocator pre-reservation uniqueness validations (duplicate `unified_customer_id` across any states, duplicate allocation key, duplicate `allocation_seq`, xref collision — each fails closed).
* `src/identity/`: resolver, conflict detector, R5 allocator, view DDL builder — pure functions, no table access (Phase 5 style).
* Notebook `06_identity_resolution.py`: orchestrator (creates tables/views idempotently, runs guard, reconciliation, acceptance checks) — **executed only after explicit approval**, mirroring Phase 5 cadence.

## 9. Upstream Amendments — applied 2026-11-24 (this spec approved)

* `02-customer-identity-resolution.md`: §2 — R4 outcome for no-gref records → `IN_REVIEW` (D4); §2 cascade conflict note superseded by the quarantine reading (D1); §2 — CIS independent R1/R2/R3 candidate evaluation with pairwise conflict handling (`CIS_IDENTIFIER_CONFLICT`); §5 — orphan path defined (D3, `ORPHAN_NO_R1_R2`); §6 — `match_rule`/`match_confidence` enums extended with `UNRESOLVED`; §7 — incremental existing-reference behavior (first-resolution-wins, verification passes, `IDENTITY_DRIFT` review, xref never rewritten); `rejected_resolutions` schema incl. `candidate_evidence` struct and full reason-code enum; R5 allocation/recovery details (D5, section 3); status → Approved.
* `02-data-model.md`: §3 — `unified_customer_id` exposed via `*_resolved` views (D6); rejected_resolutions schema per §4.2.
* `05-silver-standardization.md`: no changes (its D4 boundary statement already defers to Phase 6).

## 10. Non-Goals

No Phase 5 modifications; no Bronze or `fraud_detection` changes; no Gold/SCD2 (Phase 7+); no fuzzy/probabilistic matching — deterministic rules only.

## 11. Review Amendment Log (2026-11-24)

* **RA1** — §7: cards R1 corrected 4,315 → **4,316** (3,181 agreeing + 1,135 ordinary); product R1 total **25,225** verified unchanged; all row and column totals mathematically verified.
* **RA2** — §2.1: CIS independent R1/R2/R3 candidate evaluation; pairwise disagreements (R1vR2, R1vR3, R2vR3) → `IN_REVIEW` (`CIS_IDENTIFIER_CONFLICT`); R4 evaluated only when R1–R3 have not resolved the record; R4 never auto-adopts. batch_001 counts unchanged (zero CIS conflicts observed). Reason code added to §4.2 enum and §8 test plan.
* **RA3** — §4.2: parallel arrays `candidate_unified_ids`/`evidence_emails` replaced by structured `candidate_evidence` (`evidence_type`/`evidence_value`/`candidate_unified_id`/`match_rule`); duplicate candidate evidence representation and deterministic ordering defined; source rows preserved via (`source_system`, native reference).
* **RA4** — §5: incremental existing-reference behavior — immutability, verification passes, `IDENTITY_DRIFT` review (xref never rewritten), new-reference handling, reconciliation split, batch guard retained.
* **RA5** — §4.1/§7: xref uniqueness invariants corrected — multiple references may map to one unified customer (expected); R5 IDs never assigned to two persons; a reference never maps to two IDs.
* **RA6** — §4.3: exact per-view join contracts (source_system values + native reference columns from actual Phase 5 schemas); IN_REVIEW vs UNRESOLVED consumer distinction; reason lookup via `rejected_resolutions`.
* **RA7** — §3: R5 recovery — `customer_xref` as the authoritative allocation history, concurrency prevention, partial-write recovery, batch retry behavior, assignment retention.
* **RA8** — §4.2/§6/§7: review cases (`EMAIL_REF_CONFLICT`, `EMAIL_EMAIL_CONFLICT`, `CIS_IDENTIFIER_CONFLICT`, `NAME_DOB_REVIEW`) distinguished from unresolved identity (`ORPHAN_NO_R1_R2`); both stored in `rejected_resolutions`, separately reconciled and reported.
* **RA9** (final review) — §3: R5 recovery made safe for interleaved batches — delete-and-replay permitted **only for the latest resolved batch**; rollback of R5 assignments prohibited once later allocations exist (partial rows retained under the preserved mapping); IDs never freed for reassignment; person→ID assignments survive every recovery path; allocation globally serialized via the single orchestrator writer, not merely guarded per batch.
* **RA10** (final review) — §5: explicit Phase 6 review-disposition boundary — `IN_REVIEW` xref rows created with no ID; no manual dispositions implemented; the immutable xref row never silently updated; a future review-resolution phase must define disposition recording and assignment representation/supersession.
* **RA11** (final review) — §4.2/§5/§6/§7: `IDENTITY_DRIFT` fully defined — `_batch_id` added to the `rejected_resolutions` grain for cross-batch auditability; existing assignment preserved as a distinguished `EXISTING_ASSIGNMENT` evidence entry; new evidence recorded separately; drift never duplicates xref rows or modifies assignments; reconciliation reports drift separately from initial conflicts and orphans.
* **RA12** (final review) — §3: R5 determinism scope stated explicitly — ascending `cis_customer_id` orders allocation within a batch; global assignment is deterministic only for the fixed serialized batch-processing order and can differ if batches are processed in a different order.
* **RA13** (final review, partial-allocation recovery) — §3.1–§3.4, §4.4, §5, §7 (criterion 9), §8: introduced `silver.r5_allocation_ledger` (grain (`batch_id`, `cis_customer_id`); global `unified_customer_id` uniqueness; `RESERVED`→`COMMITTED`/`ABANDONED`; rows never deleted) and singleton `silver.identity_resolution_control` (atomic `IDLE`→`BUSY` check-and-set — enforceable single-writer at the data layer). The complete person→ID mapping becomes durable in **one atomic ledger append before any downstream write**; a partial `customer_xref` write is resumed by joining the `RESERVED` ledger rows — the exact original assignments, never re-allocated; a `BUSY` control row blocks every subsequent batch while an earlier allocation is incomplete or its recovery unresolved; abandonment is precisely defined — ledger rows retained forever (`ABANDONED`), partial xref/quarantine rows removed batch-scoped, IDs never freed or reassigned. RA9 invariants preserved and strengthened: rollback after later allocations is permitted **only** via abandonment, whose ledger retention is the "original allocation mapping preserved" escape clause. `customer_xref` remains the authoritative resolved identity mapping, reconciled 1:1 with `COMMITTED` ledger rows. The batch_001 acceptance table and all existing counts are unchanged (criterion 9 added).
* **RA14** (final review, multi-table abandonment recovery) — §3.1 (control schema gains `active_batch_state` `RUNNING`/`ABANDONING`/`ABANDONED`; ledger state enum gains `ABANDONING`), §3.3.4, §4.4, §7, §8: abandonment is **no longer described as one atomic multi-table transaction** — Delta cannot atomically commit across the ledger, xref, quarantine, and control tables. It is an idempotent, resumable multi-step protocol of individually atomic single-table transactions, **control row `BUSY` throughout**: (1) record intent (ledger `RESERVED`→`ABANDONING`, control `active_batch_state`→`ABANDONING`); (2) mark ledger rows `ABANDONED`; (3) batch-scoped delete of partial xref/quarantine rows; (4) verify cleanup and ledger/xref consistency; (5) mark complete (control `active_batch_state`→`ABANDONED`); (6) release control. Failure after any step leaves control `BUSY`/`ABANDONING` — **every new batch aborts at acquisition while abandonment is incomplete**; re-running the protocol resumes from the interrupted step (each step idempotent) and converges. Abandoned IDs are never freed or reused.
* **RA15** (final review, enforced uniqueness) — §3.1 note + new §3.5: declared `PRIMARY KEY`/`UNIQUE` constraints are **informational** in Delta, not write-enforced; the invariants are enforced by the serialized allocator **before** the durable reservation — validated: no duplicate `unified_customer_id` across any ledger rows or states (incl. `ABANDONED`); no duplicate (`batch_id`, `cis_customer_id`); no duplicate `allocation_seq` within the batch; no collision with existing `customer_xref` assignments. **Fail-closed**: any failure aborts before the ledger append — zero rows, zero IDs consumed, control released. Single-writer protocol and durable reservation design unchanged.
