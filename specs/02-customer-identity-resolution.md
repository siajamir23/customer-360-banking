# 02 — Customer Identity Resolution

**Phase:** 2 (Specifications & Data Modeling) · **Status:** Approved (Phase 6 amendments applied 2026-11-24)  
**Amendment (Phase 6, approved 2026-11-24):** implementation-detail amendments per `06-identity-resolution.md` (RA1–RA15): R4 never auto-adopts (outcome `IN_REVIEW`, reason `NAME_DOB_REVIEW`); the §2 cascade conflict note is superseded by the quarantine reading (a valid gref→B with any distinct email→A is `IN_REVIEW`/`EMAIL_REF_CONFLICT` — never auto-merged); CIS evaluates R1/R2/R3 independently before assignment (pairwise disagreement → `CIS_IDENTIFIER_CONFLICT`); orphan path defined (`UNRESOLVED` xref row + `ORPHAN_NO_R1_R2`); xref enums extended with `UNRESOLVED`; incremental drift behavior (`IDENTITY_DRIFT`, first-resolution-wins); `rejected_resolutions` schema (§6.1); R5 allocation ledger + control row (§6.2).  
**Amendment (Phase 5, approved 2026-11-21):** phone normalization aligned with the approved Silver rule — retain the **last 10 digits** after stripping non-digits; **fewer than 10 digits → NULL**; an invalid phone value **never independently rejects** an otherwise valid record. R1–R5 cascade unchanged.  
**Depends on:** `02-source-data-contracts.md`, `phase-1-environment-inspection.md` (verified `CUST-######` domain)

## Objective

Consolidate customer references from six systems — each with its own native customer ID (`CIS-`, `CB-`, `CC-`, `LN-`, `DB-`, `CS-`), partial enterprise references, and inconsistent PII — into one **unified customer identity** in the verified enterprise `CUST-######` domain, producing `silver.customer_xref` (the crosswalk every Silver product table joins on) and the unified `customer_id` used across Gold.

**Design constraint honored:** no assumption that source systems share a customer identifier. Only ~70–85% of records carry a `global_customer_ref`; the rest are matched on PII keys or registered as new customers.

## 1. Match Key Normalization (pre-matching, in Silver)

| Key | Normalization Rule |
|-----|--------------------|
| `global_customer_ref` | Uppercase, trim; validate `^CUST-[0-9]{6}$`; null otherwise |
| Email | `lower(trim())`; strip surrounding whitespace; require `@` + domain dot; invalid → null (not a match key) |
| Phone | Strip all non-digits; keep **last 10 digits** (drops country/area prefixes); **fewer than 10 digits → NULL** (aligned with Phase 5 Silver rule, approved 2026-11-21 — never an independent rejection; the record falls through to the next rule) |
| Name | `upper(trim())`; remove punctuation; collapse internal whitespace; single canonical string (`FIRST~LAST`) |
| Date of birth | Parse all 3 contracted formats → DATE; unparseable → quarantined at validation, never used for matching |

## 2. Match Rule Cascade (deterministic, applied in priority order)

| Rule | Match Key | Condition | Confidence | Outcome |
|------|----------|-----------|------------|---------|
| **R1** | `global_customer_ref` | Exact match to an existing unified ID | `EXACT` | Adopt existing `customer_id` |
| **R2** | Email (normalized) | Exact match to an existing customer's email | `EXACT` | Adopt existing `customer_id` |
| **R3** | Phone (normalized) | Exact match to an existing customer's phone | `HIGH` | Adopt existing `customer_id` |
| **R4** | Name + DOB | Both exact (normalized) | `MEDIUM` | `IN_REVIEW` — never auto-adopt; one **or more** matches divert to review, reason `NAME_DOB_REVIEW` (Phase 6 D4) |
| **R5** | — | No rule matched | `NEW` | **Register new customer**: next `CUST-######` from `CUST-100001` upward (existing domain verified to end at `CUST-010000`; no collision by construction) |

Rules stop at first hit (cascade semantics) **only when no contradictory identity evidence exists** — conflicting evidence diverts to `quarantine.rejected_resolutions` instead of silently choosing (see §5). This quarantine reading supersedes any pure cascade-precedence reading (Phase 6 D1):

* **CIS records:** R1/R2/R3 evidence is evaluated **independently before** an identity is assigned; candidates identifying **different** unified IDs (R1-vs-R2, R1-vs-R3, R2-vs-R3) → `IN_REVIEW`, reason `CIS_IDENTIFIER_CONFLICT`. R4 is evaluated only when R1–R3 have not resolved the record, and never auto-adopts.
* **Product references:** a valid `global_customer_ref` resolving to customer B while **any** distinct email resolves to customer A ≠ B → `IN_REVIEW`, reason `EMAIL_REF_CONFLICT` — even when other evidence of the same reference agrees with B (no silent choosing).

**Match seed order:** CIS customers are resolved first (they found the master population), then Core Banking, Cards, Loans, Digital, Support — each later system matches against the accumulated identity set. Order is fixed and documented for reproducibility.

## 3. Duplicate Identification (two levels)

1. **Intra-source duplicates** (within one system): same source PK repeated (exact dupes, ~2%) or near-dupes (same normalized name+DOB or email, ~1.5%). Resolved in Silver before matching: keep one survivor per source key — **latest `ingestion`/record date wins**, tie-break on most complete row (fewest nulls).
2. **Cross-source duplicates**: same natural person appearing in multiple systems — exactly what R2–R4 resolve into one unified `customer_id`.

## 4. Conflicting Record Resolution (survivorship)

**Email-matching rules (Amendment 1 — approved 2026-11-19):** email is a match key, never a merge mandate:

* An email must represent the record's holder before it can match (no shared/role emails in the synthetic population).
* Emails are normalized (`trim` + `lower`) before any comparison (§1).
* A **missing** email is never an automatic rejection: the record simply falls through to the next rule (R3/R4 for CIS records; the review/orphan path of §5 for product rows that also lack a global ref).
* A **conflicting** email match (email → customer A while a valid global ref → customer B) is quarantined, never auto-merged.
* Email alone never overrides a contradicting higher-priority identifier.

When multiple resolved records disagree on an attribute:

* **Source precedence (authoritative-source-wins):** CIS > Core Banking > Credit Card > Loans > Digital > Support for demographics and contact (CIS is the customer master); each operational system stays authoritative for its own product facts.
* **Within CIS:** most recent record wins; non-null beats null; explicit `status`/audit fields beat derived.
* **Never overwrite silently**: every applied conflict is counted and reported in the Phase 6 validation output (conflict volume by attribute and source pair).

## 5. Unresolvable Conflicts → Human Review

A record is diverted to `quarantine.rejected_resolutions` (with full original row, both candidate identities, and reason) when:
* R2–R4 match customer A **but** the record carries a different valid `global_customer_ref` (B ≠ A).
* R4 (name+DOB) matches **multiple** distinct existing customers.
* Phone matches one customer and email matches a different one (disagreeing HIGH/EXACT keys).
* No resolvable evidence at all — no valid `global_customer_ref` and no email resolving to any customer (**orphan**, Phase 6 D3): xref row with `match_rule = UNRESOLVED` plus a `rejected_resolutions` row, reason `ORPHAN_NO_R1_R2`, recording the evidence tried.

Review disposition options (documented, applied manually later): accept A, accept B, or register new. Diverted records **do not** block their source batch; product rows whose customer is in review resolve with `unified_customer_id = NULL` and are excluded from Gold (counted in reconciliation, never silently dropped).

## 6. Output — `silver.customer_xref` Schema

**Grain:** one row per (`source_system`, `source_customer_ref`). **PK:** composite of both.

| Column | Type | Description |
|--------|------|-------------|
| `source_system` | STRING | `CIS` / `CORE_BANKING` / `CREDIT_CARD` / `LOANS` / `DIGITAL` / `SUPPORT` |
| `source_customer_ref` | STRING | Native PK of the customer in that system (`CIS-#####`, `CB-#####`, …) |
| `global_customer_ref_raw` | STRING | Ref as provided in source (nullable; for audit) |
| `unified_customer_id` | STRING | Resolved `CUST-######`; NULL if in review |
| `match_rule` | STRING | `R1_GLOBAL_REF` / `R2_EMAIL` / `R3_PHONE` / `R4_NAME_DOB` / `R5_NEW` / `IN_REVIEW` / `UNRESOLVED` (orphan refs, Phase 6 D3; `R4_NAME_DOB` rows cannot occur — R4 diverts to `IN_REVIEW`) |
| `match_confidence` | STRING | `EXACT` / `HIGH` / `MEDIUM` / `NEW` / `IN_REVIEW` / `UNRESOLVED` |
| `match_timestamp` | TIMESTAMP | Resolution run time |
| `_batch_id` | STRING | Lineage |

**Invariants:** every Silver product table's `unified_customer_id` comes from this table (single source of truth — exposed via the `*_resolved` views, Phase 6 D6); each (`source_system`, `source_customer_ref`) maps to **at most one** `unified_customer_id` — **multiple references may map to the same unified customer** (many-to-one, expected); an `R5_NEW` ID is never assigned to two distinct persons and never reused.

### 6.1 `quarantine.rejected_resolutions` (Phase 6 output — authoritative schema in `06-identity-resolution.md` §4.2)

**Grain:** one row per (`source_system`, `source_customer_ref`, `reason_code`, `_batch_id`) — `_batch_id` in the grain keeps identity-drift events for the same reference across batches independently auditable. Columns: `source_system`, `source_customer_ref`, `reason_code` (`EMAIL_REF_CONFLICT` / `EMAIL_EMAIL_CONFLICT` / `CIS_IDENTIFIER_CONFLICT` / `NAME_DOB_REVIEW` / `ORPHAN_NO_R1_R2` / `UNRESOLVED_GLOBAL_REF` / `IDENTITY_DRIFT`), `candidate_evidence ARRAY<STRUCT<evidence_type STRING, evidence_value STRING, candidate_unified_id STRING, match_rule STRING>>` — one entry per distinct evidence value (`GLOBAL_REF`/`EMAIL`/`PHONE`/`NAME_DOB`, plus a distinguished `EXISTING_ASSIGNMENT` entry on drift records), deterministic ordering — `global_customer_ref_raw`, `_batch_id`, `_quarantine_timestamp`. Review cases (all reasons except `ORPHAN_NO_R1_R2`) and unresolved identity (`ORPHAN_NO_R1_R2`) are stored together but reconciled and reported separately. Original source rows remain in Silver, recoverable by (`source_system`, native reference).

### 6.2 R5 allocation & recovery (summary — full detail in `06-identity-resolution.md` §3)

`R5_NEW` assignments are reserved durably in `silver.r5_allocation_ledger` (grain (`batch_id`, `cis_customer_id`); `unified_customer_id` globally unique across all states and batches; states `RESERVED` → `COMMITTED` | `ABANDONING` → `ABANDONED`; rows never deleted) in **one atomic append before any downstream write**; downstream xref rows are written by joining the ledger, never by re-running the allocator. Allocation is globally serialized by a singleton `silver.identity_resolution_control` row via an **atomic check-and-set** (`IDLE` → `BUSY`) — enforceable single-writer. Recovery: **resume** (re-derive downstream writes from the `RESERVED` ledger mapping — the exact original assignments), **delete-and-replay** (latest batch only, enforced on the ledger), or **abandonment** (idempotent, resumable six-step protocol; control row `BUSY` throughout; abandoned IDs never freed or reused). Declared table constraints are informational — the allocator validates uniqueness (no duplicate ID across any state, no duplicate allocation key, no duplicate sequence, no xref collision) and fails closed before the reservation append. Determinism: ascending `cis_customer_id` within a batch; global assignment is deterministic for the fixed serialized batch-processing order only.

## 7. Incremental Behavior

Identity resolution is rerun per batch: new source records match against the existing xref + accumulated identities; existing xref rows are immutable (an identity, once assigned, is never re-pointed without the review flow). **Existing-reference behavior (Phase 6):** first resolution wins — every later appearance of an already-resolved reference is a verification pass: consistent evidence confirms the assignment (no new xref row); contradictory evidence is recorded as **identity drift** (a `rejected_resolutions` row, reason `IDENTITY_DRIFT`, with the existing assignment preserved as a distinguished `EXISTING_ASSIGNMENT` evidence entry) and surfaced for review — never silently ignored, never used to rewrite xref. Reconciliation reports initial conflicts, orphans, verified references, and drift as separate categories. Weekly full-audit query re-verifies invariants (Phase 9).

## Assumptions

1. Native source refs (`CB-#####` etc.) are unique **within** their system (each system's PK) — cross-system uniqueness is not assumed anywhere.
2. The enterprise `CUST-######` registry beyond `CUST-010000` is free for assignment (verified: fraud platform ends at `CUST-010000`); R5 assigns new IDs starting at **`CUST-100001`**.
3. Email is globally unique per natural person in the synthetic data except for documented conflicts (~3%) — those exercise §5.
4. The shared population is **12,000 persons**: all 10,000 existing fraud-platform customers seeded read-only with global refs (100% coverage of the verified domain) + 2,000 new persons without global refs (R5 candidates).

## Acceptance Criteria

1. 100% of non-quarantined Silver product rows carry a `unified_customer_id` traceable to exactly one xref row.
2. `R1_GLOBAL_REF` matches equal the source `global_customer_ref` carry-rate (±0.5%); remainder split across R2/R3/R4/R5 per design targets.
3. Zero double-assignments: no `(source_system, source_customer_ref)` with two unified IDs; no unified ID assigned twice by R5.
4. Conflict and review volumes are reported and non-zero (defect injection must surface in metrics).
5. All `R5_NEW` IDs conform to `^CUST-[0-9]{6}$` and are > `CUST-010000`.
