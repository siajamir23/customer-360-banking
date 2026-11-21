# 02 — Customer Identity Resolution

**Phase:** 2 (Specifications & Data Modeling) · **Status:** In review  
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
| **R4** | Name + DOB | Both exact (normalized) | `MEDIUM` | Adopt existing `customer_id` |
| **R5** | — | No rule matched | `NEW` | **Register new customer**: next `CUST-######` from `CUST-100001` upward (existing domain verified to end at `CUST-010000`; no collision by construction) |

Rules stop at first hit (cascade semantics). If a record hits a rule but with a **conflicting** `global_customer_ref` (R2–R4 match to customer A while carrying a ref of customer B), the record goes to `quarantine.rejected_resolutions` instead of silently choosing (see §5).

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

Review disposition options (documented, applied manually later): accept A, accept B, or register new. Diverted records **do not** block their source batch; product rows whose customer is in review resolve with `unified_customer_id = NULL` and are excluded from Gold (counted in reconciliation, never silently dropped).

## 6. Output — `silver.customer_xref` Schema

**Grain:** one row per (`source_system`, `source_customer_ref`). **PK:** composite of both.

| Column | Type | Description |
|--------|------|-------------|
| `source_system` | STRING | `CIS` / `CORE_BANKING` / `CREDIT_CARD` / `LOANS` / `DIGITAL` / `SUPPORT` |
| `source_customer_ref` | STRING | Native PK of the customer in that system (`CIS-#####`, `CB-#####`, …) |
| `global_customer_ref_raw` | STRING | Ref as provided in source (nullable; for audit) |
| `unified_customer_id` | STRING | Resolved `CUST-######`; NULL if in review |
| `match_rule` | STRING | `R1_GLOBAL_REF` / `R2_EMAIL` / `R3_PHONE` / `R4_NAME_DOB` / `R5_NEW` / `IN_REVIEW` |
| `match_confidence` | STRING | `EXACT` / `HIGH` / `MEDIUM` / `NEW` / `IN_REVIEW` |
| `match_timestamp` | TIMESTAMP | Resolution run time |
| `_batch_id` | STRING | Lineage |

**Invariants:** every Silver product table's `unified_customer_id` comes from this table (single source of truth); a `source_customer_ref` never maps to two `unified_customer_id`s; `R5_NEW` IDs are never reused.

## 7. Incremental Behavior

Identity resolution is rerun per batch: new source records match against the existing xref + accumulated identities; existing xref rows are immutable (an identity, once assigned, is never re-pointed without the review flow — prevents identity drift). Weekly full-audit query re-verifies invariants (Phase 9).

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
