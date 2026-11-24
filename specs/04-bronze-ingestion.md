# 04 — Bronze Ingestion Pipelines

**Phase:** 4 (Bronze Ingestion) · **Status:** Awaiting approval  
**Depends on:** `03-data-generation.md` (approved, executed), `02-source-data-contracts.md`, `02-data-model.md` §1  
**Inspection date:** 2026-11-20 (actual outputs, not assumed)

## 1. Objective & Scope

Ingest the six Phase 3 CSVs from `customer_360.default.raw_data` into six persistent Delta tables in a new `customer_360.bronze` schema — **raw, immutable, as-ingested**: every source column preserved as STRING with its original value, plus lineage metadata. No business transformations, no deduplication, no drops, no type casting.

**Out of scope:** Silver/Gold/Quarantine tables; any write to `fraud_detection`; any modification of the source CSVs; typing or validation of values (Silver concern).

## 2. Verified Inputs (inspected 2026-11-20)

Location: `/Volumes/customer_360/default/raw_data/batch_001/` (7 files: 6 CSVs + `_manifest.json`).

| File | Rows (actual = manifest) | Bytes | Business columns (verified header order) |
|------|--------------------------|-------|------------------------------------------|
| customers_raw.csv | 12,420 = 12,420 | 2,266,259 | 18 |
| accounts_raw.csv | 14,490 = 14,490 | 1,759,868 | 12 |
| cards_raw.csv | 6,831 = 6,831 | 876,598 | 11 |
| loans_raw.csv | 2,898 = 2,898 | 355,450 | 13 |
| digital_logins_raw.csv | 56,925 = 56,925 | 7,340,960 | 12 |
| support_cases_raw.csv | 9,315 = 9,315 | 978,187 | 11 |

Manifest (read, verified): `batch_001`, `base_seed 360`, `anchor_date 2026-11-30`, population 10,000 seeded + 2,000 new, 68/68 generation checks passed, per-file SHA-256 recorded. Manifest row counts are the **authoritative reconciliation baseline** for Bronze.

Nullability observed in the actual files (e.g., `customers.global_customer_ref` 2,061 nulls; `digital_logins.customer_email` 9,796 nulls; `support_tickets.csat_score` 2,321 nulls) is **expected defect engineering** (spec 02) and must arrive in Bronze unchanged.

## 3. Target Tables

Delta tables in schema `customer_360.bronze` (schema created by this phase; catalog `customer_360` exists):

| Table | Source file | `_source_system` |
|-------|-------------|-------------------|
| `customer_360.bronze.customers` | customers_raw.csv | `cis` |
| `customer_360.bronze.accounts` | accounts_raw.csv | `core_banking` |
| `customer_360.bronze.cards` | cards_raw.csv | `card_system` |
| `customer_360.bronze.loans` | loans_raw.csv | `loan_system` |
| `customer_360.bronze.digital_logins` | digital_logins_raw.csv | `digital_banking` |
| `customer_360.bronze.support_tickets` | support_cases_raw.csv | `support_system` |

**Decision D1 — naming:** the Phase 4 instructions name the last table `bronze.support_tickets`; the data-model spec (§1) and all silver/quarantine references use `support_cases`. Default in this spec: **`support_tickets`** per the latest explicit instruction; on approval the data-model spec will be amended so all layers use one name (recommended: `support_tickets` everywhere, or `support_cases` everywhere — must not diverge across layers).

## 4. Table Schemas (schema enforcement)

Every table: **all business columns STRING, in verified file order**, followed by metadata columns. Enforced by an explicit `StructType` per source — the CSV header is read first and compared name-by-name (and order) against the contract; **any mismatch (missing, extra, renamed, reordered column) aborts ingestion before any write**.

`customers` (18 + 5): `cis_customer_id`, `global_customer_ref`, `first_name`, `middle_name`, `last_name`, `date_of_birth`, `gender`, `email`, `phone`, `address_line1`, `city`, `country`, `postal_code`, `registration_date`, `kyc_status`, `customer_segment`, `annual_income`, `status`  
`accounts` (12 + 5): `account_id`, `cb_customer_ref`, `global_customer_ref`, `customer_email`, `account_number`, `account_type`, `currency`, `balance`, `status`, `open_date`, `last_activity_date`, `branch_code`  
`cards` (11 + 5): `card_id`, `cc_customer_ref`, `global_customer_ref`, `customer_email`, `card_number_masked`, `card_type`, `credit_limit`, `outstanding_balance`, `card_status`, `issue_date`, `expiry_date`  
`loans` (13 + 5): `loan_id`, `ln_customer_ref`, `global_customer_ref`, `customer_email`, `loan_type`, `principal_amount`, `outstanding_balance`, `interest_rate`, `term_months`, `monthly_installment`, `start_date`, `end_date`, `repayment_status`  
`digital_logins` (12 + 5): `login_id`, `db_customer_ref`, `global_customer_ref`, `customer_email`, `login_timestamp`, `channel`, `device_id`, `device_type`, `os`, `browser`, `login_status`, `session_duration_minutes`  
`support_tickets` (11 + 5): `ticket_id`, `cs_customer_ref`, `global_customer_ref`, `customer_email`, `channel`, `category`, `priority`, `status`, `created_date`, `resolved_date`, `csat_score`

Managed Delta tables (no explicit external location) — storage location verified post-write via `DESCRIBE DETAIL` (Decision D3; external volumes rejected: no requirement to reference a fixed path, managed keeps lifecycle simple).

## 5. Metadata Columns (every table)

| Column | Type | Value |
|--------|------|-------|
| `_source_system` | STRING | constant per table (§3) |
| `_batch_id` | STRING | `batch_001` (from manifest) |
| `_ingestion_timestamp` | TIMESTAMP | pipeline start time (single value per run, per table) |
| `_source_file` | STRING | volume URI of the CSV (constant per table; Auto Loader–style lineage) |
| `_ingestion_date` | DATE | `to_date(_ingestion_timestamp)` — **Decision D2**: data-model spec §1 defines five lineage columns; Phase 4 instructions list four. Default: include `_ingestion_date` for cross-layer convention consistency (Silver/Gold specs assume it). |

## 6. Batch Management & Idempotency

* **Batch unit:** one `_batch_id` = one complete ingestion of the six files. A batch is written table-by-table (each `.write.format("delta").mode("append")` is its own atomic Delta transaction).
* **Duplicate-batch guard (fail closed, never silent):** before any write, each table is checked for existing `_batch_id`. If the batch is already present in *any* target table, ingestion **aborts with an explicit error** listing affected tables. No overwrite, no partial append of a repeated batch.
* **Idempotency contract:** a *successful* batch is never re-appended; a *failed* run leaves only fully-committed tables (Delta atomicity per table), and the duplicate-batch guard reports exactly which tables landed — recovery is an explicit, logged procedure: `DELETE FROM <table> WHERE _batch_id = '<batch>'` then re-run (documented in the run report, never executed automatically).
* **Append-only across batches:** future batches append new rows with new `_batch_id`; previously ingested rows are never modified.
* **Source immutability:** CSVs are opened read-only; volume files never rewritten (acceptance check re-hashes them against the manifest after ingestion).

## 7. Error Handling (documented matrix)

| Condition | Detection | Handling |
|-----------|-----------|----------|
| Missing file / manifest | pre-check listing | **Abort** before any write |
| Header ≠ contract schema | header read + exact name/order compare | **Abort** before any write, naming the offending columns |
| Row count ≠ manifest count | post-read `count()` vs `_manifest.json` | **Abort** before any write (fail closed; no partial trust) |
| Malformed CSV record | `mode=PERMISSIVE` + all-STRING contract | Row **preserved** (all-STRING means no value is ever coerced or dropped); anomaly count logged in run report; no `mode=FAILFAST`, no discard |
| Duplicate batch | `_batch_id` guard | **Abort** with table list (§6) |
| Empty / all-null column | post-write profile | Warning in run report (unexpected per contract) — rows still preserved |

Rationale: with an all-STRING contract the only true "malformed" record is a structural CSV fault (bad quoting/column count); PERMISSIVE preserves such rows for audit rather than dropping them, and the row-count reconciliation guarantees nothing was silently lost.

## 8. Validation & Acceptance Criteria (post-ingestion)

1. `customer_360.bronze` contains **exactly** the six tables; all are Delta (`SHOW TABLES`, `DESCRIBE DETAIL` provider/location).
2. Each table's schema matches §4 exactly — column names, order, business types all STRING, five metadata columns appended (assert via `describe`).
3. Per-table row count == manifest count (customers 12,420 · accounts 14,490 · cards 6,831 · loans 2,898 · digital_logins 56,925 · support_tickets 9,315).
4. Metadata completeness: exactly one distinct `_batch_id` (`batch_001`), `_source_system`/`_source_file` single-valued and 100% populated, `_ingestion_timestamp` non-null, `_ingestion_date` == date of `_ingestion_timestamp`.
5. **Fidelity:** source duplicates and engineered nulls survive — full-row duplicate counts in Bronze equal Phase 3 injected duplicate counts (verified via count minus distinct); spot-check sample rows byte-equal to CSV rows on all business columns.
6. Source CSVs untouched: post-run SHA-256 of all six files equal `_manifest.json` values.
7. Idempotency guard verified: a re-run attempt aborts without writing (negative test).

## 9. Implementation Plan (after approval)

Follows Phase 3 conventions (modular `src/` + thin orchestrator + tests):

1. `src/config/bronze_config.py` — table names, file mapping, `_source_system` codes, schema contracts (§4), manifest path
2. `src/ingestion/bronze_loader.py` — pre-checks, header validation, guard, load, profile
3. `notebooks/04_bronze_ingestion.py` — thin orchestrator: create schema, run loader, emit run report + acceptance checks
4. `tests/test_bronze_ingestion.py` — schema-contract unit tests + idempotency negative test (pytest-compatible)
5. Docs: ingestion run report appended to `docs/`; `data/README.md` updated with bronze locations

Execution: create schema + tables, ingest batch_001, run all acceptance checks (§8), present validation output — **then** commit and push.
