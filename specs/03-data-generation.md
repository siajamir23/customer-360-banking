# 03 — Synthetic Multi-Source Data Generation

**Phase:** 3 (Synthetic Data Generation) · **Status:** **Proposed — awaiting approval**  
**Depends on:** all five Phase-2 specs (`02-*`)

## Objective

Produce six reproducible, entirely synthetic banking source datasets (CSV) that exactly conform to the approved source contracts, carry the controlled defects at the approved rates, and are genuinely reconcilable across systems — the source systems for the entire platform. No Bronze/Silver/Gold/Quarantine tables are created; nothing in the `fraud_detection` catalog is written.

---

## ⚠ Proposed Amendment 1 (requires approval — flagged, not silently applied)

**Gap discovered while designing this phase:** the five product contracts (accounts, cards, loans, digital logins, support cases) carry only a native customer ref and a `global_customer_ref` — **no PII match key**. But identity resolution's R1 (global ref) has a 70–85% carry rate, and R2–R4 keys (email/phone/name+DOB) exist only on CIS. As specced, **15–30% of product rows would be deterministically unresolvable**, contradicting the approved ~2% orphan rate.

**Amendment:** add one column to each of the five product contracts:

| Column | Type | Nullable | Rule |
|--------|------|----------|------|
| `customer_email` | STRING | Yes | The account/card/loan/ticket holder's email **as recorded by that source system**; populated on ~85% of rows; same normalization/validation as CIS email; ~5% invalid |

Rationale: real core-banking/card/loan extracts do carry holder email; it is the only cross-system key that makes R2 meaningful outside CIS; it also feeds the approved conflicting-key quarantine flow (~3% of rows carry an email belonging to a different person than their global ref — deliberate identity-conflict exercise, exactly the case you endorsed quarantining rather than auto-merging).

With the amendment: product rows resolve via R1 (global ref) or R2 (email); engineered residual orphans ≈ 2% (rows with neither a resolvable ref nor a matching email).

---

## 1. Generator Architecture (5 layers, single orchestrator notebook)

**Notebook:** `notebooks/01_generate_synthetic_data` (numbered like the fraud project; mirrors its proven pattern).

> Why one orchestrator notebook with six clearly-separated generator sections instead of six notebooks: the shared population layer must exist exactly once; orchestration stays trivial to run top-to-bottom for portfolio reviewers; per-source separation is achieved via independent, independently-seeded generator functions + config entries. (Six separate notebooks offered as an alternative if preferred.)

```
Layer 1  CONFIG (single dict, top of notebook — no magic numbers below it)
         ├─ base_seed = 360          ├─ batch_id = "batch_001"
         ├─ output volume path       ├─ per-source row targets
         └─ per-defect rates (all from the approved contract table)

Layer 2  SHARED POPULATION (the reconciliation backbone)
         10,000 synthetic natural persons:
         • 8,000 seeded READ-ONLY from fraud_detection.silver.customers
           (global refs CUST-000001…CUST-008000; age/country/registration_date
            aligned so enrichment never contradicts the fraud platform)
         • 2,000 new Faker-generated persons (no global ref → Phase 6 R5 assigns
           CUST-100001+ per the approved collision-free strategy)
         Each person carries canonical PII (name, dob, email, phone, address)
         used consistently by every source that knows them.

Layer 3  SIX SOURCE GENERATORS (independent functions)
         cis_customers() · core_accounts() · credit_cards()
         loan_accounts() · digital_logins() · support_tickets()
         Each: selects its person subset → assigns ONE native ref per person
         per source (stable across that person's rows — enables xref bootstrap)
         → generates business attributes (Faker + ranges) → returns DataFrame.

Layer 4  DEFECT INJECTION ENGINE (shared utilities, applied per contract)
         mixed_date_format() · null_out() · exact_duplicate() ·
         near_duplicate() · corrupt_email() · format_phone() · orphan_ref()
         → all rates read from CONFIG; all defects reproduce on rerun.

Layer 5  VALIDATION SUITE (runs last, fails loudly — §6)
```

**Libraries:** `Faker` (names, emails, cities, addresses, device/browser names), Python `random` + `pandas` (composability, CSV write). Faker instances are **seeded per generator** (`Faker.seed(base_seed + source_offset)`), so a change to one source's config never shifts another source's data — a property the validation suite asserts.

## 2. Determinism & Reconfiguration Contract

* Fixed `base_seed`; every random draw flows from a seeded generator — rerunning with identical CONFIG yields **byte-identical CSVs** (asserted via SHA-256 of files).
* All sizes configurable in CONFIG (default targets below); changing one source's target re-randomizes only that source (per-generator seed independence).
* `batch_id` parameterized: batch 2 with ~5% attribute drift is generated in Phase 7 to exercise SCD2 — not now.

## 3. Directory & Output Layout

```
Unity Catalog (Phase 3 creates ONLY these, idempotently):
  catalog  customer_360
  schema   customer_360.default
  volume   customer_360.default.raw_data

Files (UTF-8 CSV, header row, comma):
  /Volumes/customer_360/default/raw_data/batch_001/customers_raw.csv
  /Volumes/customer_360/default/raw_data/batch_001/accounts_raw.csv
  /Volumes/customer_360/default/raw_data/batch_001/cards_raw.csv
  /Volumes/customer_360/default/raw_data/batch_001/loans_raw.csv
  /Volumes/customer_360/default/raw_data/batch_001/digital_logins_raw.csv
  /Volumes/customer_360/default/raw_data/batch_001/support_cases_raw.csv

Repo (versioned):
  notebooks/01_generate_synthetic_data    (the generator)
  specs/03-data-generation.md             (this spec)
```

> Bronze/Silver/Gold/Quarantine schemas are NOT created in Phase 3 — `00_environment_setup` (Phase 4) adds them idempotently (`CREATE ... IF NOT EXISTS`). Batch-scoped folders keep later incremental batches cleanly separated for Bronze's repeatable ingestion.

## 4. Per-Source Generation Targets

| Source | Persons covered | Rows (masters) | + dupes (raw rows) | Native ref | Global-ref carry | Email carry* |
|--------|----------------|----------------|--------------------|------------|------------------|--------------|
| CIS customers | 10,000 (all) | 10,000 | +350 ≈ **10,350** | `CIS-#####` | 80% (seeded 8,000) | ~95% (contract)
| Core accounts | ~8,000 | 12,000 (1–2/person) | +240 ≈ **12,240** | `CB-#####` | 75% | ~85% |
| Credit cards | ~4,500 | 6,000 | +120 ≈ **6,120** | `CC-#####` | 80% | ~85% |
| Loans | ~2,200 | 2,500 | +50 ≈ **2,550** | `LN-#####` | 70% | ~85% |
| Digital logins | ~7,500 | 50,000 (~6.7 events/person, 12-mo window) | +1,000 ≈ **51,000** | `DB-#####` | 85% | ~85% |
| Support tickets | ~5,500 | 8,000 (1–2/person) | +160 ≈ **8,160** | `CS-#####` | 75% | ~85% |

\* email carry applies where the row's holder is known; the two columns are engineered so that rows with **neither** a resolvable global ref **nor** a matching email ≈ **2%** (orphans). Business attribute ranges (balances, limits, rates, statuses, domains) come verbatim from `02-source-data-contracts.md`.

## 5. Defect Injection (rates from the approved contract table)

| Defect | Target rate | Implementation |
|--------|-------------|----------------|
| Exact duplicate rows | 2% | Full-row copies appended, order shuffled deterministically |
| Near-duplicate rows | 1.5% | Copy with exactly one attribute altered (e.g., email case) |
| Missing values | 5–10% per nullable column | `null_out()` with per-column rate from CONFIG |
| Mixed date formats | 60% ISO / 25% DD/MM/YYYY / 15% MM-DD-YYYY | Applied to every date/datetime column |
| Invalid emails | ~5% of emails | Drop `@`, break domain dot, inject whitespace |
| Phone format variants | 3 formats | Digits identical, presentation varies (so normalization recovers them) |
| Identity conflicts | ~3% of rows with both keys | Email belongs to a different person than the global ref → the quarantine-not-merge case |
| Referential orphans | ~2% | No valid global ref + email matches no known person |

## 6. Validation Suite (end of notebook; asserts, not prints)

| # | Check | Expected |
|---|-------|----------|
| 1 | Row counts per file | Within ±2% of targets in §4 |
| 2 | Schema conformance | Exact column names **and order** per contract (incl. `customer_email` if amendment approved) |
| 3 | PK format regex | 100% conform (dupes share their master's PK by design) |
| 4 | Duplicate rates | Exact ≈ 2% ±0.5pp; near ≈ 1.5% ±0.5pp (full-row hash / PK counts) |
| 5 | Enum domains | 0 out-of-domain values in every enum column |
| 6 | Date format mix | 60/25/15 ±5pp per date column |
| 7 | Missing-value rates | Within ±2pp of per-column targets |
| 8 | Global-ref referential integrity | Every populated `global_customer_ref` **exists in** `fraud_detection.silver.customers` (read-only query) |
| 9 | Cross-source reconciliation preview | ~98% of product rows resolvable via R1 or R2; orphans ≈ 2% |
| 10 | Identity-conflict count | ~3% of dual-key rows (they must exist — the quarantine path needs material) |
| 11 | Determinism | Same-CONFIG rerun → identical SHA-256 per file |
| 12 | Seed independence | Changing one source's size re-hashes only that file |

Final output: a displayed **defect & reconciliation report** — one row per source: rows, dupes, missing %, format mix, invalid emails, orphans, conflicts.

## 7. Out of Scope (explicit)

No Bronze/Silver/Gold/Quarantine tables or schemas · no writes to `fraud_detection` (only read-only reads of `silver.customers` for seeding/integrity checks) · no batch-2 drift (Phase 7) · no dashboards.

## Acceptance Criteria

1. Six CSVs exist under `raw_data/batch_001/` with the §4 row counts and contract-exact schemas.
2. All 12 validation checks pass on the default config; failures raise, not warn.
3. Rerunning the notebook changes nothing (byte-identical files, verified by hash).
4. Global refs referentially resolve against the fraud platform; new persons carry no global ref (reserved for Phase 6 R5).
5. The defect & reconciliation report is displayed and matches the injected design rates.
