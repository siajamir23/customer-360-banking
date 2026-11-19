# 03 — Synthetic Multi-Source Data Generation

**Phase:** 3 (Synthetic Data Generation) · **Status:** **Approved 2026-11-19 with adjustments** — population 12,000 (all 10,000 fraud customers + 2,000 new); Amendment 1 approved with binding email-matching rules; modular `src/` layout  
**Depends on:** all five Phase-2 specs (`02-*`)

## Objective

Produce six reproducible, entirely synthetic banking source datasets (CSV) that exactly conform to the approved source contracts, carry the controlled defects at the approved rates, and are genuinely reconcilable across systems — the source systems for the entire platform. No Bronze/Silver/Gold/Quarantine tables are created; nothing in the `fraud_detection` catalog is written.

---

## ✅ Amendment 1 — APPROVED 2026-11-19 (with binding email-matching rules)

**Gap (found while designing this phase):** the five product contracts (accounts, cards, loans, digital logins, support cases) carried only a native customer ref and a `global_customer_ref` — no PII match key. R1 (global ref) has a 70–85% carry rate and R2–R4 keys existed only on CIS, so 15–30% of product rows would have been deterministically unresolvable, contradicting the approved ~2% orphan rate.

**Amendment (approved):** one nullable column added to each of the five product contracts:

| Column | Type | Nullable | Rule |
|--------|------|----------|------|
| `customer_email` | STRING | Yes | The holder's email **as recorded by that source system**; ~85% populated; ~5% invalid; same normalization as CIS email |

**Approved email-matching rules (binding):**

1. `customer_email` must represent the account/card/loan/login/ticket holder (or associated customer).
2. Emails are normalized before matching: trim whitespace, then lowercase.
3. A missing email is **never** an automatic rejection — the record falls through to the next match rule.
4. Conflicting email matches go to quarantine.
5. Customers are never automatically merged on email alone when other identifiers contradict one another.

With the amendment: product rows resolve via R1 (global ref) or R2 (email); engineered residual orphans ≈ 2% (rows with neither a resolvable ref nor a matching email).

---

## 1. Generator Architecture (5 layers, thin orchestrator + modular `src/`)

**Orchestrator:** `notebooks/03_generate_source_data.py` — a thin notebook that imports the versioned `src/` modules and runs them top-to-bottom; all logic lives in modular, independently-seeded modules:

```
customer-360-banking/
├── specs/03-data-generation.md
├── src/
│   ├── config/
│   │   └── generator_config.py      # CONFIG dict — seeds, sizes, rates, paths (single source of truth)
│   ├── generators/
│   │   ├── customers.py             # CIS customer master
│   │   ├── accounts.py              # Core Banking deposit accounts
│   │   ├── cards.py                 # Credit Card System
│   │   ├── loans.py                 # Loan Management System
│   │   ├── digital_banking.py       # Digital Banking login events
│   │   └── support.py               # Customer Support tickets
│   └── utilities/
│       ├── identity.py             # shared population builder + email/phone/name normalization
│       ├── defects.py               # mixed dates, nulls, dupes, corrupt emails, conflicts, orphans
│       └── validation.py            # contract assertions + reconciliation report
├── notebooks/
│   └── 03_generate_source_data.py   # thin orchestrator — no logic
├── tests/
│   └── test_data_generation.py      # pytest-runnable contract tests (also executed by the orchestrator)
└── data/
    └── README.md                    # documents UC-volume outputs, batch folders, validation reports
```

```
Layer 1  CONFIG (single dict, src/config/generator_config.py — no magic numbers anywhere else)
         ├─ base_seed = 360          ├─ batch_id = "batch_001"
         ├─ output volume path       ├─ per-source row targets
         └─ per-defect rates (all from the approved contract table)

Layer 2  SHARED POPULATION (the reconciliation backbone)
         12,000 synthetic natural persons:
         • ALL 10,000 existing fraud customers, seeded READ-ONLY from
           fraud_detection.silver.customers (CUST-000001…CUST-010000;
           100% integration coverage; age/country/registration_date
           aligned so enrichment never contradicts the fraud platform)
         • 2,000 new Faker-generated persons (no global ref → Phase 6 R5
           assigns CUST-100001+ per the approved collision-free strategy),
           demonstrating new-customer onboarding
         Each person carries canonical PII (name, dob, email, phone, address)
         used consistently by every source that knows them.

Layer 3  SIX SOURCE GENERATORS (independent modules, one per source system)
         src/generators/{customers,accounts,cards,loans,digital_banking,support}.py
         Each: selects its person subset → assigns ONE native ref per person
         per source (stable across that person's rows — enables xref bootstrap)
         → generates business attributes (Faker + ranges) → returns DataFrame.

Layer 4  DEFECT INJECTION ENGINE (src/utilities/defects.py, applied per contract)
         mixed_date_format() · null_out() · exact_duplicate() ·
         near_duplicate() · corrupt_email() · format_phone() · orphan_ref()
         → all rates read from CONFIG; all defects reproduce on rerun.

Layer 5  VALIDATION SUITE (src/utilities/validation.py — runs last, fails loudly)
```

**Libraries:** `Faker` (names, emails, cities, addresses, device/browser names), Python `random` + `pandas` (composability, CSV write). Faker instances are **seeded per generator** (`Faker.seed(base_seed + source_offset)`), so a change to one source's config never shifts another source's data — a property the validation suite asserts.

## 2. Determinism & Reconfiguration Contract

* Fixed `base_seed = 360`; every random draw flows from a seeded generator — rerunning with identical CONFIG yields **byte-identical CSVs** (asserted via SHA-256 of files).
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
  notebooks/03_generate_source_data.py   (thin orchestrator)
  src/{config,generators,utilities}/      (modular implementation — see §1)
  tests/test_data_generation.py           (contract tests)
  data/README.md                          (outputs & validation documentation)
  specs/03-data-generation.md             (this spec)
```

> Bronze/Silver/Gold/Quarantine schemas are NOT created in Phase 3 — `00_environment_setup` (Phase 4) adds them idempotently (`CREATE ... IF NOT EXISTS`). Batch-scoped folders keep later incremental batches cleanly separated for Bronze's repeatable ingestion.

## 4. Per-Source Generation Targets

| Source | Persons covered (of 12,000) | Rows (masters) | + dupes (raw rows) | Native ref | Global-ref carry | Email carry* |
|--------|----------------|----------------|--------------------|------------|------------------|--------------|
| CIS customers | 12,000 (all) | 12,000 | +420 ≈ **12,420** | `CIS-#####` | 83% (all 10,000 seeded) | ~95% (contract) |
| Core accounts | ~9,600 (80%) | 14,000 (1–2/person) | +490 ≈ **14,490** | `CB-#####` | 75% | ~85% |
| Credit cards | ~5,400 (45%) | 6,600 | +231 ≈ **6,831** | `CC-#####` | 80% | ~85% |
| Loans | ~2,640 (22%) | 2,800 | +98 ≈ **2,898** | `LN-#####` | 70% | ~85% |
| Digital logins | ~9,000 (75%) | 55,000 (~6 events/person, 12-mo window) | +1,925 ≈ **56,925** | `DB-#####` | 85% | ~85% |
| Support tickets | ~6,600 (55%) | 9,000 (1–2/person) | +315 ≈ **9,315** | `CS-#####` | 75% | ~85% |

**Row-count formula (documented — review 2026-11-19):** raw rows = masters × (1 + 2% exact duplicates + 1.5% near-duplicates) = **masters × 1.035**. The only records appended beyond masters are the two contracted duplicate-injection types; no other record types exist. (The earlier draft quoted masters × 1.02 for five sources because it counted only the 2% exact-duplicate injection; all six now use the full 3.5% formula, matching validation check 4.)

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

## 6. Validation Suite (end of orchestrator; asserts, not prints)

| # | Check | Expected |
|---|-------|----------|
| 1 | Row counts per file | Within ±2% of targets in §4 |
| 2 | Schema conformance | Exact column names **and order** per contract (including `customer_email` on the five product sources) |
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
| 13 | Amendment-1 email rules | Missing-email rows resolve via global ref (never auto-rejected); conflicting emails ≈3% present and flagged for quarantine, never merged |

Final output: a displayed **defect & reconciliation report** — one row per source: rows, dupes, missing %, format mix, invalid emails, orphans, conflicts.

## 7. Out of Scope (explicit)

No Bronze/Silver/Gold/Quarantine tables or schemas · no writes to `fraud_detection` (only read-only reads of `silver.customers` for seeding/integrity checks) · no batch-2 drift (Phase 7) · no dashboards.

## Acceptance Criteria

1. Six CSVs exist under `raw_data/batch_001/` with the §4 row counts and contract-exact schemas (including `customer_email` on the five product sources).
2. All 13 validation checks pass on the default config; failures raise, not warn.
3. Rerunning the orchestrator changes nothing (byte-identical files, verified by hash).
4. Global refs referentially resolve against the fraud platform; new persons carry no global ref (reserved for Phase 6 R5).
5. The defect & reconciliation report is displayed and matches the injected design rates.
6. The shared population is 12,000 persons: all 10,000 existing fraud customers seeded read-only with preserved canonical IDs (100% coverage of the verified domain) and 2,000 new persons reserved for R5 assignment from `CUST-100001`.
