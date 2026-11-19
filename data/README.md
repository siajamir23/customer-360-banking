# Generated Data Outputs (Phase 3)

## Location

Unity Catalog volume `customer_360.default.raw_data`, batch folder `batch_001/`:

| File | Raw rows (masters) |
|------|--------------------|
| customers_raw.csv | 12,420 (12,000) |
| accounts_raw.csv | 14,490 (14,000) |
| cards_raw.csv | 6,831 (6,600) |
| loans_raw.csv | 2,898 (2,800) |
| digital_logins_raw.csv | 56,925 (55,000) |
| support_cases_raw.csv | 9,315 (9,000) |

UTF-8, header row, comma-separated, `\n` line endings. `_manifest.json` in the
same folder holds run metadata (seed, counts, SHA-256 per file, checks passed)
and is not one of the six source datasets.

## Row-count formula

`raw = masters x (1 + 2% exact duplicates + 1.5% near duplicates) = masters x 1.035`.
The only records appended beyond masters are the two contracted duplicate
injection types (validation check 4). No other record types exist.

## Population

12,000 persons: **all 10,000 existing fraud customers** seeded read-only from
`fraud_detection.silver.customers` (canonical `CUST-######` IDs preserved, 100%
integration coverage) + 2,000 new persons with no global ref (R5 assigns
`CUST-100001+` in Phase 6 only; the generator never assigns enterprise IDs).

## Key engineering (documented)

* `global_customer_ref` carried at the contract rate per product source; every
  populated ref exists in the fraud registry (validation check 8).
* `customer_email`: rows with a ref keep email at 80%; rows without a ref always
  keep a valid matching email -> the only unresolvable (orphan) rows are the
  engineered 2% (a missing email is never the cause of unresolvability).
* Invalid emails (~5% of populated) are applied only to rows that also carry a
  global ref, so they remain resolvable via R1 (missing/invalid email is never
  an automatic rejection).
* Identity conflicts (~3% of dual-key rows): email of person B with global ref
  of person A -> quarantined in Phase 6, never merged.
* Orphans (2%): fabricated native refs (e.g. `CB-99001`), no global ref, email
  matching no person.
* Date columns: 60% ISO / 25% DD/MM/YYYY / 15% MM-DD-YYYY (datetimes have
  matching time variants).

## Reproducibility

`base_seed = 360` with per-source seed offsets; fixed event window
2025-12-01 .. 2026-11-30 (no `now()` anywhere). Identical CONFIG produces
byte-identical CSVs, verified by SHA-256 (validation checks 11 and 12).

## Regenerating

Run `notebooks/03_generate_source_data.py` top-to-bottom. It creates only the
approved objects (catalog `customer_360`, schema `default`, volume `raw_data`)
idempotently, reads `fraud_detection` read-only, and never creates Bronze /
Silver / Gold / Quarantine tables.
