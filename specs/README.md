# Component Specifications

This project follows **specification-driven development**: every major component is specified before it is implemented, and the specification is kept aligned with the code as the source of truth for what each component does and why.

## Process (per component)

1. **Specify** — write the spec document in this folder:
   * Objective
   * Functional requirements
   * Input & output schemas
   * Transformation rules
   * Validation requirements
   * Acceptance criteria
2. **Implement** — build the component in the corresponding notebook.
3. **Test** — execute the notebook and run validation queries.
4. **Validate** — confirm outputs meet the acceptance criteria.
5. **Document** — record results and deviations in the spec doc.

## Component Map

| Phase | Component | Spec | Status |
|-------|-----------|------|--------|
| 2 | Source data contracts (6 source systems) | `02-source-data-contracts.md` | In review |
| 2 | Medallion data model & Gold data products | `02-data-model.md` | **Approved — Amendment 3 (Phase 6) applied 2026-11-24** (`*_resolved` views; Silver product tables never carry `unified_customer_id`; `rejected_resolutions` schema; `r5_allocation_ledger` + `identity_resolution_control`) |
| 2 | Customer identity resolution strategy | `02-customer-identity-resolution.md` | **Approved 2026-11-24 — Phase 6 amendments applied** (quarantine conflict reading supersedes cascade; R4 → `IN_REVIEW`; CIS independent evaluation + `CIS_IDENTIFIER_CONFLICT`; orphan + drift paths; `candidate_evidence` schema §6.1; R5 allocation ledger + control §6.2) |
| 2 | SCD Type 2 strategy (dim_customer) | `02-scd-type-2.md` | In review |
| 2 | Fraud detection integration contract | `02-fraud-integration.md` | In review |
| 3 | Synthetic multi-source data generation | `03-data-generation.md` | **Approved — specs updated per review** (population 12,000; Amendment 1 with email rules; `src/` modular layout) |
| 4 | Bronze ingestion pipelines | `04-bronze-ingestion.md` | **Approved — implemented 2026-11-20** (33/33 acceptance checks; raw-is-raw, 5 lineage columns, fail-closed idempotency) |
| 5 | Silver transformation & standardization | `05-silver-standardization.md` | **Approved 2026-11-21 — proposal + Amendments 1–6 incorporated** (dedup survivorship, phone <10→NULL, distinct-record reconciliation, exactly-one-pattern parsing, native-ref inventory, isolated quarantine testing) |
| 6 | Customer identity resolution | `06-identity-resolution.md` | **Approved 2026-11-24 — RA1–RA15** (R1–R5 with CIS conflict detection, quarantine, orphans; `customer_xref`, `rejected_resolutions`, six `*_resolved` views; R5 allocation ledger/control — durable reservations, serialized allocation, resumable abandonment, enforced uniqueness; exact batch_001 baselines; commit `0642bde`) |
| 7 | SCD Type 2 implementation | `07-scd-type-2.md` | Pending |
| 8 | Gold customer data products | `08-gold-datamarts.md` | Pending |
| 9 | Data quality & reconciliation | `09-data-quality.md` | Pending |
| 10 | Pipeline orchestration | `10-orchestration.md` | Pending |
| 11 | Fraud detection integration | `11-fraud-integration.md` | Pending |
| 12 | Analytics & documentation | `12-analytics.md` | Pending |

Each spec file is added in this folder at the start of its phase, before the implementation notebook is written.
