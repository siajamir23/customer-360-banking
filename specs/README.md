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
| 2 | Medallion data model & Gold data products | `02-data-model.md` | In review |
| 2 | Customer identity resolution strategy | `02-customer-identity-resolution.md` | In review |
| 2 | SCD Type 2 strategy (dim_customer) | `02-scd-type-2.md` | In review |
| 2 | Fraud detection integration contract | `02-fraud-integration.md` | In review |
| 3 | Synthetic multi-source data generation | `03-data-generation.md` | Pending |
| 4 | Bronze ingestion pipelines | `04-bronze-ingestion.md` | Pending |
| 5 | Silver transformation & standardization | `05-silver-standardization.md` | Pending |
| 6 | Customer identity resolution | `06-identity-resolution.md` | Pending |
| 7 | SCD Type 2 implementation | `07-scd-type-2.md` | Pending |
| 8 | Gold customer data products | `08-gold-datamarts.md` | Pending |
| 9 | Data quality & reconciliation | `09-data-quality.md` | Pending |
| 10 | Pipeline orchestration | `10-orchestration.md` | Pending |
| 11 | Fraud detection integration | `11-fraud-integration.md` | Pending |
| 12 | Analytics & documentation | `12-analytics.md` | Pending |

Each spec file is added in this folder at the start of its phase, before the implementation notebook is written.
