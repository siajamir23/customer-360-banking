# Enterprise Banking Data Platform — Customer 360

> Customer 360 banking data platform built on Databricks — multi-source data integration, identity resolution, SCD Type 2, and Medallion Architecture feeding fraud detection and risk analytics.

## Overview

This project simulates a banking environment in which six operational source systems produce fragmented customer data, and consolidates it into a unified, governed, analytics-ready **Customer 360** data product on the Databricks Lakehouse. It is designed as a portfolio project demonstrating practical, production-minded data engineering: Medallion Architecture, data quality management, identity resolution, Slowly Changing Dimensions, incremental processing, and cross-platform data integration.

The platform acts as a trusted upstream data provider for the companion project [fraud-detection-lakehouse](https://github.com/siajamir23/fraud-detection-lakehouse): its gold-tier `customer_360_profile` is consumed by the fraud detection platform's silver layer to enrich risk analytics.

## Architecture

```
  Source Systems (synthetic data)          customer_360 catalog (Databricks Unity Catalog)
 ┌──────────────────────────────┐         ┌────────────────────────────────────────────┐
 │ Customer Information System  │──┐  ┌──►│ bronze    raw, as-ingested + source_system │
 │ Core Banking System          │──┼──┤   │           + batch_id + ingestion_timestamp │
 │ Credit Card System           │──┤  └──►│ silver    standardized + deduplicated      │
 │ Loan Management System       │──┼─────►│           + validated + identity resolved  │
 │ Digital Banking System       │──┤      │           + SCD Type 2 history             │
 │ Customer Support System      │──┘      │ gold      analytics data products          │
 └──────────────────────────────┘         │ quarantine  rejected records + reasons     │
                                          └────────────────────────────────────────────┘
                                                        │
                                                        ▼ (Phase 11 integration)
                                       fraud_detection.silver.customers (enriched)
                                                        │
                                                        ▼
                                       fraud_detection.gold.customer_risk_metrics
```

## Source Systems

| # | Source System | Datasets | Domain |
|---|---------------|----------|--------|
| 1 | Customer Information System (CIS) | Customer master, personal & contact info, registration details | Who the customer is |
| 2 | Core Banking System | Bank accounts, types, balances, statuses | Deposit relationships |
| 3 | Credit Card System | Card accounts, credit limits, outstanding balances, statuses | Card relationships |
| 4 | Loan Management System | Loan accounts, amounts, outstanding balances, repayment status | Lending relationships |
| 5 | Digital Banking System | Online activity, login history, device info | Engagement & channel behavior |
| 6 | Customer Support System | Complaints, support tickets, case statuses | Service experience |

Synthetic data deliberately contains realistic defects: duplicate records, missing values, inconsistent date formats, and inconsistent customer identifiers across systems — the raw material for the identity-resolution and data-quality layers.

## Repository Structure

```
customer-360-banking/
├── README.md
├── LICENSE
├── notebooks/                 # Numbered pipeline notebooks (added per phase)
├── sql/                       # Documented analytical queries (Phase 12)
├── specs/                     # Component specifications (spec-driven development)
├── jobs/                      # Lakeflow Jobs definitions (Phase 10)
└── docs/                      # Architecture docs, environment reports
```

## Development Phases

| Phase | Deliverable | Status |
|-------|-------------|--------|
| 1 | Environment inspection & project architecture | ✅ Done |
| 2 | Project specifications & data modeling | ✅ Specs complete — in review |
| 3 | Synthetic multi-source data generation | |
| 4 | Bronze ingestion pipelines | |
| 5 | Silver transformation & standardization | |
| 6 | Customer identity resolution | |
| 7 | SCD Type 2 implementation | |
| 8 | Gold customer data products | |
| 9 | Data quality & reconciliation | |
| 10 | Pipeline orchestration (Lakeflow Jobs) | |
| 11 | Integration with fraud detection | |
| 12 | Analytics & documentation | |

## Key Design Decisions

* **Separate catalog (`customer_360`), separate repo** — zero blast radius on the existing fraud detection platform; both projects remain independently reviewable portfolio pieces.
* **Shared customer ID domain (`CUST-######`)** — synthetic sources are seeded from the same customer population as the fraud detection project so the Phase 11 integration joins with referential integrity.
* **`source_system` + `batch_id` in bronze** — enables repeatable, idempotent ingestion and per-batch lineage (an extension of the single-source pattern used in the fraud project).
* **Identity resolution as an isolated stage** — cross-system matching is the hardest transformation in Customer 360; isolating it makes it independently testable and debuggable.
* **SCD Type 2 on `dim_customer`** — captures what was known about a customer *at transaction time*, a prerequisite for point-in-time fraud and risk analysis.

## Tech Stack

Python · PySpark · Spark SQL · Delta Lake · Databricks · Unity Catalog · Lakeflow Jobs

## License

[MIT](LICENSE)
