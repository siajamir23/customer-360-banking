# Phase 1 — Environment Inspection Report

**Date:** 2026-11-19  
**Scope:** Read-only inspection of the Databricks workspace and the existing fraud detection lakehouse. No tables created, no data modified, no jobs altered.

## 1. Workspace Catalogs

| Catalog | Type | Notes |
|---------|------|-------|
| `fraud_detection` | Regular (owned by workspace user) | Existing project — do not modify |
| `learn_get_started_de_sia_jamir23` | Regular | Learning materials |
| `samples` | System-provided | Databricks samples |
| `system` | System | System tables |
| `workspace` | System | Browser-friendly tables |

No catalog named `customer_360` (or similar) exists — **no naming conflicts**.

## 2. Fraud Detection Lakehouse Inventory

Catalog `fraud_detection` contains six schemas (`bronze`, `silver`, `gold`, `quarantine`, `default`, `information_schema`) and 13 managed Delta tables:

| Schema | Table | Rows (baseline) |
|--------|-------|-----------------|
| bronze | customers | 10,000 |
| bronze | merchants | 1,000 |
| bronze | transactions | 100,000 |
| silver | customers | 10,000 |
| silver | merchants | 1,000 |
| silver | transactions | 100,000 |
| gold | customer_risk_metrics | 10,000 |
| gold | daily_transaction_metrics | — |
| gold | fraud_summary_by_category | — |
| gold | fraud_summary_by_country | — |
| quarantine | invalid_customers | 0 |
| quarantine | invalid_merchants | 0 |
| quarantine | invalid_transactions | 0 |

Source CSVs land in the UC volume `fraud_detection.default.raw_data`.

## 3. Existing Orchestration

One job, `fraud_detection_pipeline`, runs a linear five-task chain (no schedule, manual runs only):

1. `generate_synthetic_data` → `01_generate_synthetic_data`
2. `bronze_ingestion` → `02_bronze_ingestion`
3. `silver_transformation` → `03_silver_transformation`
4. `gold_aggregation` → `04_gold_aggregation`
5. `data_quality_tests` → `05_data_quality_testing`

## 4. Integration-Critical Schemas

### `fraud_detection.silver.customers` (the future integration target)

| Column | Type | Notes |
|--------|------|-------|
| `customer_id` | STRING | `CUST-######` format |
| `age` | INT | |
| `country` | STRING | PH / US / SG / AU / GB |
| `registration_date` | DATE | |
| `_source_file` | STRING | Lineage |
| `_ingestion_timestamp` | TIMESTAMP | Lineage |
| `_ingestion_date` | DATE | Lineage |

Notably **narrow** — no name, contact, or segment attributes. The Customer 360 platform is the source of that enrichment.

### `fraud_detection.silver.transactions`

Keys: `customer_id` (`CUST-######`), `transaction_id` (`TXN-########`), `merchant_id` (`MER-#####`). Carries `amount DECIMAL(18,2)`, `transaction_timestamp`, `channel`, `payment_method`, `device_id`, `country`, `status`, `fraud_label INT`.

### `fraud_detection.gold.customer_risk_metrics`

Customer-grain aggregate: `customer_id`, `age`, `country`, `registration_date`, plus behavioral metrics (`total_transactions`, `total_transaction_amount`, `average_transaction_amount`, `fraudulent_transactions`, `high_value_transactions`, `unique_merchants`, `first_transaction`, `last_transaction`, `fraud_rate`, `risk_category` HIGH/MEDIUM/LOW, `risk_assessment_timestamp`).

## 5. Feasibility of a Separate `customer_360` Catalog

The workspace user created `fraud_detection` directly (via `00_environment_setup`), proving `CREATE CATALOG` privilege on the workspace metastore (AWS us-east-2). A new `customer_360` catalog with `bronze` / `silver` / `gold` / `quarantine` schemas is fully supported.

## 6. Conventions Adopted from the Existing Project

* ID formats: customers `CUST-######` (shared domain), merchants `MER-#####`, transactions `TXN-########`
* Country domain: PH / US / SG / AU / GB
* Delta writes via PySpark with lineage columns (`_source_file`, `_ingestion_timestamp`, `_ingestion_date`)
* Quarantine pattern: `invalid_<entity>` tables with rejection metadata
* Numbered notebooks per pipeline stage; job JSON under `jobs/`

## 7. Key Decisions for This Project

1. **Synthetic sources seeded from the same customer population** (`CUST-######` domain) so the Phase 11 integration joins with referential integrity.
2. **Identity resolution as its own pipeline stage** — the hardest transformation in Customer 360 is isolated and testable.
3. **SCD Type 2 on `dim_customer`** — enables point-in-time analysis for fraud and risk use cases.
4. **Bronze extended with `source_system` + `batch_id`** — multi-source ingestion needs per-batch lineage and idempotent reloads.
