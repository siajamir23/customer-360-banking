# 02 — Medallion Data Model & Gold Data Products

**Phase:** 2 (Specifications & Data Modeling) · **Status:** In review  
**Depends on:** `02-source-data-contracts.md`, `02-customer-identity-resolution.md`, `02-scd-type-2.md`

## Objective

Define every dataset in the `customer_360` catalog — Bronze, Silver, Gold, Quarantine — with schema, grain, keys, and relationships, and fully specify the eight Gold data products (purpose, grain, PK/FK, columns, transformation logic, relationships).

## Naming Conventions

* Tables: `snake_case`, plural entities (`customers`, `accounts`); quarantine tables prefixed `invalid_`
* Lineage metadata: leading underscore (`_source_system`, `_batch_id`, `_ingestion_timestamp`, `_ingestion_date`, `_source_file`)
* Surrogate keys: `*_sk` (BIGINT); business keys: natural IDs (`customer_id`, `account_id`)
* Dates: `_date` suffix; timestamps: `_timestamp` suffix; booleans: `is_` prefix

---

## 1. Bronze — Raw Landing (`customer_360.bronze`)

**Principle:** raw, immutable, as-ingested. All business columns STRING (raw-is-raw); typing is a Silver concern. Batch-append with per-batch lineage. **No business logic, no dedup, no drops.**

| Table | Source System | Grain | Volume (target) |
|-------|--------------|-------|----------------|
| `bronze.customers` | CIS | customer master record (dupes included) | ~12,420 raw (12,000 masters) |
| `bronze.accounts` | Core Banking | deposit account | ~14,280 raw (14,000 masters) |
| `bronze.cards` | Credit Card | credit card | ~6,730 raw (6,600 masters) |
| `bronze.loans` | Loan Management | loan | ~2,855 raw (2,800 masters) |
| `bronze.digital_logins` | Digital Banking | login event | ~56,100 raw (55,000 masters) |
| `bronze.support_cases` | Customer Support | support ticket | ~9,180 raw (9,000 masters) |

Common appended columns on every Bronze table: `_source_system STRING`, `_batch_id STRING`, `_ingestion_timestamp TIMESTAMP`, `_ingestion_date DATE`, `_source_file STRING`.

Ingestion properties (Phase 4): repeatable/idempotent per batch (`_batch_id` partition of work), schema tracking via Delta schema evolution when a source adds a column, and Auto Loader–style `_metadata.file_path` for `_source_file`.

---

## 2. Quarantine — Rejected Records (`customer_360.quarantine`)

**Principle:** invalid records are preserved, never dropped. One `invalid_<entity>` table per Bronze entity, plus a generic `rejected_resolutions` for identity-resolution review cases (Phase 6).

| Table | Contains |
|-------|----------|
| `quarantine.invalid_customers`, `invalid_accounts`, `invalid_cards`, `invalid_loans`, `invalid_digital_logins`, `invalid_support_cases` | Rows failing Silver validation, with `_rejection_reason` (rule ID + message), `_source_system`, `_batch_id`, `_ingestion_timestamp`, and all original columns |
| `quarantine.rejected_resolutions` | Identity-resolution conflicts needing human review (see identity spec) |

Rejection is per-rule: one row may carry multiple reasons; the row is stored once per distinct failure reason for auditability (documented trade-off vs. single-row storage).

---

## 3. Silver — Standardized & Integrated (`customer_360.silver`)

**Principle:** typed, deduplicated, validated, identity-resolved. One table per source entity (same names as Bronze) **plus** the identity crosswalk.

| Table | Grain | Key Changes vs. Bronze |
|-------|-------|------------------------|
| `silver.customers` | one row per deduplicated CIS customer | typed columns; dates parsed to DATE/TIMESTAMP; email/phone/name normalized; dedup by `cis_customer_id` keeping most recent |
| `silver.accounts` | one row per deposit account | typed; dates parsed; `unified_customer_id` added by identity resolution |
| `silver.cards` | one row per card | typed; dates parsed; `unified_customer_id` |
| `silver.loans` | one row per loan | typed; dates parsed; `unified_customer_id` |
| `silver.digital_logins` | one row per login event | typed; timestamp parsed; `unified_customer_id` |
| `silver.support_cases` | one row per ticket | typed; dates parsed; `unified_customer_id` |
| `silver.customer_xref` | one row per (source_system, source_customer_ref) | **identity-resolution output** — the crosswalk between every source reference and the unified `CUST-######` (schema in identity spec) |

All Silver tables keep `_source_system`, `_batch_id`, `_ingestion_timestamp`, `_ingestion_date`, `_source_file`.

---

## 4. Gold — Analytics Data Products (`customer_360.gold`)

### Entity-Relationship Overview

```
                    dim_date (conformed calendar)
                       │
        dim_customer (SCD2) ── fact_customer_accounts ── dim_account (Type 1)
        customer_id, versions        │ customer_id+account_id      account_id (ACC-/CRD-/LON-)
                                 (bridge: holdings)
                       │
        customer_360_profile (1 row / current customer — integration-facing)
                       │
     ┌─────────────────┼──────────────────┐
customer_product_summary  customer_activity_summary  customer_financial_summary
(1 row / customer)        (1 row / customer/month)   (1 row / customer)
```

---

### 4.1 `gold.dim_customer` — Customer Dimension (SCD Type 2)

* **Business purpose:** conformed, historical customer dimension for all facts; supports point-in-time analysis ("what did we know about this customer at transaction time").
* **Grain:** one row per customer **version**.
* **Primary key:** `customer_sk` (surrogate). Business key: `customer_id` (`CUST-######`).
* **Foreign keys:** none (dimension). Referenced by all facts via `customer_id`/`customer_sk`.
* **Columns:**

| Column | Type | Description |
|--------|------|-------------|
| `customer_sk` | BIGINT | Surrogate key, monotonically increasing across versions |
| `customer_id` | STRING | Business key — unified `CUST-######` from identity resolution |
| `first_name`, `middle_name`, `last_name` | STRING | Normalized (trimmed, title case) |
| `date_of_birth` | DATE | Parsed |
| `gender` | STRING | M/F/U |
| `email`, `phone` | STRING | Normalized per identity spec |
| `address_line1`, `city`, `country`, `postal_code` | STRING | Standardized address |
| `kyc_status` | STRING | VERIFIED/PENDING/REJECTED |
| `customer_segment` | STRING | RETAIL/PREMIUM/PRIVATE/MASS_AFFLUENT |
| `annual_income` | DECIMAL(12,2) | Declared income |
| `customer_status` | STRING | ACTIVE/INACTIVE/CLOSED |
| `first_registration_date` | DATE | Earliest registration across sources |
| `effective_start_date` | DATE | SCD2 validity start |
| `effective_end_date` | DATE | SCD2 validity end (`9999-12-31` for current) |
| `is_current` | BOOLEAN | Exactly one TRUE row per customer |
| `record_hash` | STRING | SHA-2 of tracked attributes — change detection |
| `_batch_id`, `_ingestion_timestamp` | — | Lineage |

* **Transformation logic:** built from `silver.customers` joined to `silver.customer_xref` (CIS as authoritative source — see identity spec); SCD2 mechanics in `02-scd-type-2.md`.
* **Relationships:** `customer_id` joins to `customer_360_profile`, all summaries, `fact_customer_accounts`, and (Phase 11) `fraud_detection.silver.customers`.

### 4.2 `gold.dim_account` — Product/Account Dimension (Type 1)

* **Business purpose:** unified view of every product the bank holds for customers — deposit accounts, cards, and loans in one conformed dimension with a common attribute model.
* **Grain:** one row per product/account (current state).
* **Primary key:** `account_sk`. Business key: `account_id` (namespace: `ACC-#####` deposits, `CRD-#####` cards, `LON-#####` loans — no collisions by construction).
* **Type 1 (current state)** rationale: balances/statuses are fast-changing measures — snapshotting them in SCD2 would create version churn; history lives in the fact and summaries, and balance-level history is out of scope for this iteration (documented trade-off).
* **Columns:**

| Column | Type | Description |
|--------|------|-------------|
| `account_sk` | BIGINT | Surrogate key |
| `account_id` | STRING | Unified business key (ACC-/CRD-/LON- prefixed) |
| `product_type` | STRING | `DEPOSIT`, `CARD`, `LOAN` |
| `product_subtype` | STRING | account_type / card_type / loan_type |
| `account_number_display` | STRING | Masked account or PAN |
| `currency` | STRING | ISO currency (cards/loans inherit PHP-equivalent domain where source lacks currency) |
| `product_status` | STRING | Unified status model per product type |
| `opened_date` | DATE | open/issue/start date (unified name) |
| `closed_date` | DATE | close/expiry/end date, NULL if open |
| `credit_limit` | DECIMAL(12,2) | Cards only |
| `principal_amount` | DECIMAL(14,2) | Loans only |
| `interest_rate` | DECIMAL(5,2) | Loans only |
| `term_months` | INT | Loans only |
| `_batch_id`, `_ingestion_timestamp` | — | Lineage |

* **Transformation logic:** UNION of typed `silver.accounts` (→DEPOSIT), `silver.cards` (→CARD), `silver.loans` (→LOAN) projected onto the common column model; per-type columns NULL outside their type.
* **Relationships:** `account_id` referenced by `fact_customer_accounts`.

### 4.3 `gold.dim_date` — Conformed Calendar

* **Business purpose:** conformed date dimension for period-based analysis; avoids re-deriving date parts in every query.
* **Grain:** one row per calendar date. **Primary key:** `date_key` (INT `YYYYMMDD`).
* **Range:** 2015-01-01 → current date + 2 years. Generated, not sourced.
* **Columns:** `date_key`, `full_date`, `day_of_month`, `day_of_week`, `day_name`, `is_weekend`, `week_of_year`, `month`, `month_name`, `quarter`, `year`, `year_month` (`YYYY-MM`), `is_month_start`, `is_month_end`.
* **Relationships:** joined via `full_date`/`year_month` by activity and fact tables.

### 4.4 `gold.fact_customer_accounts` — Product Holdings Bridge

* **Business purpose:** records which customer holds which product — the fact table linking customers to accounts; supports joint-holder modeling and product-tenure analysis.
* **Grain:** one row per **customer–account relationship**.
* **Primary key:** (`customer_id`, `account_id`). Foreign keys: `customer_id` → `dim_customer`, `account_id` → `dim_account`.
* **Columns:**

| Column | Type | Description |
|--------|------|-------------|
| `customer_id` | STRING | FK → `dim_customer` |
| `account_id` | STRING | FK → `dim_account` |
| `holder_role` | STRING | `PRIMARY` (single-holder model in v1; `JOINT` reserved) |
| `relationship_start_date` | DATE | Product opened/issued/disbursed for this customer |
| `relationship_end_date` | DATE | NULL while active |
| `is_active_relationship` | BOOLEAN | Derived from product status and end date |
| `_batch_id`, `_ingestion_timestamp` | — | Lineage |

* **Transformation logic:** projection of each typed Silver product table through `silver.customer_xref` to `unified_customer_id`. Pure bridge — balances live in `dim_account` and summaries (no duplicated measures; documented decision).
* **Relationships:** PK pair is the FK target for summary derivations; `customer_id`/`account_id` must both resolve (referential integrity check in Phase 9).

### 4.5 `gold.customer_360_profile` — Unified Customer Profile (integration-facing)

* **Business purpose:** the flagship current-state profile — one trusted row per customer; **the data product consumed by the fraud detection platform** (Phase 11).
* **Grain:** one row per **current** customer. **Primary key:** `customer_id`.
* **Transformation logic:** `dim_customer` (current row only) LEFT JOIN aggregates of `fact_customer_accounts`+`dim_account`, `silver.digital_logins`, `silver.support_cases`.
* **Columns:**

| Column | Type | Description |
|--------|------|-------------|
| `customer_id` | STRING | PK — unified `CUST-######` |
| `first_name`, `last_name`, `date_of_birth`, `age`, `gender` | — | Demographics (age derived at build time) |
| `email`, `phone`, `city`, `country` | STRING | Normalized contact |
| `kyc_status`, `customer_segment`, `customer_status` | STRING | Status attributes |
| `annual_income` | DECIMAL(12,2) | Declared income |
| `first_registration_date` | DATE | Earliest known onboarding |
| `source_systems_count` | INT | # of source systems where this customer resolved |
| `deposit_accounts` / `active_deposit_accounts` | INT | Holding counts |
| `cards` / `active_cards` | INT | Holding counts |
| `loans` / `active_loans` | INT | Holding counts |
| `total_deposit_balance` | DECIMAL(15,2) | Sum of deposit balances |
| `total_credit_limit` / `total_card_outstanding` | DECIMAL(12,2) | Card aggregates |
| `card_utilization_pct` | DECIMAL(5,2) | Card outstanding / credit limit (NULL when no limit) |
| `total_loan_outstanding` | DECIMAL(14,2) | Loan aggregate |
| `logins_last_90d` / `failed_login_rate_90d` | — | Digital engagement |
| `open_support_tickets` / `fraud_alert_tickets_12m` | INT | Service history (FRAUD_ALERT category) |
| `avg_csat` | DECIMAL(3,2) | Satisfaction where available |
| `engagement_segment` | STRING | Derived: `DIGITAL_ACTIVE` / `MULTI_CHANNEL` / `LOW_ENGAGEMENT` |
| `profile_generated_at` | TIMESTAMP | Build timestamp — freshness contract |
| `_batch_id` | STRING | Lineage |

* **Relationships:** 1:1 with current `dim_customer`; 1:N implicit to `fact_customer_accounts`, login and ticket facts.

### 4.6 `gold.customer_product_summary` — Product Holdings Summary

* **Business purpose:** analyst-friendly per-customer product metrics without joining three product tables.
* **Grain:** one row per customer. **PK:** `customer_id` (FK → `dim_customer`).
* **Columns:** `customer_id`, `deposit_accounts`, `active_deposit_accounts`, `total_deposit_balance`, `avg_deposit_balance`, `cards`, `active_cards`, `credit_limit_total`, `card_outstanding_total`, `card_utilization_pct`, `loans`, `active_loans`, `loan_principal_total`, `loan_outstanding_total`, `monthly_installment_total`, `total_products`, `active_products`, `first_product_opened_date`, `last_product_opened_date`, `_batch_id`.
* **Transformation logic:** `fact_customer_accounts` JOIN `dim_account` aggregated per `customer_id`; utilization = `card_outstanding_total / credit_limit_total` (NULL when no limit).

### 4.7 `gold.customer_activity_summary` — Engagement Summary

* **Business purpose:** monthly engagement metrics per customer — trend-ready input for behavior and risk analytics (e.g., failed-login spikes).
* **Grain:** one row per **customer per activity month**. **PK:** (`customer_id`, `activity_month`). FK: `customer_id` → `dim_customer`.
* **Columns:** `customer_id`, `activity_month` (`YYYY-MM`), `login_events`, `successful_logins`, `failed_logins`, `failed_login_rate`, `distinct_devices`, `primary_channel` (mode of channel), `avg_session_minutes`, `active_days`, `support_tickets_created`, `support_tickets_resolved`, `avg_csat`, `_batch_id`.
* **Transformation logic:** `silver.digital_logins` and `silver.support_cases` aggregated by `unified_customer_id` + month(window of event timestamp).

### 4.8 `gold.customer_financial_summary` — Financial KPI Summary

* **Business purpose:** per-customer financial health metrics, including credit-risk-relevant ratios consumed later by fraud/risk analytics.
* **Grain:** one row per customer. **PK:** `customer_id` (FK → `dim_customer`).
* **Columns:** `customer_id`, `total_deposit_balance`, `total_credit_limit`, `total_card_outstanding`, `card_utilization_pct`, `total_loan_outstanding`, `monthly_installment_total`, `annual_income`, `estimated_monthly_income`, `debt_service_ratio` (installments / monthly income), `net_position_estimate` (deposits − card outstanding − loan outstanding), `has_overlimit_card`, `has_defaulted_loan`, `has_overdraft_account`, `_batch_id`.
* **Transformation logic:** `customer_product_summary` + income from `dim_customer`; ratios guarded against NULL/zero denominators.

---

## Referential Integrity Rules (validated in Phase 9)

1. Every `fact_customer_accounts.customer_id` exists in `dim_customer` (current or historical).
2. Every `fact_customer_accounts.account_id` exists in `dim_account`.
3. Exactly one `is_current = TRUE` row per `customer_id` in `dim_customer`.
4. `dim_customer.customer_id` values with `CUST-######` format ≤ `CUST-010000` exist in `fraud_detection.silver.customers` (cross-platform consistency for Phase 11).
5. Profile ↔ summaries row counts reconcile: `customer_360_profile`, `customer_product_summary`, `customer_financial_summary` all have exactly one row per active customer.

## Acceptance Criteria

1. All Gold DDL (Phase 8) is generated from this spec — column names, types, and grain match 1:1.
2. Every table above has documented grain, PK, FKs, and lineage columns before implementation.
3. A rendered ERD is added to `docs/` at Phase 8 completion.
