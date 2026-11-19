# 02 — Fraud Detection Integration

**Phase:** 2 (Specifications & Data Modeling) · **Status:** In review  
**Depends on:** `phase-1-environment-inspection.md`, `02-data-model.md`, verified live queries of the fraud platform

## Objective

Define how `customer_360.gold.customer_360_profile` becomes consumable by the existing fraud detection lakehouse — **without modifying the existing platform before explicit approval** — so that enriched, customer-aware risk analytics become possible (fraud rates by segment, behavior vs. account history, high-risk customer identification).

## 1. Verified Facts About the Fraud Platform (Phase 1 + Phase 2 read-only queries)

| Fact | Verified Value | Source |
|------|---------------|--------|
| `fraud_detection.silver.customers` rows / distinct IDs | 10,000 / 10,000 | live query 2026-11-19 |
| `customer_id` format | `^CUST-[0-9]{6}$` — **10,000 of 10,000 conform** (CUST-000001 … CUST-010000) | live query |
| Transaction→customer referential coverage | 100% (0 orphan transaction customers) | live query |
| `silver.customers` schema | `customer_id STRING`, `age INT`, `country STRING`, `registration_date DATE`, lineage cols — **no name/contact/segment attributes** | `readTable` |
| `gold.customer_risk_metrics` | customer-grain risk metrics keyed on `customer_id`; risk_category HIGH/MEDIUM/LOW | `readTable` |
| `silver.customers` write mode | **full overwrite per pipeline run** (job task `03_silver_transformation` rebuilds from bronze with `mode("overwrite")` + `overwriteSchema`) | project docs + notebook |
| Orchestration | `fraud_detection_pipeline` job, linear 5 tasks, manual | Jobs API |

**Conclusion:** the identifier domains are **exactly compatible** — the enterprise `CUST-######` domain verified in this phase is the same one the fraud platform keys on. No key translation needed; `customer_360_profile.customer_id` joins directly.

## 2. Critical Architectural Constraint (drives the whole strategy)

Because `fraud_detection.silver.customers` is **rebuilt from bronze on every fraud-pipeline run**, any enrichment written *into* that table by an external job is **wiped on the next run**. A bolt-on "push enrichment into their silver" job silently loses data. The integration must therefore be either part of the fraud pipeline itself or additive alongside it.

## 3. Integration Options

| | Option A — Enriched table (additive) | Option B — Pipeline task (modifies fraud job) |
|---|---|---|
| Approach | New table `fraud_detection.silver.customers_enriched` = `silver.customers` LEFT JOIN `customer_360.gold.customer_360_profile` on `customer_id`, rebuilt after each fraud silver task | New task `03b_enrich_customers` appended to `fraud_detection_pipeline` after `silver_transformation`, writing enrichment columns into silver (schema evolution) or the enriched table |
| Existing tables modified | **None** — new table only | Fraud job definition (existing notebooks untouched, but job edited) |
| Rebuild-safe | Yes if scheduled after fraud silver task | Yes (in-pipeline) |
| Blast radius | Lowest | Medium |
| Downstream change needed | `gold.customer_risk_metrics` generation reads enriched table (Phase 11 decision) | Same |
| **Recommendation** | **Start here (Phase 11 default)** | Production consolidation later, with explicit approval |

Chosen default: **Option A** — it honors the project rule "do not modify the existing fraud_detection catalog unless explicitly instructed" (new table is additive), and it survives fraud pipeline rebuilds. Option B remains documented for the production-grade consolidation once both platforms are stable. **Final decision deferred to Phase 11 with user approval.**

## 4. Enrichment Contract (`customer_360_profile` → fraud platform)

**Join key:** `customer_id` (verified compatible). **Join type: LEFT** (fraud customers keep their row even without a 360 profile). **Expected coverage: ~100%** of fraud customers match a 360 profile — the approved shared population seeds **all 10,000** existing fraud customers (approved adjustment 2026-11-19); residual non-coverage arises only from quarantined identity conflicts (~1–3%) and is reported as a coverage metric, never hidden. The 2,000 additional new customers (`CUST-100001+`, assigned by R5 in Phase 6) exist only in the 360 domain and never appear in fraud transactions, by design.

| `customer_360_profile` column | Enrichment use in fraud analytics |
|------------------------------|----------------------------------|
| `customer_segment` | Fraud rates by customer segment (target use case) |
| `kyc_status` | Risk weighting; unverified-identity risk analysis |
| `country`, `city` | Geo risk comparison vs. transaction country (mismatch detection input) |
| `customer_status` | Dormant-account abuse detection |
| `first_registration_date` | Customer tenure vs. fraud incidence |
| `deposit_accounts`, `cards`, `loans`, `active_*` | Product-holding vs. fraud patterns |
| `total_deposit_balance`, `total_card_outstanding`, `total_loan_outstanding`, `card_utilization_pct` | Financial context for high-value fraud |
| `logins_last_90d`, `failed_login_rate_90d` | Digital behavior vs. fraudulent transaction correlation |
| `fraud_alert_tickets_12m`, `open_support_tickets` | Prior-fraud-signal enrichment |
| `engagement_segment` | Behavior cohorting |

## 5. Required Transformations (Option A)

1. Build `customers_enriched` = fraud `silver.customers` LEFT JOIN 360 profile — fraud columns kept **unchanged and first** (backward compatible for any existing consumer), enrichment columns suffixed where names could collide (none collide today — verified column list).
2. All enrichment columns NULL-safe for unmatched customers (~20%); `has_360_profile BOOLEAN` flag added for explicit coverage analysis.
3. Rebuild trigger runs **after** the fraud silver task — Phase 10 orchestration wires the dependency (separate job with a `customers_enriched` task that can be triggered after `fraud_detection_pipeline`, or added to the fraud job as Option B).
4. Coverage + reconciliation checks (Phase 9/11): row count of enriched == row count of fraud silver.customers (exactly one row per customer, LEFT JOIN cannot fan out — profile PK is `customer_id`).

## 6. Schema Conflict Analysis

| Check | Result |
|-------|--------|
| Join key type | STRING = STRING — compatible, no cast |
| Column name collisions | None: fraud silver has no name/contact/segment columns (verified) — all enrichment columns are additive |
| Grain compatibility | Both 1 row per customer (profile PK = customer_id) — no fan-out on join |
| Date semantics | Fraud `registration_date` (DATE) vs. 360 `first_registration_date` (DATE): different semantics (source-system registration vs. earliest across systems) — both kept, not merged |
| Data type domain | `country` values both ∈ {PH, US, SG, AU, GB} — verified consistent domains |

## 7. Guardrails (until Phase 11 explicit approval)

1. No writes of any kind to `fraud_detection` catalog during Phases 3–10. All integration work is read-only + spec.
2. `customer_360` Gold builds never read from fraud bronze/silver as **inputs** to its own transformations (one-way dependency: 360 → fraud only, no circularity).
3. The only cross-catalog reads permitted: identity verification queries (read-only, this phase) and the Phase 11 enrichment join.

## Acceptance Criteria

1. Enrichment join produces exactly 10,000 rows (one per fraud customer) with ≥97% non-null `has_360_profile` (100% seeded coverage minus quarantined conflicts) — validated in Phase 11 before any write.
2. Zero columns of the existing `fraud_detection.silver.customers` are altered by the integration (additive-only proof: column diff before/after).
3. Coverage, join-fan-out, and null-rate metrics are reported and persisted for audit.
4. The fraud pipeline continues to run unchanged and passes its existing data quality task after integration.
