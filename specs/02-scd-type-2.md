# 02 — Slowly Changing Dimension Type 2 (dim_customer)

**Phase:** 2 (Specifications & Data Modeling) · **Status:** In review  
**Depends on:** `02-data-model.md` (§4.1), `02-customer-identity-resolution.md`

## Objective

Maintain the full history of tracked customer attributes in `gold.dim_customer` so any consumer can answer **"what did we know about this customer at time T?"** — a prerequisite for point-in-time fraud and risk analysis (e.g., attributing a 2025 transaction to the customer's segment *as of 2025*, not today's).

## 1. SCD2 Columns

| Column | Type | Rule |
|--------|------|------|
| `customer_sk` | BIGINT | Surrogate key — **one per version row**, assigned at insert |
| `customer_id` | STRING | Business key (`CUST-######`) — repeats across versions |
| `effective_start_date` | DATE | Date this version became true (batch processing date of first appearance of the attribute set) |
| `effective_end_date` | DATE | Date this version stopped being true; **`9999-12-31` sentinel for the current version** (closed-open interval `[start, end)`) |
| `is_current` | BOOLEAN | TRUE exactly for the version with `effective_end_date = 9999-12-31` |
| `record_hash` | STRING | Change-detection hash (see §2) |

**Uniqueness invariants:** exactly one `is_current` row per `customer_id`; version intervals per `customer_id` are non-overlapping and gap-free when read as `[start, end)`.

## 2. `record_hash` — Change Detection

`record_hash = sha2(concat_ws('||', coalesce(attr_1,''), …, coalesce(attr_n,'')), 256)` over the **tracked attributes only** (lineage and SCD columns excluded):

`first_name`, `middle_name`, `last_name`, `date_of_birth`, `gender`, `email`, `phone`, `address_line1`, `city`, `country`, `postal_code`, `kyc_status`, `customer_segment`, `annual_income`, `customer_status`

Rules:
* `NULL` vs empty-string must hash identically → `coalesce(attr,'')` (NULL-change is not a version change).
* Whitespace/case differences cannot create false versions — attributes are already normalized upstream (Silver), so the hash sees canonical values only.
* `first_registration_date` and `source_systems_count` are **not tracked** (monotonic facts, not states).

## 3. Maintenance Algorithm (per batch)

For each `customer_id` arriving in the batch, compare the new tracked-attribute set against the current version:

1. **No current version exists** → insert version 1: `effective_start_date = batch date`, `effective_end_date = 9999-12-31`, `is_current = TRUE`.
2. **Hash unchanged** → no write (idempotent; only `_ingestion_timestamp` untouched). Preserves batch idempotency: re-running a batch creates zero duplicate versions.
3. **Hash changed** →
   * Expire current version: `effective_end_date = batch date − 1 day`, `is_current = FALSE` (this is an UPDATE — implemented as Delta `MERGE` keyed on `(customer_id, is_current)`).
   * Insert new version: `effective_start_date = batch date`, end sentinel, `is_current = TRUE`, new `customer_sk`, new hash.
4. **Same-batch correction** (a customer changes twice within one batch) → collapse to one new version per batch (the latest state); intra-batch churn does not create versions — batch grain for versioning is a documented decision.

## 4. Point-in-Time Query Pattern (consumer contract)

To get the customer record **as known at event time**:

```sql
SELECT e.*, d.*
FROM   events e
JOIN   gold.dim_customer d
       ON d.customer_id = e.customer_id
      AND e.event_date >= d.effective_start_date
      AND e.event_date <  d.effective_end_date
```

`9999-12-31` as an open-ended sentinel makes the join correct for current versions without a NULL special case.

## 5. History Retention & Replay

* History is **append-mostly**: only the expiring row is updated (narrow MERGE), new versions append. No `VACUUM`-sensitive rewrites of historical rows.
* Initial load (Phase 7): all customers insert as version 1 with the batch date as start — synthetic sources are full snapshots, so version-1 history begins at platform go-live. Backdating fake history is explicitly out of scope (documented).
* Later phases (3+) that re-generate source data with attribute changes (e.g., a customer moving city) demonstrate true versioning; the generator will emit a second batch with ~5% attribute drift to exercise the mechanism.

## 6. Why SCD2 only for `dim_customer`?

* `dim_customer`: tracked attributes are slowly-changing states where point-in-time correctness matters for fraud/risk (contract, segment, KYC, address).
* `dim_account`: balances/statuses are fast-changing measures — SCD2 would explode version count; the account dimension stays Type 1, and history is carried by the summaries (documented trade-off, revisit if account-status history becomes a requirement).

## Acceptance Criteria

1. After any batch: exactly one `is_current = TRUE` row per `customer_id` in `dim_customer`.
2. Re-running the same batch changes nothing (hash-based idempotency proven by identical row counts + version counts).
3. An injected attribute change produces exactly one new version, with the previous version closed at `batch_date − 1`.
4. Point-in-time join (§4) returns the pre-change attributes for events before the change date and post-change attributes after — validated with a seeded test customer in Phase 7.
5. `record_hash` is deterministic: rebuilding from the same Silver inputs reproduces identical hashes.
