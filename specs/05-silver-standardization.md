# 05 — Silver Layer: Standardization, Deduplication, Validation & Quarantine

**Phase:** 5 (Silver transformation) · **Status:** Approved — proposal 2026-11-20 + mandatory Amendments 1–6 (2026-11-21) + Amendments 7–8 (2026-11-24) + Amendment 9 (2026-11-24, post-production reconciliation) incorporated

**Depends on:** `02-source-data-contracts.md`, `02-data-model.md`, `02-customer-identity-resolution.md`, `03-data-generation.md`, `04-bronze-ingestion.md`, live Bronze inspection 2026-11-20/21 (all batch_001 expectations below verified against actual data, not spec assumptions)

## Objective

Transform the six Bronze STRING tables into **typed, standardized, deduplicated, validated** Silver tables in `customer_360.silver`, quarantining every rejected record with a traceable reason, without modifying Bronze, without identity resolution (Phase 6), without Gold (Phase 7+), and without touching `fraud_detection`.

### Non-goals (hard scope boundaries)

* No identity resolution: rules R1–R5, `customer_xref`, and `unified_customer_id` are **Phase 6** (D4). Phase 5 performs only the identity spec's §1 *match-key normalization* (D5).
* No Gold tables; no SCD2.
* Bronze is immutable (acceptance check 8 proves it). Raw source values are preserved in Bronze and never mutated (Amendment 5).
* `fraud_detection` catalog: zero writes, zero reads (Phase 5 has no legitimate read need).

---

## 1. Approved Decisions (D1–D10, as amended)

| ID | Decision | Disposition |
|----|----------|-------------|
| D1 | Six Silver tables, same names as Bronze (`support_tickets` everywhere per data-model Amendment 2); no Gold | Approved |
| D2 | Dedup-eliminated rows go to a shared `quarantine.duplicate_records` (never silently dropped) | Approved, amended layout (§9.2) |
| D3 | Shared `silver.processing_audit` — one row per (table, batch) with full reconciliation counters | Approved |
| D4 | `unified_customer_id` deferred to Phase 6; Phase 5 silver tables do not carry the column | Approved |
| D5 | Identity spec §1 match-key **normalization** now (email, phone, `name_key`, `global_customer_ref` format validation) — no match rules | Approved |
| D6 | Invalid email / invalid `global_customer_ref` → NULL in Silver + audit count; never a rejection | Approved |
| D7 | `invalid_*` tables legitimately empty for batch_001; quarantine proven by isolated synthetic tests (Amendment 6) | Approved |
| D8 | `_silver_processed_timestamp` added to all Silver rows, in addition to the five Bronze lineage columns | Approved |
| D9 | Quarantine tables carry all **five** Bronze lineage columns (adds `_source_file`, `_ingestion_date` beyond data-model §2's four) | Approved |
| D10 | `try_to_date` / `try_cast` / `try_to_timestamp` everywhere, with exactly-one-pattern verification (Amendment 4) | Approved, amended (§5.2)

## 2. Native Source Reference Inventory (Amendment 5 — from approved contracts, verified live 2026-11-21)

Bronze preserves all original values untouched. Silver validates as follows. **Principle:** identity *match-key* contact fields (email, phone, `global_customer_ref`) that are malformed are NULLed and audited (D6); **every other malformed non-null value follows its contract's validation rule and rejects to quarantine** (live batch_001 counts are all zero, so no rows are affected today).

| Reference | Owner system | Format (contract) | Mandatory | Silver validation | On violation |
|-----------|--------------|-------------------|-----------|-------------------|-------------|
| `cis_customer_id` | CIS | `^CIS-[0-9]{5}$` | Yes (PK) | format check | quarantine `PK_INVALID_FORMAT` |
| `account_id` | Core Banking | `^ACC-[0-9]{5}$` | Yes (PK) | format check | quarantine `PK_INVALID_FORMAT` |
| `card_id` | Card System | `^CRD-[0-9]{5}$` | Yes (PK) | format check | quarantine `PK_INVALID_FORMAT` |
| `loan_id` | Loan System | `^LON-[0-9]{5}$` | Yes (PK) | format check | quarantine `PK_INVALID_FORMAT` |
| `login_id` | Digital Banking | `^LOG-[0-9]{7}$` (7 digits) | Yes (PK) | format check | quarantine `PK_INVALID_FORMAT` |
| `ticket_id` | Support System | `^TKT-[0-9]{5}$` | Yes (PK) | format check | quarantine `PK_INVALID_FORMAT` |
| `cb_customer_ref` | Core Banking | `^CB-[0-9]{5}$` | Yes | format check | quarantine `NATIVE_REF_INVALID_FORMAT` |
| `cc_customer_ref` | Card System | `^CC-[0-9]{5}$` | Yes | format check | quarantine `NATIVE_REF_INVALID_FORMAT` |
| `ln_customer_ref` | Loan System | `^LN-[0-9]{5}$` | Yes | format check | quarantine `NATIVE_REF_INVALID_FORMAT` |
| `db_customer_ref` | Digital Banking | `^DB-[0-9]{5}$` | Yes | format check | quarantine `NATIVE_REF_INVALID_FORMAT` |
| `cs_customer_ref` | Support System | `^CS-[0-9]{5}$` | Yes | format check | quarantine `NATIVE_REF_INVALID_FORMAT` |
| `global_customer_ref` | all six | `^CUST-[0-9]{6}$` | Nullable | uppercase/trim; format-else-NULL (identity §1, D6) | NULL + audit `invalid_refs_nulled` (live: 0 malformed) |
| `account_number` | Core Banking | `^[0-9]{10}$` | Yes | format check | quarantine `ACCOUNT_NUMBER_INVALID` |
| `branch_code` | Core Banking | `^BR-[0-9]{3}$` | Nullable | format check when present | quarantine `BRANCH_CODE_INVALID` (live: 0) |
| `card_number_masked` | Card System | `^\*{4}-\*{4}-\*{4}-[0-9]{4}$` | Yes | format check | quarantine `MASKED_PAN_INVALID` (live: 0) |

No identifier formats are invented; the enterprise `CUST-######` domain and R5 assignment remain Phase 6 concerns.

## 3. Silver Table Schemas (`customer_360.silver`)

All six tables additionally carry, **verbatim from Bronze**: `_source_system`, `_batch_id`, `_ingestion_timestamp`, `_ingestion_date`, `_source_file`; plus `_silver_processed_timestamp` (D8). Logical types follow the **source contracts** exactly (Amendment 5; no invented widths).

### 3.1 `silver.customers` — PK `cis_customer_id`

| Column | Type | Notes |
|--------|------|-------|
| `cis_customer_id` | STRING | upper/trim; `^CIS-[0-9]{5}$` |
| `global_customer_ref` | STRING | upper/trim; `^CUST-[0-9]{6}$` else NULL (D6) |
| `first_name`, `middle_name`, `last_name` | STRING | trimmed; letters/punct per contract |
| `name_key` | STRING | identity §1: `FIRST~LAST` upper-cased, punctuation removed, whitespace collapsed (match key for Phase 6) |
| `date_of_birth` | DATE | 3-format parse; ≤ registration_date |
| `gender` | STRING | M/F/U domain |
| `email` | STRING | `lower(trim())`; invalid → NULL (D6) |
| `phone` | STRING | Amendment 2 (§5.4): exactly 10 digits or NULL |
| `address_line1`, `city`, `country`, `postal_code` | STRING | trimmed; country domain; postal 4–6 chars when present |
| `registration_date` | DATE | parsed (no wall-clock bounds — A2) |
| `kyc_status` | STRING | domain |
| `customer_segment` | STRING | domain, nullable |
| `annual_income` | DECIMAL(12,2) | ≥ 0, ≤ 10,000,000; nullable |
| `status` | STRING | domain |

### 3.2 `silver.accounts` — PK `account_id`
`cb_customer_ref` STRING · `global_customer_ref` STRING · `customer_email` STRING (normalized, invalid→NULL) · `account_number` STRING · `account_type` STRING · `currency` STRING · `balance` DECIMAL(15,2) nullable · `status` STRING · `open_date` DATE · `last_activity_date` DATE nullable · `branch_code` STRING nullable.

### 3.3 `silver.cards` — PK `card_id`
`cc_customer_ref` STRING · `global_customer_ref` STRING · `customer_email` STRING (normalized) · `card_number_masked` STRING · `card_type` STRING · `credit_limit` DECIMAL(12,2) nullable · `outstanding_balance` DECIMAL(12,2) nullable · `card_status` STRING · `issue_date` DATE · `expiry_date` DATE.

### 3.4 `silver.loans` — PK `loan_id`
`ln_customer_ref` STRING · `global_customer_ref` STRING · `customer_email` STRING (normalized) · `loan_type` STRING · `principal_amount` DECIMAL(14,2) · `outstanding_balance` DECIMAL(14,2) · `interest_rate` DECIMAL(5,2) (3.00–25.00) · `term_months` INT (6–360) · `monthly_installment` DECIMAL(12,2) · `start_date` DATE · `end_date` DATE nullable · `repayment_status` STRING.

### 3.5 `silver.digital_logins` — PK `login_id`
`db_customer_ref` STRING · `global_customer_ref` STRING · `customer_email` STRING (normalized) · `login_timestamp` TIMESTAMP · `channel` STRING · `device_id` STRING · `device_type` STRING · `os` STRING · `browser` STRING · `login_status` STRING · `session_duration_minutes` INT nullable (cast via DECIMAL, **integral values only** — Bronze stores `'150.0'`-style decimal notation, A8; NULL only when `login_status = FAILED`).

### 3.6 `silver.support_tickets` — PK `ticket_id`
`cs_customer_ref` STRING · `global_customer_ref` STRING · `customer_email` STRING (normalized) · `channel` STRING · `category` STRING · `priority` STRING · `status` STRING · `created_date` DATE · `resolved_date` DATE nullable · `csat_score` INT nullable (cast via DECIMAL(2,1); Bronze stores `'1.0'`–`'5.0'` — A5; NULL only when unresolved).

## 4. Deduplication Policy (Amendment 1)

**Scope:** within batch, within table, keyed on the entity PK (identity spec §3 level 1). Cross-source duplication is Phase 6.

**Order of operations (fixed):** dedup first, then validate survivors. Every Bronze row is therefore classified exactly once (§8 reconciliation).

**Duplicate classification (Amendment 7, approved 2026-11-24; literal semantics reaffirmed 2026-11-24):** every eliminated row is classified against **all records in its PK group, not only the selected survivor**. A loser is `EXACT_DUPLICATE` iff its raw serialized business bytes are **byte-identical to at least one OTHER record anywhere in the same PK group** (no ordering requirement; the first member of a byte-identical subset is *not* NEAR when another identical record exists in the group); otherwise it is `NEAR_DUPLICATE` (not byte-identical to any other record in the group). Survivor selection (Amendment 1 precedence below) is unchanged and does not participate in classification.

**Near duplicates** (same PK, ≥1 differing business attribute): one **survivor** selected by this precedence — **Amendment 1 order; business/event recency is explicitly NOT a criterion:**

1. **Most complete record** — fewest NULL business attributes.
2. **Lowest SHA-256 hash** over the *canonicalized source* business columns (post-§5 transformation values; derived columns such as `name_key` excluded; deterministic length-prefixed serialization — NULL ≠ empty, delimiter-safe — sha2-256; remediated 2026-11-24, previously `concat_ws('||', …)`).
3. **Stable deterministic tie-breaker** — lexicographic comparison over the full serialized canonical row, then raw bytes, then the Bronze lineage columns for full determinism among byte-identical rows (Amendment 7). Extending the tie-break does not change which business record survives — rows tied through raw bytes differ only in lineage (immaterial choice, documented) — it only pins which identical copy is retained and makes the exact/near labels of identical copies deterministic.

**Documentation requirements (Amendment 1):** the hash tie-breaker is **deterministic but does not establish factual correctness** — it guarantees reproducibility, not truth. If an authoritative source update timestamp becomes available in a future phase, it MAY be introduced as a higher-priority criterion via spec amendment; Phase 5 data carries none.

**Dropped records:** every eliminated row is written to `quarantine.duplicate_records` (§9.2) with its reason and the surviving PK (`_survivor_pk`). Nothing is discarded silently.

**Expected batch_001 eliminations** (verified live 2026-11-24 under the reaffirmed literal A7 definition): customers 244 exact + 176 near = 420 · accounts 284+206 = 490 · cards 132+99 = 231 · loans 56+42 = 98 · digital_logins 1,248+677 = 1,925 · support_tickets 203+112 = 315. *Why the exact/near split changed from the previously recorded 241/179, 280/210, 202/113, 1,239/686:* under the literal definition, **both** members of a byte-identical loser pair are EXACT — each is byte-identical to another record in the group — even when the survivor is a distinct variant. In the affected groups (3 customers, 4 accounts, 1 support_tickets, 9 digital_logins; the swapcase-email pattern where the uppercase-raw row wins the deterministic tie-break, e.g. CIS-03247) the previous "first-copy-retained" rule classified one twin EXACT and the other NEAR, under-counting EXACT by exactly one per group (+3/+4/+1/+9). Silver rows, duplicate totals, rejection counts, and reconciliation identities are unchanged.

## 5. Transformation Rules

### 5.1 Casting
Explicit `try_cast` per column to the §3 types. A non-null Bronze value that fails its cast is quarantined `NUMERIC_UNPARSEABLE` (live: 0). Nullable numeric columns keep NULL (A1: 741 `balance`, 346 `credit_limit`, 341 cards `outstanding_balance` pass through).

### 5.2 Dates & timestamps (Amendment 4)

Approved parser patterns — **defined from actual Bronze values** (verified 2026-11-20):

* Dates (10 date columns): `yyyy-MM-dd` (ISO, ~60%), `dd/MM/yyyy` (~25%), `MM-dd-yyyy` (~15%).
* `login_timestamp`: `yyyy-MM-dd HH:mm:ss` (ISO, carries seconds, 33,981 rows), `dd/MM/yyyy HH:mm` (14,340), `MM-dd-yyyy HH:mm` (8,604 — third pattern observed live; contracts §5 lists two; A6).

Rules:

1. Parse with `try_to_date` / `try_to_timestamp` over the pattern set, `coalesce`d in the listed order.
2. **Exactly-one-pattern verification:** for every non-null value, the number of approved patterns that match must be **exactly 1**. Zero matches → record quarantined `INVALID_DATE_FORMAT` / `INVALID_TIMESTAMP_FORMAT`. More than one match (impossible today — patterns are separator-disjoint: ISO/MDY use `-`, DMY uses `/`) → **batch fails closed** as a systemic format-ambiguity fault; no parser is ever silently selected.
3. Wall-clock bounds (`≤ current date`, trailing-12-months) are **generation-time contract rules, not Silver rules** (A2 — evidence: every product date ≤ 2026-11-30, generator ran 2026-11-19; re-checking against wall clock would make results drift day by day and quarantine 4,051 valid rows today).
4. Silver enforces **cross-field ordering** instead (§7).

### 5.3 Email
`lower(trim())`; require `^[^@ ]+@[^@ ]+\.[^@ ]+$`; invalid → **NULL** + audit count `invalid_emails_nulled` (D6 — identity §1: invalid email is not a match key; never a rejection; production-verified counts 410/277/142/52/1,225/210 per **A9**: invalid **after** normalization — `lower(trim())` is applied *before* the regex. The generator's raw injected email defects total 611/396/215/88/1,880/289, of which 201/119/73/36/655/79 are whitespace-wrapped values (` email@example.org `) that become valid once trimmed and are therefore preserved in Silver, not nulled). Raw value remains in Bronze.

### 5.4 Phone (Amendment 2)

1. Strip all non-digit characters.
2. If ≥10 digits remain → keep the **last 10**.
3. If <10 digits → normalized phone = **NULL**, counted in `processing_audit.invalid_phones_nulled`.
4. Never reject the record for an invalid phone.

Acceptance rule: **exactly 10 digits or NULL — nothing else** (live: all 12,420 phones have exactly 10 digits; counts `invalid_phones_nulled = 0` for batch_001). Note (A4): this <10-digit threshold supersedes identity §1's <7 for Phase 5; the identity spec will be aligned at Phase 6 (no behavioral difference on this batch).

### 5.5 Identifiers & text
Uppercase/trim all PKs and native refs, validate per §2. Trim all STRING fields. `name_key` built per §3.1. No other coercion, no business logic, no value invention.

## 6. Pass-Through by Design (never rejections)

* Contract-nullable engineered NULLs (email 5–10%, dates, numerics per A1).
* Invalid emails / short phones / malformed `global_customer_ref` → NULL + audit (D6, Amendment 2).
* **139 negative balances** (overdrafts) and **128 over-limit cards** — valid real-world states; the cards contract itself says violations are flagged, not quarantined.
* Duplicate rows — handled by dedup (§4), not quarantine.
* Referential orphans (~2%) — **deferred to Phase 6 by design**: product rows whose customer ref resolves to nobody cannot be detected until the identity crosswalk exists; they pass through Silver with native refs + `global_customer_ref` + emails preserved for matching (identity §5 routes unresolvable cases to review then).

## 7. Validation Rules & Rejection Reasons

Every rule maps to an approved source-contract validation rule. Quarantine writes **one row per distinct failure reason** (data-model §2), but reconciliation counts **distinct source records** (Amendment 3, §8).

| Rule ID | Condition | Live batch_001 |
|---------|-----------|----------------|
| `PK_NULL` / `PK_INVALID_FORMAT` | null / malformed PK (§2 formats) | 0 / 0 |
| `NATIVE_REF_INVALID_FORMAT` | malformed mandatory native customer ref | 0 |
| `REQUIRED_FIELD_NULL` | contract-required field null (names, country, kyc/status, account_type/currency, account_number, card_type/status, loan numerics/dates, channel/device fields, ticket enums, created_date) | 0 |
| `INVALID_DATE_FORMAT` / `INVALID_TIMESTAMP_FORMAT` | unparseable required date/timestamp (Amendment 4 §5.2) | 0 |
| `NUMERIC_UNPARSEABLE` | non-null numeric fails cast | 0 |
| `ENUM_VIOLATION` | value outside contract domain (gender, country, kyc, segment, status, account_type, currency, card/status, loan/repayment, channel/device/login, support channel/category/priority/status) | 0 |
| `NAME_INVALID` | first/last name not letters after trim | 0 |
| `POSTAL_INVALID` | postal_code not 4–6 chars when present | 0 |
| `ACCOUNT_NUMBER_INVALID` / `BRANCH_CODE_INVALID` / `MASKED_PAN_INVALID` | §2 formats | 0 |
| `INCOME_OUT_OF_RANGE` | annual_income outside [0, 10,000,000] | 0 |
| `RATE_OUT_OF_RANGE` / `TERM_OUT_OF_RANGE` / `AMOUNT_INVALID` | interest 3.00–25.00; term 6–360; principal/installment > 0; outstanding ≥ 0 | 0 |
| `DOB_AFTER_REGISTRATION` | date_of_birth > registration_date | 0 |
| `LAST_ACTIVITY_BEFORE_OPEN` | parsed `last_activity_date` < `open_date` | 0 |
| `EXPIRY_NOT_AFTER_ISSUE` | `expiry_date` ≤ `issue_date` | 0 |
| `END_BEFORE_START` | `end_date` ≤ `start_date` | 0 |
| `END_DATE_REQUIRED` | `repayment_status = PAID_OFF` and `end_date` NULL | 0 |
| `RESOLVED_BEFORE_CREATED` | `resolved_date` < `created_date` | 0 |
| `RESOLVED_DATE_REQUIRED` | status ∈ {RESOLVED, CLOSED} and `resolved_date` NULL | 0 |
| `CSAT_INVALID` | csat outside 1–5, or populated while unresolved | 0 |
| `SESSION_DURATION_INVALID` | negative duration, populated when FAILED, or NULL when SUCCESS | 0 |
| `LOAN_ANOMALY_OUTSTANDING_GT_PRINCIPAL` | flagged in audit (`loan_anomalies_flagged`), **not quarantined** (contract: "flagged as anomaly") | 0 |

**Deliberately not enforced in Silver** (generation-anchored, documented): DOB age band 18–100 **at registration** (A3 — 535 live rows are under 18 at registration because DOB was anchored to generation-time age while registration dates are independent; a Phase 3 spec amendment is recommended, this Phase 5 spec does not act on it); wall-clock upper bounds (A2).

## 8. Reconciliation Formula (Amendment 3 — distinct records, never reason-rows)

For each entity `T` and batch `B`:

```
bronze_rows(T,B) = silver_rows(T,B) + duplicate_rows(T,B) + rejected_distinct(T,B)
```

* `silver_rows` — rows appended to `silver.T` for `B`.
* `duplicate_rows` — rows in `quarantine.duplicate_records` for (`T`,`B`) (§4: exact + near losers; each eliminated row counted once).
* `rejected_distinct(T,B)` — **distinct rejected source records** in `invalid_T` for `B`, counted as `COUNT(DISTINCT <PK>)`. Because processing order is dedup-then-validate, exactly one survivor per PK is validated, so rejected records are unique by PK; a record with **k failure reasons is written k times** (row-per-reason audit) but **counted once**. The general definition (if PKs could ever repeat among rejects) is distinct full original business rows.
* Secondary identity (PK partition): `bronze_distinct_pks(T,B) = silver_distinct_pks(T,B) + rejected_distinct(T,B)` — every Bronze PK resolves to exactly one survivor, which lands in Silver **or** quarantine, never both, never neither.

Multiple rejection reasons can therefore never inflate the source-to-target reconciliation. `processing_audit` persists both `rejected_distinct_rows` and `rejection_reason_rows` (the latter for defect analytics only).

**Batch_001 expectations:** customers 12,420 = 12,000 + 420 + 0 · accounts 14,490 = 14,000 + 490 + 0 · cards 6,831 = 6,600 + 231 + 0 · loans 2,898 = 2,800 + 98 + 0 · digital_logins 56,925 = 55,000 + 1,925 + 0 · support_tickets 9,315 = 9,000 + 315 + 0.

## 9. Quarantine & Audit Schemas

### 9.1 `customer_360.quarantine.invalid_<entity>` (six tables)
All original Bronze business columns (raw STRING values, unchanged) + `_rejection_reason` STRING (`<RULE_ID>: <message>`) + the five Bronze lineage columns (D9) + `_quarantined_timestamp`. **Row per distinct failure reason**; reconciliation uses distinct records (§8).

### 9.2 `customer_360.quarantine.duplicate_records` (shared, D2)
`_entity` STRING (`customers`…`support_tickets`) · `_record_pk` STRING · `_dedup_reason` STRING (`EXACT_DUPLICATE` / `NEAR_DUPLICATE`) · `_survivor_pk` STRING · `_source_row_json` STRING (full original business row as JSON — the entities have different column sets, so the shared table stores the complete row losslessly) · five Bronze lineage columns (D9) + `_quarantined_timestamp`.

### 9.3 `customer_360.silver.processing_audit` (shared, D3)
One row per (`table`, `batch`): `table_name`, `batch_id`, `run_timestamp`, `bronze_rows`, `silver_rows`, `duplicate_rows` (`exact_duplicate_rows`, `near_duplicate_rows`), `rejected_distinct_rows`, `rejection_reason_rows`, `invalid_emails_nulled`, `invalid_refs_nulled`, `invalid_phones_nulled`, `loan_anomalies_flagged`, `checks_passed`, `checks_total`, `notes`.

### 9.4 Quarantine testing (Amendment 6)
Synthetic invalid records exist **only** inside unit tests / temporary test DataFrames and temp views. Nothing synthetic is ever appended to Bronze, Silver, or the production quarantine tables; batch_001 source data is never modified. The production `invalid_*` tables are written exclusively by the real pipeline over real Bronze rows.

## 10. Incremental Processing & Idempotency

* Batch-at-a-time processing keyed on `_batch_id`, mirroring Phase 4: pre-checks → per-table (dedup → transform → validate) → **single atomic append** to `silver.<entity>` + `quarantine.invalid_<entity>` + `duplicate_records` + `processing_audit`.
* **Fail-closed duplicate-batch guard**: any target already containing rows for the batch aborts the whole run before any write (Phase 4 hardening: `F.col` only, no swallowed exceptions).
* Silver append-only; no overwrite mode anywhere; re-running a processed batch aborts; repair is the documented manual procedure (targeted `DELETE WHERE _batch_id` or Delta time-travel restore), never automatic.
* Dedup scope is within-batch by design; cross-batch duplicate handling is documented as Phase 6+ scope (identity invariants prevent identity drift; xref rows immutable).
* batch_002 (planned ~5% attribute drift) processes identically without touching batch_001 rows.

## 11. Acceptance Criteria (11 checks, all fail-closed)

1. **Schema/type exactness** — each Silver table matches §3 exactly (names, order, contract types).
2. **Reconciliation identity (Amendment 3)** — both §8 identities hold per table per batch, against the batch_001 expectations above; multi-reason records counted once.
3. **PK uniqueness** — zero PK repeats in every Silver table; zero full-row duplicate business rows.
4. **Normalization proofs** — email valid-or-NULL and lowercase; phone **exactly 10 digits or NULL** (Amendment 2); zero `other`-format dates/timestamps; `name_key` populated on customers; all strings trimmed.
5. **Engineered-null preservation** — nullable-column NULL counts equal Bronze counts adjusted for invalid→NULL (production-verified 2026-11-24 per **A9**: customers email NULL = 1,009 = 615 engineered-NULL families + 394 invalid-nulled families; the earlier "634 engineered + 611 invalid = 1,245" arithmetic double-counted raw defects that trimming recovers or dedup eliminates).
6. **Value-nullification audit** — `invalid_emails_nulled`, `invalid_refs_nulled`, `invalid_phones_nulled` in `processing_audit` match Bronze-side counts exactly, evaluated under the §5.3 rule order (normalize first, validate after — A9).
7. **Dedup determinism (Amendment 1)** — recomputing survivor selection reproduces identical survivors (hash comparison); precedence order documented in code; tie-break firings logged; hash documented as deterministic-not-truth-asserting.
8. **Bronze untouched** — Bronze row counts and latest Delta version identical before/after the run.
9. **Idempotency negative test** — re-running the processed batch aborts; zero rows added to any table.
10. **Quarantine positive test — isolated (Amendment 6)** — synthetic bad rows (≥1 per rule ID) exercised in unit tests on temp DataFrames only; each produces the correct reason; nothing written to production tables.
11. **Lineage completeness** — five Bronze lineage columns + `_silver_processed_timestamp` populated on 100% of Silver rows; five lineage columns on 100% of quarantine rows.

## 12. Implementation Plan (after final approval)

1. `src/config/silver_config.py` — schemas, rule IDs, pattern sets, expected batch_001 counters.
2. `src/silver/standardizer.py` — pure functions: date/timestamp parsing + exactly-one-pattern check, email/phone/identifier/name_key normalization, hash & completeness survivor selection.
3. `src/silver/transform.py` — per-table pipelines: dedup → transform → validate → quarantine/audit writers, with the fail-closed batch guard.
4. `tests/test_silver_transformation.py` — unit tests incl. isolated synthetic-quarantine tests (Amendment 6) and explicit coverage of **all three approved date/timestamp patterns at both precisions** (A6); run **before** any write.
5. `notebooks/05_silver_standardization.py` — orchestrator + the 11 acceptance checks.
6. Run tests → run pipeline → acceptance report → update `data/README.md` → commit & push.

No Phase 3/4 files are modified; `fraud_detection` untouched; no Gold; no identity resolution.

## 13. Assumptions & Deviations (all evidence-based, flagged for the record)

* **A1 — Nullable numeric NULLs pass through** (741 balance, 346 credit_limit, 341 cards outstanding): the contracts' defect table governs ("missing values on nullable fields → NULL handling in Silver"); the cards row's "NULLs quarantined" phrasing is read as metrics-handling, not rejection. Unparseable non-null values still quarantine.
* **A2 — Wall-clock date bounds are generation-anchored** and are not re-enforced in Silver (all dates ≤ 2026-11-30 vs generation on 2026-11-19; 4,051 rows would otherwise be "future" today and the count would drift daily). Cross-field ordering rules are enforced instead.
* **A3 — DOB age band (18–100) is generation-anchored — synthetic-data limitation, recorded for future review (per final approval 2026-11-21):** **535 synthetic customers (4.3%) are under 18 at registration** because DOB reflects generation-time age while registration dates are independent/inherited. Silver introduces **no age-based rejection rule** — none is enforceable without an approved source-contract requirement as consumed (the contracts' age band is a generation-time constraint; see A2). **batch_001 is not regenerated or modified.** The discrepancy is recorded here as a synthetic-data limitation for future review; a Phase 2/3 spec amendment is the venue to revisit it. Silver enforces only `DOB_AFTER_REGISTRATION` (0 violations).
* **A4 — Phone threshold**: Amendment 2's <10-digits→NULL supersedes the identity spec's original <7 rule; **the identity spec was aligned accordingly on 2026-11-21** (same normalization, same never-independently-rejects semantics, R1–R5 unchanged). No behavioral difference on batch_001 (all phones have exactly 10 digits).
* **A5 — `csat_score` stored as `'1.0'`–`'5.0'`** in Bronze (valid values; cast via DECIMAL(2,1) to INT).
* **A7 — Exact/near classification is group-wide (approved 2026-11-24; literal semantics reaffirmed 2026-11-24):** a duplicate loser is `EXACT_DUPLICATE` iff its raw serialized business bytes are **byte-identical to at least one OTHER record anywhere in the same PK group** — no ordering requirement; the first member of a byte-identical subset is NOT near when another identical record exists. Otherwise it is `NEAR_DUPLICATE` (not byte-identical to any other record in the group). Classification is evaluated against all records in the group, never only the survivor; survivor selection (Amendment 1 precedence) is unchanged and was not adjusted to force counts. Verified on batch_001 under the literal definition: customers 244/176, accounts 284/206, cards 132/99, loans 56/42, digital_logins 1,248/677, support_tickets 203/112 (totals 420/490/231/98/1,925/315 — invariant). An interim implementation used an earlier-record ("first-copy-retained") reading that produced 241/179, 280/210, 202/113, 1,239/686 — it under-counted EXACT by one per group where a byte-identical pair lost to a distinct survivor, and is superseded by this entry.
* **A8 — `session_duration_minutes` stored as integral decimal notation** in Bronze (verified 2026-11-24: 52,892 `'150.0'`-style rows, 4,033 NULL on FAILED, no integer notation, no genuinely fractional values): cast via DECIMAL to INT with **integral values only** — `'150.0'` → 150, `'65.0'` → 65, NULL → NULL; genuinely fractional values (`'150.7'`) are rejected as `SESSION_DURATION_INVALID` and never silently truncated to an integer. Same policy as `csat_score` (A5). `term_months` remains strict integer-notation parsing (verified: all 2,898 values integer notation).
* **A6 — Third timestamp pattern** (`MM-dd-yyyy HH:mm`, **8,604 rows** observed) is approved alongside the two patterns the contracts §5 lists for `login_timestamp`; Amendment 4 requires defining patterns from actual values, so all three are approved parsers. **Parsing is explicit per pattern (no format inference), and unit tests must cover all three approved patterns at both timestamp precisions** (ISO with seconds `yyyy-MM-dd HH:mm:ss`; `dd/MM/yyyy HH:mm`; `MM-dd-yyyy HH:mm`) — see §12.
* **D2 layout** — the shared `duplicate_records` table stores original rows as `_source_row_json` because the six entities have different column sets; all key fields (`_entity`, `_record_pk`, `_survivor_pk`, reason, lineage) are first-class columns.
* **A9 — Email invalidity is evaluated after normalization** (approved 2026-11-24, post-production Option A): §5.3's written order — `lower(trim(email))` followed by regex validation — is authoritative and the implementation already follows it. The generator injects three raw email defect forms (`@`→" at ", dot-stripped domain, whitespace-wrapped ` value `); the first two remain invalid after trimming, but the third becomes a valid email once trimmed and is **preserved in Silver, not nulled**. Production-verified `invalid_emails_nulled` = 410/277/142/52/1,225/210 (customers…support_tickets); the previously recorded 611/396/215/88/1,880/289 counted raw injected defects including 201/119/73/36/655/79 whitespace-wrapped ones. Also verified in the production run: `quarantine.duplicate_records` batch_001 total = **3,479** (the exact sum of the per-entity approved counts — 244+176, 284+206, 132+99, 56+42, 1,248+677, 203+112; the "3,279" quoted in the 2026-11-24 execution approval was an arithmetic slip). No transformation logic, Silver data, audit rows, or other business rule changed — specification and `EXPECTED_BATCH_001` were reconciled to the production results.
