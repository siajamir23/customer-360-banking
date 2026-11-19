# 02 — Source Data Contracts

**Phase:** 2 (Specifications & Data Modeling) · **Status:** In review  
**Depends on:** `phase-1-environment-inspection.md` (verified fraud platform facts)

## Objective

Define the logical and physical contracts for the six synthetic source systems that feed the Customer 360 platform: every column's type, meaning, key role, nullability, and validation rules, plus the deliberate data defects each source injects. These contracts are the input for Bronze ingestion (Phase 4) and Silver validation (Phase 5).

## Conventions & Assumptions

1. **Raw grain**: every source file is landed as-is into `customer_360.bronze` with **all business columns typed STRING** (raw-is-raw principle, same as the fraud project); logical types below are enforced in Silver.
2. **Identifier domains**: each operational system uses its **own native customer reference** (`CIS-`, `CB-`, `CC-`, `LN-`, `DB-`, `CS-` prefixes). Some records additionally carry a `global_customer_ref` in the enterprise `CUST-######` domain — verified in Phase 1 to be the exact domain of `fraud_detection.silver.customers` (CUST-000001 … CUST-010000, 100% conformant, 100% distinct). Identity resolution (spec `02-customer-identity-resolution.md`) unifies all references to that `CUST-######` domain.
3. **Population seed**: the shared synthetic population is **12,000 persons** — **all 10,000 existing fraud-platform customers** (read-only source inputs, `CUST-000001…CUST-010000`, giving 100% integration coverage of the verified domain) plus **2,000 new synthetic customers** who carry no `global_customer_ref` and receive identifiers from identity-resolution rule R5 starting at `CUST-100001` (Phase 6). Seeded persons inherit age/country/registration dates from the fraud platform so the two platforms never contradict each other.
4. **Country domain**: `PH, US, SG, AU, GB` (inherited from the fraud platform). **Currencies**: `PHP, USD, SGD, AUD, GBP`.
5. **Row-count targets** (masters, excluding injected duplicates; safe for serverless compute): customers 12,000 · accounts 14,000 · cards 6,600 · loans 2,800 · digital logins 55,000 · support cases 9,000.
6. **Monetary values** are DECIMAL(x,2); **no real PII** — all data is synthetic.
7. **Email handling (Amendment 1 — approved 2026-11-19):** product-source `customer_email` represents the record's holder; emails are normalized (`trim` + `lower`) before any matching; a **missing** email is never an automatic rejection (the record falls to the next match rule); a **conflicting** email match is quarantined, never auto-merged; email alone never overrides a contradicting higher-priority identifier.

## Deliberate Defect Injection (applies across sources)

| Defect | Rate | Rationale |
|--------|------|-----------|
| Exact duplicate rows | ~2% | Silver deduplication by row window |
| Near-duplicate rows (key attr same, one field differs) | ~1.5% | Survivorship rules in identity resolution |
| Missing values on nullable fields | 5–10% per affected column | NULL handling in Silver |
| Mixed date formats (`YYYY-MM-DD`, `DD/MM/YYYY`, `MM-DD-YYYY`) | ~40% of date columns | Normalization rules in Silver |
| Invalid emails (missing `@`, uppercase, whitespace) | ~5% of emails | Email validation + normalization |
| Inconsistent phone formats (`+63 917 123 4567`, `0917-123-4567`, `(02) 8123 4567`) | ~all phones | Phone normalization (digits only, last 10) |
| Cross-system attribute conflicts (e.g., different email for same person) | ~3% of matched persons | Conflict resolution precedence |
| Referential orphans (product rows whose customer ref resolves to nobody) | ~2% | Quarantine + reconciliation reporting |

---

## 1. Customer Information System (CIS) — `customers_raw`

**Grain:** one row per customer master record (with injected duplicates). **Primary key:** `cis_customer_id`. **Volume:** ~10,000 rows. **Authoritative for:** demographics, contact details, KYC, segment.

| Column | Logical Type | Description | Key | Nullable | Validation Rules |
|--------|-------------|-------------|-----|----------|------------------|
| `cis_customer_id` | STRING | CIS-native customer reference (`CIS-#####`) | **PK** | No | Matches `^CIS-[0-9]{5}$`; duplicates allowed in raw, deduped in Silver |
| `global_customer_ref` | STRING | Enterprise customer ID (`CUST-######`) where the customer is already registered; **populated for all 10,000 seeded persons (~83% of records)**; the 2,000 new customers carry NULL | Business key | Yes | Matches `^CUST-[0-9]{6}$` when present; must exist in the enterprise ID registry (fraud platform domain) |
| `first_name` | STRING | Given name | | No | Non-empty, letters only after trimming; casing varies (normalized in Silver) |
| `middle_name` | STRING | Middle name / initial | | Yes | Letters or single letter |
| `last_name` | STRING | Family name | | No | Non-empty, letters only after trimming |
| `date_of_birth` | STRING | Date of birth — **mixed formats** | Match key | No | Parses under any of the 3 contracted formats; age 18–100 at registration |
| `gender` | STRING | `M`, `F`, `U` | | Yes | Domain check |
| `email` | STRING | Primary email — case varies, some invalid | Match key | Yes | Contains `@` and a dot in the domain; normalized `lower(trim())` |
| `phone` | STRING | Primary phone — inconsistent formats | Match key | Yes | ≥ 7 digits after stripping non-digits; normalized to last-10-digits |
| `address_line1` | STRING | Street address | | Yes | Non-empty when present |
| `city` | STRING | City | | Yes | Non-empty when present |
| `country` | STRING | ISO code | | No | ∈ {PH, US, SG, AU, GB} |
| `postal_code` | STRING | ZIP/postal code | | Yes | 4–6 chars |
| `registration_date` | STRING | Customer onboarding date — **mixed formats** | | No | Parses; ≤ current date; ≥ 1990-01-01 |
| `kyc_status` | STRING | `VERIFIED`, `PENDING`, `REJECTED` | | No | Domain check |
| `customer_segment` | STRING | `RETAIL`, `PREMIUM`, `PRIVATE`, `MASS_AFFLUENT` | | Yes | Domain check; nullable (assigned by 360 segmentation later) |
| `annual_income` | DECIMAL(12,2) | Declared annual income (used for DTI in financial summary) | | Yes | ≥ 0; ≤ 10,000,000 |
| `status` | STRING | `ACTIVE`, `INACTIVE`, `CLOSED` | | No | Domain check |

---

## 2. Core Banking System — `accounts_raw`

**Grain:** one row per deposit account. **Primary key:** `account_id`. **Volume:** ~12,000 rows (1–2 accounts per customer).

| Column | Logical Type | Description | Key | Nullable | Validation Rules |
|--------|-------------|-------------|-----|----------|------------------|
| `account_id` | STRING | Deposit account ID (`ACC-#####`) | **PK** | No | `^ACC-[0-9]{5}$` |
| `cb_customer_ref` | STRING | Core-banking-native customer reference (`CB-#####`) | FK→identity | No | `^CB-[0-9]{5}$`; resolves via identity resolution |
| `global_customer_ref` | STRING | Enterprise `CUST-######` when known — **populated ~75%** | Business key | Yes | `^CUST-[0-9]{6}$` when present |
| `customer_email` | STRING | Account holder's email as recorded by Core Banking (identity-resolution match key — Amendment 1) | Match key | Yes | Represents the holder; ~85% populated; normalized `lower(trim())` before matching; missing ≠ rejection; conflicting match → quarantine |
| `account_number` | STRING | Public account number | | No | 10 digits |
| `account_type` | STRING | `SAVINGS`, `CURRENT`, `FIXED_DEPOSIT` | | No | Domain check |
| `currency` | STRING | `PHP`, `USD`, `SGD`, `AUD`, `GBP` | | No | Domain check |
| `balance` | DECIMAL(15,2) | Current balance; a few negatives = overdrafts (valid), NULLs = missing | | Yes | Numeric; NULL → quarantined if unparseable |
| `status` | STRING | `ACTIVE`, `DORMANT`, `CLOSED` | | No | Domain check |
| `open_date` | STRING | Account opened — **mixed formats** | | No | Parses; ≤ current date |
| `last_activity_date` | STRING | Last transaction date — mixed formats | | Yes | Parses; ≥ `open_date` |
| `branch_code` | STRING | Branch (`BR-###`) | | Yes | `^BR-[0-9]{3}$` |

---

## 3. Credit Card System — `cards_raw`

**Grain:** one row per credit card. **Primary key:** `card_id`. **Volume:** ~6,000 rows.

| Column | Logical Type | Description | Key | Nullable | Validation Rules |
|--------|-------------|-------------|-----|----------|------------------|
| `card_id` | STRING | Card account ID (`CRD-#####`) | **PK** | No | `^CRD-[0-9]{5}$` |
| `cc_customer_ref` | STRING | Card-system-native customer reference (`CC-#####`) | FK→identity | No | `^CC-[0-9]{5}$` |
| `global_customer_ref` | STRING | Enterprise `CUST-######` when known — **~80%** | Business key | Yes | `^CUST-[0-9]{6}$` when present |
| `customer_email` | STRING | Card holder's email as recorded by the Credit Card System (identity-resolution match key — Amendment 1) | Match key | Yes | Represents the holder; ~85% populated; normalized `lower(trim())` before matching; missing ≠ rejection; conflicting match → quarantine |
| `card_number_masked` | STRING | Masked PAN (`****-****-****-1234`) | | No | Masked format; never store full PAN (PCI mindset) |
| `card_type` | STRING | `CLASSIC`, `GOLD`, `PLATINUM`, `SIGNATURE` | | No | Domain check |
| `credit_limit` | DECIMAL(12,2) | Approved limit | | Yes | > 0; NULLs quarantined for limit-based metrics |
| `outstanding_balance` | DECIMAL(12,2) | Current outstanding | | Yes | ≥ 0; ≤ credit_limit is expected — violations flagged (not quarantined: real over-limit states exist) |
| `card_status` | STRING | `ACTIVE`, `BLOCKED`, `EXPIRED`, `CANCELLED` | | No | Domain check |
| `issue_date` | STRING | Card issued — mixed formats | | No | Parses; ≤ current date |
| `expiry_date` | STRING | Card expiry — mixed formats | | No | Parses; > `issue_date` |

---

## 4. Loan Management System — `loans_raw`

**Grain:** one row per loan. **Primary key:** `loan_id`. **Volume:** ~2,500 rows.

| Column | Logical Type | Description | Key | Nullable | Validation Rules |
|--------|-------------|-------------|-----|----------|------------------|
| `loan_id` | STRING | Loan ID (`LON-#####`) | **PK** | No | `^LON-[0-9]{5}$` |
| `ln_customer_ref` | STRING | Loan-system-native customer reference (`LN-#####`) | FK→identity | No | `^LN-[0-9]{5}$` |
| `global_customer_ref` | STRING | Enterprise `CUST-######` when known — **~70%** (weakest integration maturity, more identity-resolution work) | Business key | Yes | `^CUST-[0-9]{6}$` when present |
| `customer_email` | STRING | Loan holder's email as recorded by the Loan Management System (identity-resolution match key — Amendment 1) | Match key | Yes | Represents the holder; ~85% populated; normalized `lower(trim())` before matching; missing ≠ rejection; conflicting match → quarantine |
| `loan_type` | STRING | `PERSONAL`, `HOME`, `AUTO`, `EDUCATION`, `BUSINESS` | | No | Domain check |
| `principal_amount` | DECIMAL(14,2) | Original principal | | No | > 0 |
| `outstanding_balance` | DECIMAL(14,2) | Remaining balance | | No | ≥ 0; > `principal_amount` → flagged as anomaly (data defect) |
| `interest_rate` | DECIMAL(5,2) | Annual rate % | | No | 3.00–25.00 |
| `term_months` | INT | Tenor | | No | 6–360 |
| `monthly_installment` | DECIMAL(12,2) | Monthly payment | | No | > 0 |
| `start_date` | STRING | Disbursement date — mixed formats | | No | Parses; ≤ current date |
| `end_date` | STRING | Scheduled payoff — mixed formats | | Yes | Parses; > `start_date`; required when `repayment_status = PAID_OFF` |
| `repayment_status` | STRING | `CURRENT`, `LATE`, `DEFAULT`, `PAID_OFF` | | No | Domain check |

---

## 5. Digital Banking System — `digital_logins_raw`

**Grain:** one row per login **event** (highest-volume fact-like source). **Primary key:** `login_id`. **Volume:** ~50,000 rows over a trailing 12-month window.

| Column | Logical Type | Description | Key | Nullable | Validation Rules |
|--------|-------------|-------------|-----|----------|------------------|
| `login_id` | STRING | Event ID (`LOG-#######`) | **PK** | No | `^LOG-[0-9]{7}$` |
| `db_customer_ref` | STRING | Digital-system-native customer reference (`DB-#####`) | FK→identity | No | `^DB-[0-9]{5}$` |
| `global_customer_ref` | STRING | Enterprise `CUST-######` when known — **~85%** (best-integrated system) | Business key | Yes | `^CUST-[0-9]{6}$` when present |
| `customer_email` | STRING | Digital-banking user's email as recorded by the Digital Banking System (identity-resolution match key — Amendment 1) | Match key | Yes | Represents the user; ~85% populated; normalized `lower(trim())` before matching; missing ≠ rejection; conflicting match → quarantine |
| `login_timestamp` | STRING | Event time — **mixed formats** (`YYYY-MM-DD HH:MM:SS`, `DD/MM/YYYY HH:MM`) | | No | Parses to TIMESTAMP; within trailing 12 months |
| `channel` | STRING | `WEB`, `MOBILE_APP` | | No | Domain check |
| `device_id` | STRING | Device fingerprint | | No | Non-empty |
| `device_type` | STRING | `ANDROID`, `IOS`, `DESKTOP`, `TABLET` | | No | Domain check |
| `os` | STRING | OS name | | Yes | Free text |
| `browser` | STRING | Browser name | | Yes | Free text |
| `login_status` | STRING | `SUCCESS`, `FAILED` | | No | Domain check |
| `session_duration_minutes` | INT | Session length | | Yes | ≥ 0; NULL allowed only when `login_status = FAILED` |

---

## 6. Customer Support System — `support_cases_raw`

**Grain:** one row per support ticket. **Primary key:** `ticket_id`. **Volume:** ~8,000 rows.

| Column | Logical Type | Description | Key | Nullable | Validation Rules |
|--------|-------------|-------------|-----|----------|------------------|
| `ticket_id` | STRING | Ticket ID (`TKT-#####`) | **PK** | No | `^TKT-[0-9]{5}$` |
| `cs_customer_ref` | STRING | Support-system-native customer reference (`CS-#####`) | FK→identity | No | `^CS-[0-9]{5}$` |
| `global_customer_ref` | STRING | Enterprise `CUST-######` when known — **~75%** | Business key | Yes | `^CUST-[0-9]{6}$` when present |
| `customer_email` | STRING | Contact email for the ticket raiser as recorded by the Customer Support System (identity-resolution match key — Amendment 1) | Match key | Yes | Represents the associated customer; ~85% populated; normalized `lower(trim())` before matching; missing ≠ rejection; conflicting match → quarantine |
| `channel` | STRING | `EMAIL`, `PHONE`, `CHAT`, `BRANCH`, `MOBILE_APP` | | No | Domain check |
| `category` | STRING | `BILLING`, `FRAUD_ALERT`, `ACCOUNT_ACCESS`, `TECHNICAL`, `LOAN`, `CARD` | | No | Domain check |
| `priority` | STRING | `LOW`, `MEDIUM`, `HIGH`, `CRITICAL` | | No | Domain check |
| `status` | STRING | `OPEN`, `IN_PROGRESS`, `RESOLVED`, `CLOSED` | | No | Domain check |
| `created_date` | STRING | Ticket created — mixed formats | | No | Parses; ≤ current date |
| `resolved_date` | STRING | Ticket resolved — mixed formats | | Yes | Parses; ≥ `created_date`; required when `status ∈ {RESOLVED, CLOSED}` |
| `csat_score` | INT | Post-resolution satisfaction | | Yes | 1–5; NULL allowed only when unresolved |

---

## Acceptance Criteria

1. Every source contract above compiles to a CSV generator configuration (Phase 3) with no undocumented columns.
2. All regex/domain/nullability rules in this document are implemented as Silver validation checks (Phase 5) and quarantine rejection reasons.
3. Defect injection rates are reproducible via fixed random seed; rerunning generation yields identical data.
4. `global_customer_ref` values drawn from the enterprise domain exist in `fraud_detection.silver.customers` (referential integrity for Phase 11).
5. No source file contains real PII, full card numbers, or full national identifiers.
