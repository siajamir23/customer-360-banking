"""Phase 5 Silver configuration (spec specs/05-silver-standardization.md, commit bfb1313c).

Single source of truth for the Silver layer: typed schemas (spec section 3,
contract-aligned logical types), the approved date/timestamp patterns,
normalization rules (incl. Amendment 2 phone), the full rejection-rule
registry (spec section 7), the Amendment 1 dedup precedence, quarantine/audit
targets, and the verified batch_001 expectations (live-inspected 2026-11-20/21).

Hard boundaries (spec section 'Non-goals'): no identity resolution, no
unified_customer_id, no Gold, Bronze immutable, fraud_detection untouched.
"""

from src.config.bronze_config import BUSINESS_COLUMNS as BRONZE_BUSINESS_COLUMNS

CATALOG = "customer_360"
SILVER_SCHEMA = "silver"
QUARANTINE_SCHEMA = "quarantine"
SILVER_FQN = f"{CATALOG}.{SILVER_SCHEMA}"
QUARANTINE_FQN = f"{CATALOG}.{QUARANTINE_SCHEMA}"

BRONZE_FQN = f"{CATALOG}.bronze"

# ---- lineage (spec: five Bronze columns verbatim + silver marker, D8) ----
LINEAGE_COLUMNS = [
    "_source_system", "_batch_id", "_ingestion_timestamp",
    "_ingestion_date", "_source_file",
]
SILVER_TS_COL = "_silver_processed_timestamp"

# ---- approved parser patterns (Amendment 4; [0-9] classes avoid SQL-string
# ---- escape mangling; patterns verified separator-disjoint on live data)
DATE_PATTERNS = ["yyyy-MM-dd", "dd/MM/yyyy", "MM-dd-yyyy"]
DATE_REGEXES = [
    r"^[0-9]{4}-[0-9]{2}-[0-9]{2}$",
    r"^[0-9]{2}/[0-9]{2}/[0-9]{4}$",
    r"^[0-9]{2}-[0-9]{2}-[0-9]{4}$",
]
TIMESTAMP_PATTERNS = ["yyyy-MM-dd HH:mm:ss", "dd/MM/yyyy HH:mm", "MM-dd-yyyy HH:mm"]
TIMESTAMP_REGEXES = [
    r"^[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}$",
    r"^[0-9]{2}/[0-9]{2}/[0-9]{4} [0-9]{2}:[0-9]{2}$",
    r"^[0-9]{2}-[0-9]{2}-[0-9]{4} [0-9]{2}:[0-9]{2}$",
]

# ---- normalization (spec section 5)
GREF_REGEX = r"^CUST-[0-9]{6}$"          # else NULL + audit (D6)
EMAIL_REGEX = r"^[^@ ]+@[^@ ]+\.[^@ ]+$"  # else NULL + audit (D6)
PHONE_KEEP_DIGITS = 10                    # Amendment 2: last 10; fewer -> NULL
NAME_LETTERS_REGEX = r"^[A-Za-z.'\- ]+$"  # contract: letters (incl. punctuation) after trim

# ---- quarantine / audit targets (spec section 9)
INVALID_TABLE_PREFIX = "invalid_"
DUPLICATE_TABLE = f"{QUARANTINE_FQN}.duplicate_records"
AUDIT_TABLE = f"{SILVER_FQN}.processing_audit"
# Review fix 3: processing_audit identifies its batch by `batch_id` (NOT the
# `_batch_id` lineage column). The idempotency guard and the manual recovery
# instructions must use the correct identifier column per target.
AUDIT_BATCH_COLUMN = "batch_id"

# ---- Amendment 1 dedup precedence (NO recency criterion):
# ---- 1. fewest NULL business attributes; 2. lowest SHA-256 over
# ---- canonicalized business columns; 3. stable lexicographic tie-break.
DEDUP_PRECEDENCE = ["fewest_nulls", "lowest_canonical_hash", "lexicographic_tiebreak"]

# ---- rejection-rule registry (spec section 7). Order matters only for
# ---- readability; every rule maps 1:1 to an approved contract validation.
RULE_IDS = [
    "PK_NULL", "PK_INVALID_FORMAT", "NATIVE_REF_INVALID_FORMAT",
    "REQUIRED_FIELD_NULL", "INVALID_DATE_FORMAT", "INVALID_TIMESTAMP_FORMAT",
    "NUMERIC_UNPARSEABLE", "ENUM_VIOLATION", "NAME_INVALID", "POSTAL_INVALID",
    "ACCOUNT_NUMBER_INVALID", "BRANCH_CODE_INVALID", "MASKED_PAN_INVALID",
    "INCOME_OUT_OF_RANGE", "RATE_OUT_OF_RANGE", "TERM_OUT_OF_RANGE",
    "AMOUNT_INVALID", "DOB_AFTER_REGISTRATION", "LAST_ACTIVITY_BEFORE_OPEN",
    "EXPIRY_NOT_AFTER_ISSUE", "END_BEFORE_START", "END_DATE_REQUIRED",
    "RESOLVED_BEFORE_CREATED", "RESOLVED_DATE_REQUIRED", "CSAT_INVALID",
    "SESSION_DURATION_INVALID",
]
# LOAN_ANOMALY_OUTSTANDING_GT_PRINCIPAL is an audit flag, NOT a rejection.
LOAN_ANOMALY_FLAG = "loan_anomalies_flagged"

# ---- Integer cast policy (review fix 5 + Decision 1, approved 2026-11-24):
# ---- csat_score (A5) and session_duration_minutes (A8) are stored in
# ---- Bronze as integral DECIMAL NOTATION ('4.0', '150.0') and cast via
# ---- DECIMAL with integral-values-only semantics. Every other int column
# ---- (term_months) is cast strictly: integer notation only.
INT_CAST_VIA_DECIMAL = {"csat_score", "session_duration_minutes"}

# ---- Review fix 6: derived Silver columns EXCLUDED from the dedup
# ---- hash/completeness inputs. The approved input is the set of
# ---- canonicalized SOURCE business columns (name_key is derived from
# ---- first/last name; including it would double-count those attributes).
DERIVED_SILVER_COLUMNS = {"customers": ["name_key"]}

# ---- typed Silver schemas (spec section 3; business columns only).
# ---- Order = final Silver column order, before the lineage columns.
TYPED_COLUMNS = {
    "customers": [
        ("cis_customer_id", "string"), ("global_customer_ref", "string"),
        ("first_name", "string"), ("middle_name", "string"), ("last_name", "string"),
        ("name_key", "string"), ("date_of_birth", "date"), ("gender", "string"),
        ("email", "string"), ("phone", "string"), ("address_line1", "string"),
        ("city", "string"), ("country", "string"), ("postal_code", "string"),
        ("registration_date", "date"), ("kyc_status", "string"),
        ("customer_segment", "string"), ("annual_income", "decimal(12,2)"),
        ("status", "string"),
    ],
    "accounts": [
        ("account_id", "string"), ("cb_customer_ref", "string"),
        ("global_customer_ref", "string"), ("customer_email", "string"),
        ("account_number", "string"), ("account_type", "string"),
        ("currency", "string"), ("balance", "decimal(15,2)"), ("status", "string"),
        ("open_date", "date"), ("last_activity_date", "date"),
        ("branch_code", "string"),
    ],
    "cards": [
        ("card_id", "string"), ("cc_customer_ref", "string"),
        ("global_customer_ref", "string"), ("customer_email", "string"),
        ("card_number_masked", "string"), ("card_type", "string"),
        ("credit_limit", "decimal(12,2)"), ("outstanding_balance", "decimal(12,2)"),
        ("card_status", "string"), ("issue_date", "date"), ("expiry_date", "date"),
    ],
    "loans": [
        ("loan_id", "string"), ("ln_customer_ref", "string"),
        ("global_customer_ref", "string"), ("customer_email", "string"),
        ("loan_type", "string"), ("principal_amount", "decimal(14,2)"),
        ("outstanding_balance", "decimal(14,2)"), ("interest_rate", "decimal(5,2)"),
        ("term_months", "int"), ("monthly_installment", "decimal(12,2)"),
        ("start_date", "date"), ("end_date", "date"), ("repayment_status", "string"),
    ],
    "digital_logins": [
        ("login_id", "string"), ("db_customer_ref", "string"),
        ("global_customer_ref", "string"), ("customer_email", "string"),
        ("login_timestamp", "timestamp"), ("channel", "string"),
        ("device_id", "string"), ("device_type", "string"), ("os", "string"),
        ("browser", "string"), ("login_status", "string"),
        ("session_duration_minutes", "int"),
    ],
    "support_tickets": [
        ("ticket_id", "string"), ("cs_customer_ref", "string"),
        ("global_customer_ref", "string"), ("customer_email", "string"),
        ("channel", "string"), ("category", "string"), ("priority", "string"),
        ("status", "string"), ("created_date", "date"), ("resolved_date", "date"),
        ("csat_score", "int"),
    ],
}

# ---- per-entity validation config (all keys are lists/dicts consumed by
# ---- src/silver/transform.py; every entry traces to a contract row)
ENTITIES = {
    "customers": {
        "pk": "cis_customer_id", "pk_regex": r"^CIS-[0-9]{5}$",
        "native_refs": {},  # the PK is the native CIS ref; malformed -> PK_INVALID_FORMAT
        "email_col": "email", "phone_col": "phone",
        "required": ["first_name", "last_name", "date_of_birth", "country",
                    "registration_date", "kyc_status", "status"],
        "enums": {
            "gender": ["M", "F", "U"],
            "country": ["PH", "US", "SG", "AU", "GB"],
            "kyc_status": ["VERIFIED", "PENDING", "REJECTED"],
            "customer_segment": ["RETAIL", "PREMIUM", "PRIVATE", "MASS_AFFLUENT"],
            "status": ["ACTIVE", "INACTIVE", "CLOSED"],
        },
        "dates": {"date_of_birth": True, "registration_date": True},
        "timestamps": {},
        "numerics": {
            "annual_income": {"type": "decimal(12,2)", "min": 0, "max": 10000000,
                               "rule": "INCOME_OUT_OF_RANGE"},
        },
        "name_cols": ["first_name", "last_name"],
        "postal_col": "postal_code",
        "formats": {},
        "special_rules": ["DOB_AFTER_REGISTRATION"],
    },
    "accounts": {
        "pk": "account_id", "pk_regex": r"^ACC-[0-9]{5}$",
        "native_refs": {"cb_customer_ref": r"^CB-[0-9]{5}$"},
        "email_col": "customer_email", "phone_col": None,
        "required": ["cb_customer_ref", "account_number", "account_type",
                      "currency", "status", "open_date"],
        "enums": {
            "account_type": ["SAVINGS", "CURRENT", "FIXED_DEPOSIT"],
            "currency": ["PHP", "USD", "SGD", "AUD", "GBP"],
            "status": ["ACTIVE", "DORMANT", "CLOSED"],
        },
        "dates": {"open_date": True, "last_activity_date": False},
        "timestamps": {},
        "numerics": {"balance": {"type": "decimal(15,2)"}},  # no range: overdrafts valid
        "name_cols": [], "postal_col": None,
        "formats": {
            "account_number": {"regex": r"^[0-9]{10}$", "rule": "ACCOUNT_NUMBER_INVALID",
                                "nullable": False},
            "branch_code": {"regex": r"^BR-[0-9]{3}$", "rule": "BRANCH_CODE_INVALID",
                            "nullable": True},
        },
        "special_rules": ["LAST_ACTIVITY_BEFORE_OPEN"],
    },
    "cards": {
        "pk": "card_id", "pk_regex": r"^CRD-[0-9]{5}$",
        "native_refs": {"cc_customer_ref": r"^CC-[0-9]{5}$"},
        "email_col": "customer_email", "phone_col": None,
        "required": ["cc_customer_ref", "card_number_masked", "card_type",
                     "card_status", "issue_date", "expiry_date"],
        "enums": {
            "card_type": ["CLASSIC", "GOLD", "PLATINUM", "SIGNATURE"],
            "card_status": ["ACTIVE", "BLOCKED", "EXPIRED", "CANCELLED"],
        },
        "dates": {"issue_date": True, "expiry_date": True},
        "timestamps": {},
        "numerics": {
            "credit_limit": {"type": "decimal(12,2)", "min": 0, "exclusive_min": True,
                             "rule": "AMOUNT_INVALID"},
            "outstanding_balance": {"type": "decimal(12,2)", "min": 0,
                                     "rule": "AMOUNT_INVALID"},
        },
        "name_cols": [], "postal_col": None,
        "formats": {
            "card_number_masked": {"regex": r"^[*]{4}-[*]{4}-[*]{4}-[0-9]{4}$",
                                   "rule": "MASKED_PAN_INVALID", "nullable": False},
        },
        "special_rules": ["EXPIRY_NOT_AFTER_ISSUE"],
    },
    "loans": {
        "pk": "loan_id", "pk_regex": r"^LON-[0-9]{5}$",
        "native_refs": {"ln_customer_ref": r"^LN-[0-9]{5}$"},
        "email_col": "customer_email", "phone_col": None,
        "required": ["ln_customer_ref", "loan_type", "principal_amount",
                     "outstanding_balance", "interest_rate", "term_months",
                     "monthly_installment", "start_date", "repayment_status"],
        "enums": {
            "loan_type": ["PERSONAL", "HOME", "AUTO", "EDUCATION", "BUSINESS"],
            "repayment_status": ["CURRENT", "LATE", "DEFAULT", "PAID_OFF"],
        },
        "dates": {"start_date": True, "end_date": False},
        "timestamps": {},
        "numerics": {
            "principal_amount": {"type": "decimal(14,2)", "min": 0,
                                  "exclusive_min": True, "rule": "AMOUNT_INVALID"},
            "outstanding_balance": {"type": "decimal(14,2)", "min": 0,
                                     "rule": "AMOUNT_INVALID"},
            "interest_rate": {"type": "decimal(5,2)", "min": 3, "max": 25,
                              "rule": "RATE_OUT_OF_RANGE"},
            "term_months": {"type": "int", "min": 6, "max": 360,
                            "rule": "TERM_OUT_OF_RANGE"},
            "monthly_installment": {"type": "decimal(12,2)", "min": 0,
                                     "exclusive_min": True, "rule": "AMOUNT_INVALID"},
        },
        "name_cols": [], "postal_col": None,
        "formats": {},
        "special_rules": ["END_BEFORE_START", "END_DATE_REQUIRED"],
    },
    "digital_logins": {
        "pk": "login_id", "pk_regex": r"^LOG-[0-9]{7}$",
        "native_refs": {"db_customer_ref": r"^DB-[0-9]{5}$"},
        "email_col": "customer_email", "phone_col": None,
        "required": ["db_customer_ref", "login_timestamp", "channel", "device_id",
                     "device_type", "login_status"],
        "enums": {
            "channel": ["WEB", "MOBILE_APP"],
            "device_type": ["ANDROID", "IOS", "DESKTOP", "TABLET"],
            "login_status": ["SUCCESS", "FAILED"],
        },
        "dates": {},
        "timestamps": {"login_timestamp": True},
        "numerics": {},  # session handled by SESSION_DURATION_INVALID
        "name_cols": [], "postal_col": None,
        "formats": {},
        "special_rules": ["SESSION_DURATION_INVALID"],
    },
    "support_tickets": {
        "pk": "ticket_id", "pk_regex": r"^TKT-[0-9]{5}$",
        "native_refs": {"cs_customer_ref": r"^CS-[0-9]{5}$"},
        "email_col": "customer_email", "phone_col": None,
        "required": ["cs_customer_ref", "channel", "category", "priority",
                     "status", "created_date"],
        "enums": {
            "channel": ["EMAIL", "PHONE", "CHAT", "BRANCH", "MOBILE_APP"],
            "category": ["BILLING", "FRAUD_ALERT", "ACCOUNT_ACCESS", "TECHNICAL",
                          "LOAN", "CARD"],
            "priority": ["LOW", "MEDIUM", "HIGH", "CRITICAL"],
            "status": ["OPEN", "IN_PROGRESS", "RESOLVED", "CLOSED"],
        },
        "dates": {"created_date": True, "resolved_date": False},
        "timestamps": {},
        "numerics": {},  # csat handled by CSAT_INVALID
        "name_cols": [], "postal_col": None,
        "formats": {},
        "special_rules": ["RESOLVED_BEFORE_CREATED", "RESOLVED_DATE_REQUIRED",
                          "CSAT_INVALID"],
    },
}

ENTITY_ORDER = ["customers", "accounts", "cards", "loans",
                "digital_logins", "support_tickets"]


def bronze_fqn(entity):
    return f"{BRONZE_FQN}.{entity}"


def silver_fqn(entity):
    return f"{SILVER_FQN}.{entity}"


def invalid_fqn(entity):
    return f"{QUARANTINE_FQN}.{INVALID_TABLE_PREFIX}{entity}"


def bronze_business_columns(entity):
    return list(BRONZE_BUSINESS_COLUMNS[entity])


def typed_schema_map(entity):
    return {name: typ for name, typ in TYPED_COLUMNS[entity]}


def business_columns(entity):
    """Business column names (raw bronze order == typed silver order, minus
    derived name_key; name_key is produced by transformation)."""
    return list(BRONZE_BUSINESS_COLUMNS[entity])


# ---- verified batch_001 expectations (live inspection 2026-11-20/21).
# ---- invalid_emails corrected 2026-11-24 (Amendment 9, Option A): counts
# ---- of emails invalid AFTER normalization -- lower(trim()) applied before
# ---- the regex, per spec 5.3 -- verified in the production run. The prior
# ---- counts (611/396/215/88/1880/289) included whitespace-wrapped emails
# ---- that become valid once trimmed and are preserved, not nulled.
# ---- Reconciliation identities (Amendment 3) must match these exactly:
# ----   bronze_rows = silver_rows + duplicate_rows + rejected_distinct
# ---- where duplicate_rows = exact + near.
EXPECTED_BATCH_001 = {
    "customers": {"bronze_rows": 12420, "silver_rows": 12000,
                  "exact_dup": 244, "near_dup": 176, "rejected_distinct": 0,
                  "invalid_emails": 410},
    "accounts": {"bronze_rows": 14490, "silver_rows": 14000,
                 "exact_dup": 284, "near_dup": 206, "rejected_distinct": 0,
                 "invalid_emails": 277},
    "cards": {"bronze_rows": 6831, "silver_rows": 6600,
              "exact_dup": 132, "near_dup": 99, "rejected_distinct": 0,
              "invalid_emails": 142},
    "loans": {"bronze_rows": 2898, "silver_rows": 2800,
              "exact_dup": 56, "near_dup": 42, "rejected_distinct": 0,
              "invalid_emails": 52},
    "digital_logins": {"bronze_rows": 56925, "silver_rows": 55000,
                       "exact_dup": 1248, "near_dup": 677, "rejected_distinct": 0,
                       "invalid_emails": 1225},
    "support_tickets": {"bronze_rows": 9315, "silver_rows": 9000,
                         "exact_dup": 203, "near_dup": 112, "rejected_distinct": 0,
                         "invalid_emails": 210},
}
# Verified zero-defect counters for batch_001 (spec section 7):
EXPECTED_BATCH_001_ZEROS = {
    "invalid_refs_nulled": 0, "invalid_phones_nulled": 0,
    "loan_anomalies_flagged": 0,
}
