"""Bronze ingestion configuration (Phase 4).

Single source of truth for the six bronze target tables, their source files,
source-system codes, and schema contracts. All contracts verified against the
ACTUAL batch_001 headers on 2026-11-20 (spec 04-bronze-ingestion.md section 2).

Bronze principle: raw-is-raw -- all business columns STRING in verified file
order; no business logic, no dedup, no drops (spec 02-data-model.md section 1).
"""

CATALOG = "customer_360"
SCHEMA = "bronze"
SCHEMA_FQN = f"{CATALOG}.{SCHEMA}"

BATCH_ROOT = "/Volumes/customer_360/default/raw_data"
MANIFEST_PATH = f"{BATCH_ROOT}/batch_001/_manifest.json"

# lineage metadata appended to every table (data-model convention: five cols)
METADATA_COLUMNS = [
    ("_source_system", "string"),
    ("_batch_id", "string"),
    ("_ingestion_timestamp", "timestamp"),
    ("_ingestion_date", "date"),
    ("_source_file", "string"),
]

# Verified column order per source file (2026-11-20 inspection of batch_001).
BUSINESS_COLUMNS = {
    "customers": [
        "cis_customer_id", "global_customer_ref", "first_name", "middle_name",
        "last_name", "date_of_birth", "gender", "email", "phone",
        "address_line1", "city", "country", "postal_code", "registration_date",
        "kyc_status", "customer_segment", "annual_income", "status",
    ],
    "accounts": [
        "account_id", "cb_customer_ref", "global_customer_ref",
        "customer_email", "account_number", "account_type", "currency",
        "balance", "status", "open_date", "last_activity_date", "branch_code",
    ],
    "cards": [
        "card_id", "cc_customer_ref", "global_customer_ref", "customer_email",
        "card_number_masked", "card_type", "credit_limit",
        "outstanding_balance", "card_status", "issue_date", "expiry_date",
    ],
    "loans": [
        "loan_id", "ln_customer_ref", "global_customer_ref", "customer_email",
        "loan_type", "principal_amount", "outstanding_balance", "interest_rate",
        "term_months", "monthly_installment", "start_date", "end_date",
        "repayment_status",
    ],
    "digital_logins": [
        "login_id", "db_customer_ref", "global_customer_ref",
        "customer_email", "login_timestamp", "channel", "device_id",
        "device_type", "os", "browser", "login_status",
        "session_duration_minutes",
    ],
    "support_tickets": [
        "ticket_id", "cs_customer_ref", "global_customer_ref",
        "customer_email", "channel", "category", "priority", "status",
        "created_date", "resolved_date", "csat_score",
    ],
}

# table -> (source file, source-system code)
SOURCES = {
    "customers": ("customers_raw.csv", "cis"),
    "accounts": ("accounts_raw.csv", "core_banking"),
    "cards": ("cards_raw.csv", "card_system"),
    "loans": ("loans_raw.csv", "loan_system"),
    "digital_logins": ("digital_logins_raw.csv", "digital_banking"),
    "support_tickets": ("support_cases_raw.csv", "support_system"),
}


def table_fqn(table):
    return f"{SCHEMA_FQN}.{table}"


def batch_dir(batch_id):
    return f"{BATCH_ROOT}/{batch_id}"


def full_schema(table):
    """Complete column list: business columns then metadata (names only)."""
    return list(BUSINESS_COLUMNS[table]) + [c for c, _ in METADATA_COLUMNS]
