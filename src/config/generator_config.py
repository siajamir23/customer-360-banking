"""Configuration for synthetic multi-source data generation (Phase 3).

Single source of truth for seeds, sizes, defect rates, and paths.
Approved spec: specs/03-data-generation.md (Approved 2026-11-19 with adjustments).

Row-count contract (documented, review 2026-11-19):
    raw rows = masters x (1 + exact_duplicate + near_duplicate) = masters x 1.035
The only records appended beyond masters are the two contracted duplicate
injection types; no other record types exist.
"""

BASE_SEED = 360
BATCH_ID = "batch_001"

# Unity Catalog objects Phase 3 is approved to create (idempotently, and ONLY these)
CATALOG = "customer_360"
SCHEMA = "default"
VOLUME = "raw_data"
VOLUME_ROOT = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}"
BATCH_DIR = f"{VOLUME_ROOT}/{BATCH_ID}"

# Read-only seed input (never written to)
FRAUD_CUSTOMERS_TABLE = "fraud_detection.silver.customers"

# Fixed event window: no now() anywhere, so reruns are byte-identical.
EVENT_WINDOW = ("2025-12-01", "2026-11-30")
ANCHOR_DATE = "2026-11-30"

# Approved shared population: ALL 10,000 existing fraud customers + 2,000 new.
# New persons carry NO global ref; R5 assigns CUST-100001+ in Phase 6 only.
POPULATION = {
    "seeded_fraud_customers": 10_000,
    "new_customers": 2_000,
    "total": 12_000,
}

# Master row targets per source (before duplicate injection)
TARGETS = {
    "customers": 12_000,
    "accounts": 14_000,
    "cards": 6_600,
    "loans": 2_800,
    "digital_logins": 55_000,
    "support_cases": 9_000,
}

# Share of the 12,000 persons covered by each product source
PERSON_COVERAGE = {
    "accounts": 0.80,        # ~9,600 persons, 1-2 accounts each -> 14,000 masters
    "cards": 0.45,            # ~5,400 persons -> 6,600
    "loans": 0.22,            # ~2,640 persons -> 2,800
    "digital_logins": 0.75,  # ~9,000 persons, ~6 events each -> 55,000
    "support_cases": 0.55,   # ~6,600 persons -> 9,000
}

# Share of product rows carrying a valid global_customer_ref (contract)
GLOBAL_REF_CARRY = {
    "accounts": 0.75,
    "cards": 0.80,
    "loans": 0.70,
    "digital_logins": 0.85,
    "support_cases": 0.75,
}

DEFECTS = {
    "exact_duplicate": 0.02,             # of masters, appended as full-row copies
    "near_duplicate": 0.015,             # of masters, one attribute altered
    "invalid_email": 0.05,                # of populated emails
    "identity_conflict": 0.03,            # of dual-key rows (ref + valid email)
    "orphan": 0.02,                       # of rows resolvable via neither R1 nor R2
    "date_formats": [0.60, 0.25, 0.15],   # ISO / DD-MM-YYYY / MM-DD-YYYY
    # Email-presence engineering (keeps unresolvable rate == orphan target):
    # rows WITH a global ref keep email present at 80% (resolvable via R1 either
    # way); rows WITHOUT a ref always keep a valid, matching email (R2), so the
    # only unresolvable rows are the engineered 2% orphans (missing email is
    # never the cause -- approved rule 3).
    "email_present_with_ref": 0.80,
    "orphan_email_present": 0.50,        # orphan rows: email present but matches nobody
}

COUNTRY_CURRENCY = {"PH": "PHP", "US": "USD", "SG": "SGD", "AU": "AUD", "GB": "GBP"}
COUNTRIES = list(COUNTRY_CURRENCY)

# Per-generator seed offsets: changing one source's config re-randomizes only
# that source (validation check 12, seed independence).
SOURCE_SEED_OFFSETS = {
    "population": 0,
    "customers": 1,
    "accounts": 2,
    "cards": 3,
    "loans": 4,
    "digital_logins": 5,
    "support_cases": 6,
}

SOURCE_FILES = {
    "customers": "customers_raw.csv",
    "accounts": "accounts_raw.csv",
    "cards": "cards_raw.csv",
    "loans": "loans_raw.csv",
    "digital_logins": "digital_logins_raw.csv",
    "support_cases": "support_cases_raw.csv",
}


def raw_rows(masters: int) -> int:
    """masters x (1 + exact_dup + near_dup) = masters x 1.035 (documented formula)."""
    return (masters
            + round(masters * DEFECTS["exact_duplicate"])
            + round(masters * DEFECTS["near_duplicate"]))


EXPECTED_RAW = {name: raw_rows(m) for name, m in TARGETS.items()}
# customers 12,420 / accounts 14,490 / cards 6,831 / loans 2,898
# digital_logins 56,925 / support_cases 9,315
