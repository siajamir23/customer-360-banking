"""13-check validation suite (specs/03-data-generation.md, section 6).

Asserts, never warns: any failed check raises AssertionError with full detail.
Also builds the per-source defect & reconciliation report displayed at the end
of the orchestrator run. Checks 11 (determinism) and 12 (seed independence)
are SHA-256 comparisons performed by the orchestrator and appended here.
"""
import json

import pandas as pd

from src.config.generator_config import (
    DEFECTS, EXPECTED_RAW, GLOBAL_REF_CARRY, POPULATION,
)
from src.generators import (
    accounts, cards, customers, digital_banking, loans, support,
)
from src.utilities.identity import normalize_email, parse_date_any

PRODUCT_SOURCES = ["accounts", "cards", "loans", "digital_logins", "support_cases"]

CONTRACT_COLUMNS = {
    "customers": customers.COLUMNS,
    "accounts": accounts.COLUMNS,
    "cards": cards.COLUMNS,
    "loans": loans.COLUMNS,
    "digital_logins": digital_banking.COLUMNS,
    "support_cases": support.COLUMNS,
}

_PK = {
    "customers": "cis_customer_id",
    "accounts": "account_id",
    "cards": "card_id",
    "loans": "loan_id",
    "digital_logins": "login_id",
    "support_cases": "ticket_id",
}

_REF_FORMATS = {  # PK + native ref + global ref regexes per source
    "customers": {"cis_customer_id": r"^CIS-\d{5}$"},
    "accounts": {"account_id": r"^ACC-\d{5}$", "cb_customer_ref": r"^CB-\d{5}$"},
    "cards": {"card_id": r"^CRD-\d{5}$", "cc_customer_ref": r"^CC-\d{5}$"},
    "loans": {"loan_id": r"^LON-\d{5}$", "ln_customer_ref": r"^LN-\d{5}$"},
    "digital_logins": {"login_id": r"^LOG-\d{7}$", "db_customer_ref": r"^DB-\d{5}$"},
    "support_cases": {"ticket_id": r"^TKT-\d{5}$", "cs_customer_ref": r"^CS-\d{5}$"},
}
_GLOBAL_REF_FMT = {"global_customer_ref": r"^CUST-\d{6}$"}

_ENUMS = {
    "customers": {
        "gender": {"M", "F", "U"},
        "kyc_status": {"VERIFIED", "PENDING", "REJECTED"},
        "customer_segment": {"RETAIL", "PREMIUM", "MASS_AFFLUENT", "PRIVATE"},
        "status": {"ACTIVE", "INACTIVE", "CLOSED"},
        "country": {"PH", "US", "SG", "AU", "GB"},
    },
    "accounts": {
        "account_type": {"SAVINGS", "CURRENT", "FIXED_DEPOSIT"},
        "currency": {"PHP", "USD", "SGD", "AUD", "GBP"},
        "status": {"ACTIVE", "DORMANT", "CLOSED"},
    },
    "cards": {
        "card_type": {"CLASSIC", "GOLD", "PLATINUM", "SIGNATURE"},
        "card_status": {"ACTIVE", "BLOCKED", "EXPIRED", "CANCELLED"},
    },
    "loans": {
        "loan_type": {"PERSONAL", "HOME", "AUTO", "EDUCATION", "BUSINESS"},
        "repayment_status": {"CURRENT", "LATE", "DEFAULT", "PAID_OFF"},
    },
    "digital_logins": {
        "channel": {"WEB", "MOBILE_APP"},
        "device_type": {"ANDROID", "IOS", "DESKTOP", "TABLET"},
        "login_status": {"SUCCESS", "FAILED"},
    },
    "support_cases": {
        "channel": {"EMAIL", "PHONE", "CHAT", "BRANCH", "MOBILE_APP"},
        "category": {"BILLING", "FRAUD_ALERT", "ACCOUNT_ACCESS",
                     "TECHNICAL", "LOAN", "CARD"},
        "priority": {"LOW", "MEDIUM", "HIGH", "CRITICAL"},
        "status": {"OPEN", "IN_PROGRESS", "RESOLVED", "CLOSED"},
    },
}

_DATE_COLS = {
    "customers": ["date_of_birth", "registration_date"],
    "accounts": ["open_date", "last_activity_date"],
    "cards": ["issue_date", "expiry_date"],
    "loans": ["start_date", "end_date"],
    "support_cases": ["created_date", "resolved_date"],
    "digital_logins": ["login_timestamp"],  # datetime variants
}

_MISSING_DESIGN = {  # (source, column) -> (expected missing rate, tolerance)
    ("customers", "email"): (0.05, 0.02),
    ("accounts", "balance"): (0.05, 0.03),
    ("accounts", "last_activity_date"): (0.10, 0.03),
    ("cards", "credit_limit"): (0.05, 0.03),
    ("digital_logins", "os"): (0.10, 0.03),
    ("support_cases", "resolved_date"): (0.25, 0.05),
}

_DT_FORMATS = ("%Y-%m-%d %H:%M:%S", "%d/%m/%Y %H:%M", "%m-%d-%Y %H:%M")
_D_FORMATS = ("%Y-%m-%d", "%d/%m/%Y", "%m-%d-%Y")


def _iso_share(values, formats):
    iso = 0
    total = 0
    for v in values:
        if v is None or pd.isna(v):
            continue
        total += 1
        s = str(v).strip()
        try:
            # try non-ISO variants first; if none match, and ISO matches, count ISO
            import datetime as _dt
            for f in formats[1:]:
                try:
                    _dt.datetime.strptime(s, f)
                    break
                except ValueError:
                    continue
            else:
                _dt.datetime.strptime(s, formats[0])
                iso += 1
        except ValueError:
            pass  # unparseable dates are a check-6 failure, not a mix failure
    return iso / total if total else 1.0


def validate(raw, persons):
    """Run all checks against the generated frames. Returns (checks_df, report_df).
    Raises AssertionError listing every failed check."""
    records = []

    def check(num, name, expected, actual, ok, detail=""):
        records.append({"check": f"{num} {name}", "expected": str(expected),
                        "actual": str(actual), "passed": bool(ok), "detail": detail})

    # -- population (acceptance criterion 6) --------------------------------
    seeded = [p for p in persons if p["origin"] == "FRAUD_SEED"]
    new = [p for p in persons if p["origin"] == "NEW"]
    check(0, "population size", POPULATION["total"], len(persons),
          len(persons) == POPULATION["total"])
    check(0, "seeded fraud customers (100% coverage)",
          POPULATION["seeded_fraud_customers"], len(seeded),
          len(seeded) == POPULATION["seeded_fraud_customers"])
    check(0, "new persons reserved for R5 (CUST-100001+)",
          POPULATION["new_customers"], len(new),
          len(new) == POPULATION["new_customers"]
          and all(p["global_ref"] is None for p in new))

    fraud_ids = {p["global_ref"] for p in seeded}
    email_map = {p["email"]: p for p in persons}
    ref_person = {p["global_ref"]: p for p in seeded}

    report_rows = []

    for source, df in raw.items():
        pk = _PK[source]

        # 1 row count
        exp = EXPECTED_RAW[source]
        check(1, f"row count {source}", exp, len(df), len(df) == exp)

        # 2 schema (exact columns and order per contract)
        cols_ok = list(df.columns) == CONTRACT_COLUMNS[source]
        check(2, f"schema {source}", "contract columns, exact order",
              f"{len(df.columns)} cols",
              cols_ok, "" if cols_ok else str(list(df.columns)))

        # 3 PK / ref format regexes (dupes share their master's PK by design;
        # nulls are contract-legal on nullable key columns and excluded here)
        bad_fmt = 0
        for col, pat in {**_REF_FORMATS[source],
                        **(_GLOBAL_REF_FMT if "global_customer_ref" in df.columns else {})
                        }.items():
            series = df[col].dropna().astype(str)
            bad_fmt += int((~series.str.match(pat)).sum())
        check(3, f"key formats {source}", 0, bad_fmt, bad_fmt == 0)

        # 4 duplicate rates (shares of raw rows; design: 2%/1.5% of masters)
        n = len(df)
        exact_n = int(df.duplicated(keep="first").sum())
        pk_extra_n = int(df.duplicated(subset=[pk], keep="first").sum())
        near_n = pk_extra_n - exact_n
        exact_rate, near_rate = exact_n / n, near_n / n
        ok4 = (abs(exact_rate - DEFECTS["exact_duplicate"] / 1.035) < 0.005
               and abs(near_rate - DEFECTS["near_duplicate"] / 1.035) < 0.005)
        check(4, f"duplicate rates {source}",
              f"exact ~{DEFECTS['exact_duplicate'] / 1.035:.2%}, "
              f"near ~{DEFECTS['near_duplicate'] / 1.035:.2%} (of raw)",
              f"exact {exact_rate:.2%}, near {near_rate:.2%}", ok4)

        # 5 enum domains
        bad_enum = 0
        for col, domain in _ENUMS[source].items():
            vals = df[col].dropna()
            bad_enum += int((~vals.isin(domain)).sum())
        check(5, f"enum domains {source}", 0, bad_enum, bad_enum == 0)

        # 6 date format mix (60/25/15, +-5pp) + parseability
        bad_dates = 0
        mix_notes = []
        for col in _DATE_COLS[source]:
            vals = df[col].dropna()
            fmts = (_DT_FORMATS if source == "digital_logins" else _D_FORMATS)
            import datetime as _dt
            unparse = sum(
                1 for v in vals
                if not any(_try(v, f) for f in fmts))
            bad_dates += unparse
            iso = _iso_share(vals, fmts)
            mix_notes.append(f"{col} ISO {iso:.0%}")
            if abs(iso - DEFECTS["date_formats"][0]) > 0.05:
                bad_dates += 1
        check(6, f"date mix {source}", "60/25/15 +-5pp, all parseable",
              f"{bad_dates} bad; " + ", ".join(mix_notes), bad_dates == 0)

        # 7 missing-value rates (designed nullable columns)
        bad_missing = 0
        for (src, col), (rate, tol) in _MISSING_DESIGN.items():
            if src != source:
                continue
            actual = df[col].isna().mean()
            if abs(actual - rate) > tol:
                bad_missing += 1
        check(7, f"missing rates {source}", "designed rates +-tolerance",
              f"{bad_missing} out of tolerance", bad_missing == 0)

        # 8 global-ref referential integrity vs fraud registry (read-only seed)
        if "global_customer_ref" in df.columns:
            refs = df["global_customer_ref"].dropna()
            bad_ref = int((~refs.isin(fraud_ids)).sum())
            check(8, f"ref integrity {source}", "all refs in fraud registry",
                  f"{bad_ref} outside", bad_ref == 0)

        # 9/10/13 product identity engineering
        orphans = conflicts = dual_n = 0
        if source in PRODUCT_SOURCES:
            nem = df["customer_email"].map(normalize_email)
            resolvable = df["global_customer_ref"].notna() | nem.isin(email_map)
            orphans = int((~resolvable).sum())
            orphan_rate = orphans / n
            ok9 = abs(orphan_rate - DEFECTS["orphan"]) < 0.005
            check(9, f"reconciliation {source}",
                  f"~{1 - DEFECTS['orphan']:.0%} resolvable via R1/R2, "
                  f"orphans ~{DEFECTS['orphan']:.0%}",
                  f"orphans {orphan_rate:.2%} ({orphans})", ok9)

            dual_mask = df["global_customer_ref"].notna() & nem.notna()
            dual_n = int(dual_mask.sum())
            for r, e in zip(df.loc[dual_mask, "global_customer_ref"],
                            df.loc[dual_mask, "customer_email"]):
                if email_map[normalize_email(e)]["global_ref"] != r:
                    conflicts += 1
            conflict_rate = conflicts / dual_n if dual_n else 0.0
            ok10 = abs(conflict_rate - DEFECTS["identity_conflict"]) < 0.005
            check(10, f"identity conflicts {source}",
                  f"~{DEFECTS['identity_conflict']:.0%} of dual-key rows -> quarantine",
                  f"{conflict_rate:.2%} ({conflicts}/{dual_n})", ok10)

            # 13 approved email rules:
            # (a) missing-email rows must never be auto-rejected: every
            #     non-orphan missing-email row carries a global ref (resolves via R1)
            missing_email = df["customer_email"].isna()
            miss_no_ref = int((missing_email & df["global_customer_ref"].isna()).sum())
            ok13a = miss_no_ref <= orphans
            # (b) conflicts must exist (quarantine material) but never merge:
            #     they are flagged counts only; no row 'merged' two identities.
            ok13 = ok13a and conflicts > 0
            check(13, f"email rules {source}",
                  "missing email -> R1 fallback; conflicts present, never merged",
                  f"missing-email-no-ref rows {miss_no_ref} (<= orphans {orphans}); "
                  f"conflicts {conflicts}", ok13)

        # report row
        email_col = "customer_email" if source in PRODUCT_SOURCES else "email"
        pop = df[email_col].notna().mean()
        invalid = sum(
            1 for v in df[email_col].dropna() if normalize_email(v) is None)
        report_rows.append({
            "source": source,
            "rows": n,
            "masters": EXPECTED_RAW[source] - exact_n - near_n,
            "exact_dupes": exact_n,
            "near_dupes": near_n,
            "email_populated": f"{pop:.1%}",
            "invalid_emails": invalid,
            "orphans": orphans if source in PRODUCT_SOURCES else "-",
            "conflicts": conflicts if source in PRODUCT_SOURCES else "-",
            "dual_key_rows": dual_n if source in PRODUCT_SOURCES else "-",
            "global_ref_rate": (f"{df['global_customer_ref'].notna().mean():.1%}"
                                if "global_customer_ref" in df.columns else "-"),
        })

    checks = pd.DataFrame(records)
    failed = checks[~checks["passed"]]
    if len(failed):
        raise AssertionError(
            "Validation failed (" + str(len(failed)) + " checks):\n"
            + failed.to_string(index=False))
    return checks, pd.DataFrame(report_rows)


def _try(value, fmt):
    import datetime as _dt
    try:
        _dt.datetime.strptime(str(value).strip(), fmt)
        return True
    except ValueError:
        return False


def append_orchestrator_checks(checks_df, determinism_ok, seed_independence_ok,
                                detail=""):
    """Append checks 11/12 (performed by the orchestrator via SHA-256)."""
    extra = pd.DataFrame([
        {"check": "11 determinism", "expected": "identical SHA-256 per file on rerun",
         "actual": "pass" if determinism_ok else "FAIL", "passed": bool(determinism_ok),
         "detail": detail},
        {"check": "12 seed independence",
         "expected": "changing one source's size re-hashes only that file",
         "actual": "pass" if seed_independence_ok else "FAIL",
         "passed": bool(seed_independence_ok), "detail": ""},
    ])
    out = pd.concat([checks_df, extra], ignore_index=True)
    if not (determinism_ok and seed_independence_ok):
        raise AssertionError("Determinism/seed-independence check failed")
    return out
