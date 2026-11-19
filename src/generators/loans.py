"""Loan Management System generator (Phase 3). Same key engineering as
accounts (see src/generators/accounts.py header)."""
import random
from datetime import date, timedelta

import pandas as pd

from src.config.generator_config import (
    ANCHOR_DATE, BASE_SEED, DEFECTS, GLOBAL_REF_CARRY, PERSON_COVERAGE,
    SOURCE_SEED_OFFSETS, TARGETS,
)
from src.utilities import defects as dfx

COLUMNS = [
    "loan_id", "ln_customer_ref", "global_customer_ref", "customer_email",
    "loan_type", "principal_amount", "outstanding_balance", "interest_rate",
    "term_months", "monthly_installment", "start_date", "end_date",
    "repayment_status",
]

TERMS = [6, 12, 24, 36, 48, 60, 84, 120, 180, 240, 360]


def _near_alter(row, rng):
    """Near-duplicate: exactly one domain-safe attribute altered."""
    row["interest_rate"] = round(
        min(25.0, float(row["interest_rate"]) + 0.25), 2)
    return row


def generate(persons, n_masters=None, coverage=None, carry=None, rng=None):
    n_masters = TARGETS["loans"] if n_masters is None else n_masters
    coverage = PERSON_COVERAGE["loans"] if coverage is None else coverage
    carry = GLOBAL_REF_CARRY["loans"] if carry is None else carry
    rng = rng or random.Random(BASE_SEED + SOURCE_SEED_OFFSETS["loans"])
    anchor = date.fromisoformat(ANCHOR_DATE)

    n_covered = min(round(len(persons) * coverage), n_masters)
    covered = sorted(rng.sample(range(len(persons)), n_covered))
    n_orphan = round(n_masters * DEFECTS["orphan"])
    n_linked = n_masters - n_orphan
    extra = n_linked - n_covered
    two = set(rng.sample(covered, extra)) if extra > 0 else set()

    ln_ref = {pi: f"LN-{rank + 1:05d}" for rank, pi in enumerate(covered)}

    def n_rows(pi):
        return 2 if pi in two else 1

    seeded_rows = sum(n_rows(pi) for pi in covered if persons[pi]["global_ref"])
    carry_p = min(1.0, carry * n_masters / seeded_rows) if seeded_rows else 0.0

    rows = []
    loan = 10_001

    def loan_row(p, rng, ref=None, email=None):
        start = p["registration_date"] + timedelta(days=rng.randint(30, 900))
        if start > anchor:
            start = anchor
        term = rng.choice(TERMS)
        rate = round(rng.uniform(3.0, 25.0), 2)
        principal = round(rng.uniform(50_000, 5_000_000), 2)
        r = rate / 100 / 12
        installment = round(principal * r / (1 - (1 + r) ** (-term)), 2)
        status = rng.choices(
            ["CURRENT", "LATE", "DEFAULT", "PAID_OFF"],
            weights=[0.70, 0.15, 0.05, 0.10])[0]
        end = start + timedelta(days=term * 30)
        if status == "PAID_OFF":
            outstanding = 0.0
            end_out = end
        else:
            months_elapsed = min(term, max(0, (anchor - start).days // 30))
            outstanding = round(
                principal * max(0.05, 1 - months_elapsed / term), 2)
            end_out = end if rng.random() < 0.40 else None
        return {
            "loan_id": f"LON-{loan:05d}",
            "ln_customer_ref": None,
            "global_customer_ref": ref,
            "customer_email": email,
            "loan_type": rng.choices(
                ["PERSONAL", "HOME", "AUTO", "EDUCATION", "BUSINESS"],
                weights=[0.40, 0.20, 0.20, 0.10, 0.10])[0],
            "principal_amount": principal,
            "outstanding_balance": outstanding,
            "interest_rate": rate,
            "term_months": term,
            "monthly_installment": installment,
            "start_date": dfx.format_date(rng, start),
            "end_date": dfx.format_date(rng, end_out) if end_out else None,
            "repayment_status": status,
        }

    for pi in covered:
        p = persons[pi]
        for _ in range(n_rows(pi)):
            ref = (p["global_ref"]
                   if p["global_ref"] and rng.random() < carry_p else None)
            email = (p["email"]
                     if ref is None or rng.random() < DEFECTS["email_present_with_ref"]
                     else None)
            if ref and email and rng.random() < DEFECTS["identity_conflict"]:
                other = persons[rng.randrange(len(persons))]
                guard = 0
                while other["email"] == p["email"] and guard < 20:
                    other = persons[rng.randrange(len(persons))]
                    guard += 1
                email = other["email"]
            row = loan_row(p, rng, ref, email)
            row["ln_customer_ref"] = ln_ref[pi]
            rows.append(row)
            loan += 1

    for j in range(n_orphan):
        p = persons[j % len(persons)]
        row = loan_row(p, rng, None,
                       f"unmatched{j}@orphan-pool.example"
                       if rng.random() < DEFECTS["orphan_email_present"] else None)
        row["ln_customer_ref"] = f"LN-{99_000 + j:05d}"
        rows.append(row)
        loan += 1

    df = pd.DataFrame(rows, columns=COLUMNS)
    ref_col = COLUMNS.index("global_customer_ref")
    allowed = [i for i in range(len(df)) if df.iat[i, ref_col] is not None]
    df = dfx.corrupt_emails_at(df, "customer_email", allowed, rng)
    return dfx.inject_duplicates(df, _near_alter, rng)
