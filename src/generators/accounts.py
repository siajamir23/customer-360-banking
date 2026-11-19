"""Core Banking deposit-accounts generator (Phase 3).

Shared key engineering (all product generators follow this pattern):
- global_customer_ref carried on ~75% of rows (only seeded persons can carry one;
  per-row probability is scaled so the OVERALL rate hits the contract target);
- customer_email: rows with a ref keep it at 80%, rows without a ref always keep
  a valid matching email -> unresolvable rows == the engineered 2% orphans;
- identity conflicts: ~3% of dual-key rows carry another person's email
  (quarantine material for Phase 6 -- never merged);
- orphans: 2% of rows with fabricated native refs (CB-99xxx) resolving to nobody.
"""
import random
from datetime import date, timedelta

import pandas as pd

from src.config.generator_config import (
    ANCHOR_DATE, BASE_SEED, COUNTRY_CURRENCY, DEFECTS,
    GLOBAL_REF_CARRY, PERSON_COVERAGE, SOURCE_SEED_OFFSETS, TARGETS,
)
from src.utilities import defects as dfx

COLUMNS = [
    "account_id", "cb_customer_ref", "global_customer_ref", "customer_email",
    "account_number", "account_type", "currency", "balance", "status",
    "open_date", "last_activity_date", "branch_code",
]


def _near_alter(row, rng):
    """Near-duplicate: exactly one domain-safe attribute altered."""
    row["branch_code"] = f"BR-{rng.randint(1, 120):03d}"
    return row


def generate(persons, n_masters=None, coverage=None, carry=None, rng=None):
    n_masters = TARGETS["accounts"] if n_masters is None else n_masters
    coverage = PERSON_COVERAGE["accounts"] if coverage is None else coverage
    carry = GLOBAL_REF_CARRY["accounts"] if carry is None else carry
    rng = rng or random.Random(BASE_SEED + SOURCE_SEED_OFFSETS["accounts"])
    anchor = date.fromisoformat(ANCHOR_DATE)

    n_covered = min(round(len(persons) * coverage), n_masters)
    covered = sorted(rng.sample(range(len(persons)), n_covered))
    n_orphan = round(n_masters * DEFECTS["orphan"])
    n_linked = n_masters - n_orphan
    extra = n_linked - n_covered
    two = set(rng.sample(covered, extra)) if extra > 0 else set()

    cb_ref = {pi: f"CB-{rank + 1:05d}" for rank, pi in enumerate(covered)}  # 1/person, stable

    def n_rows(pi):
        return 2 if pi in two else 1

    seeded_rows = sum(n_rows(pi) for pi in covered if persons[pi]["global_ref"])
    carry_p = min(1.0, carry * n_masters / seeded_rows) if seeded_rows else 0.0

    rows = []
    acct = 10_001
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
                email = other["email"]  # conflicting match -> Phase 6 quarantine
            open_d = p["registration_date"] + timedelta(days=rng.randint(0, 200))
            if open_d > anchor:
                open_d = anchor
            act = open_d + timedelta(
                days=rng.randint(0, max(1, (anchor - open_d).days)))
            balance = round(rng.uniform(500, 4_000_000), 2)
            if rng.random() < 0.01:  # overdraft (valid negative)
                balance = -round(rng.uniform(10, 5_000), 2)
            rows.append({
                "account_id": f"ACC-{acct:05d}",
                "cb_customer_ref": cb_ref[pi],
                "global_customer_ref": ref,
                "customer_email": email,
                "account_number": str(rng.randint(10**9, 10**10 - 1)),
                "account_type": rng.choices(
                    ["SAVINGS", "CURRENT", "FIXED_DEPOSIT"],
                    weights=[0.55, 0.30, 0.15])[0],
                "currency": COUNTRY_CURRENCY[p["country"]],
                "balance": balance if rng.random() > 0.05 else None,
                "status": rng.choices(
                    ["ACTIVE", "DORMANT", "CLOSED"], weights=[0.80, 0.12, 0.08])[0],
                "open_date": dfx.format_date(rng, open_d),
                "last_activity_date": (dfx.format_date(rng, act)
                                       if rng.random() > 0.10 else None),
                "branch_code": (f"BR-{rng.randint(1, 120):03d}"
                                if rng.random() > 0.06 else None),
            })
            acct += 1

    # Orphans (2%): native ref resolves to nobody; email (when present) matches nobody.
    for j in range(n_orphan):
        open_d = date(2024, 6, 1) + timedelta(days=rng.randint(0, 500))
        rows.append({
            "account_id": f"ACC-{acct:05d}",
            "cb_customer_ref": f"CB-{99_000 + j:05d}",
            "global_customer_ref": None,
            "customer_email": (f"unmatched{j}@orphan-pool.example"
                               if rng.random() < DEFECTS["orphan_email_present"]
                               else None),
            "account_number": str(rng.randint(10**9, 10**10 - 1)),
            "account_type": rng.choices(
                ["SAVINGS", "CURRENT", "FIXED_DEPOSIT"],
                weights=[0.55, 0.30, 0.15])[0],
            "currency": "PHP",
            "balance": round(rng.uniform(500, 4_000_000), 2) if rng.random() > 0.05 else None,
            "status": rng.choices(
                ["ACTIVE", "DORMANT", "CLOSED"], weights=[0.80, 0.12, 0.08])[0],
            "open_date": dfx.format_date(rng, open_d),
            "last_activity_date": (dfx.format_date(rng, open_d + timedelta(days=30))
                                   if rng.random() > 0.10 else None),
            "branch_code": f"BR-{rng.randint(1, 120):03d}" if rng.random() > 0.06 else None,
        })
        acct += 1

    df = pd.DataFrame(rows, columns=COLUMNS)
    # Invalid emails only on rows that carry a global ref (stay resolvable via R1).
    ref_col = COLUMNS.index("global_customer_ref")
    allowed = [i for i in range(len(df)) if df.iat[i, ref_col] is not None]
    df = dfx.corrupt_emails_at(df, "customer_email", allowed, rng)
    return dfx.inject_duplicates(df, _near_alter, rng)
