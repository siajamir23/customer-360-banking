"""Credit Card System generator (Phase 3). Same key engineering as accounts
(see src/generators/accounts.py header)."""
import random
from datetime import date, timedelta

import pandas as pd

from src.config.generator_config import (
    ANCHOR_DATE, BASE_SEED, DEFECTS, GLOBAL_REF_CARRY, PERSON_COVERAGE,
    SOURCE_SEED_OFFSETS, TARGETS,
)
from src.utilities import defects as dfx

COLUMNS = [
    "card_id", "cc_customer_ref", "global_customer_ref", "customer_email",
    "card_number_masked", "card_type", "credit_limit", "outstanding_balance",
    "card_status", "issue_date", "expiry_date",
]


def _near_alter(row, rng):
    """Near-duplicate: exactly one domain-safe attribute altered."""
    row["card_number_masked"] = f"****-****-****-{rng.randint(0, 9999):04d}"
    return row


def generate(persons, n_masters=None, coverage=None, carry=None, rng=None):
    n_masters = TARGETS["cards"] if n_masters is None else n_masters
    coverage = PERSON_COVERAGE["cards"] if coverage is None else coverage
    carry = GLOBAL_REF_CARRY["cards"] if carry is None else carry
    rng = rng or random.Random(BASE_SEED + SOURCE_SEED_OFFSETS["cards"])
    anchor = date.fromisoformat(ANCHOR_DATE)

    n_covered = min(round(len(persons) * coverage), n_masters)
    covered = sorted(rng.sample(range(len(persons)), n_covered))
    n_orphan = round(n_masters * DEFECTS["orphan"])
    n_linked = n_masters - n_orphan
    extra = n_linked - n_covered
    two = set(rng.sample(covered, extra)) if extra > 0 else set()

    cc_ref = {pi: f"CC-{rank + 1:05d}" for rank, pi in enumerate(covered)}

    def n_rows(pi):
        return 2 if pi in two else 1

    seeded_rows = sum(n_rows(pi) for pi in covered if persons[pi]["global_ref"])
    carry_p = min(1.0, carry * n_masters / seeded_rows) if seeded_rows else 0.0

    rows = []
    card = 10_001

    def card_row(p, rng, ref=None, email=None):
        issue = p["registration_date"] + timedelta(days=rng.randint(0, 900))
        if issue > anchor:
            issue = anchor
        limit = round(rng.uniform(20_000, 1_000_000), 2)
        outstanding = round(rng.uniform(0, limit), 2)
        if rng.random() < 0.02:  # over-limit state (valid, flagged downstream)
            outstanding = round(limit * rng.uniform(1.01, 1.15), 2)
        return {
            "card_id": f"CRD-{card:05d}",
            "cc_customer_ref": None,  # filled by caller
            "global_customer_ref": ref,
            "customer_email": email,
            "card_number_masked": f"****-****-****-{rng.randint(0, 9999):04d}",
            "card_type": rng.choices(
                ["CLASSIC", "GOLD", "PLATINUM", "SIGNATURE"],
                weights=[0.50, 0.25, 0.15, 0.10])[0],
            "credit_limit": limit if rng.random() > 0.05 else None,
            "outstanding_balance": outstanding if rng.random() > 0.05 else None,
            "card_status": rng.choices(
                ["ACTIVE", "BLOCKED", "EXPIRED", "CANCELLED"],
                weights=[0.85, 0.05, 0.07, 0.03])[0],
            "issue_date": dfx.format_date(rng, issue),
            "expiry_date": dfx.format_date(
                rng, issue + timedelta(days=365 * rng.choice([3, 4, 5]) + rng.randint(0, 364))),
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
            row = card_row(p, rng, ref, email)
            row["cc_customer_ref"] = cc_ref[pi]
            rows.append(row)
            card += 1

    for j in range(n_orphan):
        p = persons[j % len(persons)]  # PII shell only; keys are fabricated below
        row = card_row(p, rng, None,
                       f"unmatched{j}@orphan-pool.example"
                       if rng.random() < DEFECTS["orphan_email_present"] else None)
        row["cc_customer_ref"] = f"CC-{99_000 + j:05d}"
        rows.append(row)
        card += 1

    df = pd.DataFrame(rows, columns=COLUMNS)
    ref_col = COLUMNS.index("global_customer_ref")
    allowed = [i for i in range(len(df)) if df.iat[i, ref_col] is not None]
    df = dfx.corrupt_emails_at(df, "customer_email", allowed, rng)
    return dfx.inject_duplicates(df, _near_alter, rng)
