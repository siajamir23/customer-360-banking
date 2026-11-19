"""Customer Support System generator (Phase 3). Same key engineering as
accounts (see src/generators/accounts.py header)."""
import random
from datetime import date, timedelta

import pandas as pd

from src.config.generator_config import (
    BASE_SEED, DEFECTS, EVENT_WINDOW, GLOBAL_REF_CARRY, PERSON_COVERAGE,
    SOURCE_SEED_OFFSETS, TARGETS,
)
from src.utilities import defects as dfx

COLUMNS = [
    "ticket_id", "cs_customer_ref", "global_customer_ref", "customer_email",
    "channel", "category", "priority", "status", "created_date",
    "resolved_date", "csat_score",
]


def _near_alter(row, rng):
    """Near-duplicate: exactly one domain-safe attribute altered."""
    row["priority"] = rng.choice(["LOW", "MEDIUM", "HIGH", "CRITICAL"])
    return row


def generate(persons, n_masters=None, coverage=None, carry=None, rng=None):
    n_masters = TARGETS["support_cases"] if n_masters is None else n_masters
    coverage = PERSON_COVERAGE["support_cases"] if coverage is None else coverage
    carry = GLOBAL_REF_CARRY["support_cases"] if carry is None else carry
    rng = rng or random.Random(BASE_SEED + SOURCE_SEED_OFFSETS["support_cases"])

    win_start = date.fromisoformat(EVENT_WINDOW[0])
    win_end = date.fromisoformat(EVENT_WINDOW[1])
    span_days = (win_end - win_start).days

    n_covered = min(round(len(persons) * coverage), n_masters)
    covered = sorted(rng.sample(range(len(persons)), n_covered))
    n_orphan = round(n_masters * DEFECTS["orphan"])
    n_linked = n_masters - n_orphan
    extra = n_linked - n_covered
    two = set(rng.sample(covered, extra)) if extra > 0 else set()

    cs_ref = {pi: f"CS-{rank + 1:05d}" for rank, pi in enumerate(covered)}

    def n_rows(pi):
        return 2 if pi in two else 1

    seeded_rows = sum(n_rows(pi) for pi in covered if persons[pi]["global_ref"])
    carry_p = min(1.0, carry * n_masters / seeded_rows) if seeded_rows else 0.0

    rows = []
    ticket = 10_001

    def ticket_row(p, rng, ref=None, email=None, cs_ref_value=None):
        created = win_start + timedelta(days=rng.randint(0, span_days))
        status = rng.choices(
            ["OPEN", "IN_PROGRESS", "RESOLVED", "CLOSED"],
            weights=[0.10, 0.15, 0.45, 0.30])[0]
        resolved = None
        csat = None
        if status in ("RESOLVED", "CLOSED"):
            resolved = created + timedelta(days=rng.randint(0, 30))
            if resolved > win_end:
                resolved = win_end
            csat = rng.randint(1, 5)
        return {
            "ticket_id": f"TKT-{ticket:05d}",
            "cs_customer_ref": cs_ref_value,
            "global_customer_ref": ref,
            "customer_email": email,
            "channel": rng.choices(
                ["EMAIL", "PHONE", "CHAT", "BRANCH", "MOBILE_APP"],
                weights=[0.30, 0.25, 0.20, 0.15, 0.10])[0],
            "category": rng.choice(
                ["BILLING", "FRAUD_ALERT", "ACCOUNT_ACCESS",
                 "TECHNICAL", "LOAN", "CARD"]),
            "priority": rng.choices(
                ["LOW", "MEDIUM", "HIGH", "CRITICAL"],
                weights=[0.40, 0.35, 0.20, 0.05])[0],
            "status": status,
            "created_date": dfx.format_date(rng, created),
            "resolved_date": dfx.format_date(rng, resolved) if resolved else None,
            "csat_score": csat,
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
            row = ticket_row(p, rng, ref, email, cs_ref[pi])
            rows.append(row)
            ticket += 1

    for j in range(n_orphan):
        p = persons[j % len(persons)]
        row = ticket_row(p, rng, None,
                         f"unmatched{j}@orphan-pool.example"
                         if rng.random() < DEFECTS["orphan_email_present"] else None,
                         cs_ref_value=f"CS-{99_000 + j:05d}")
        rows.append(row)
        ticket += 1

    df = pd.DataFrame(rows, columns=COLUMNS)
    ref_col = COLUMNS.index("global_customer_ref")
    allowed = [i for i in range(len(df)) if df.iat[i, ref_col] is not None]
    df = dfx.corrupt_emails_at(df, "customer_email", allowed, rng)
    return dfx.inject_duplicates(df, _near_alter, rng)
