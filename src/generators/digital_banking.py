"""Digital Banking login-events generator (Phase 3). Same key engineering as
accounts (see src/generators/accounts.py header). Highest-volume source.

Login events: 55,000 masters distributed ~6 per covered person within the fixed
12-month event window (2025-12-01 .. 2026-11-30; no now() anywhere)."""
import random
from datetime import date, datetime, time, timedelta

import pandas as pd

from src.config.generator_config import (
    BASE_SEED, DEFECTS, EVENT_WINDOW, GLOBAL_REF_CARRY, PERSON_COVERAGE,
    SOURCE_SEED_OFFSETS, TARGETS,
)
from src.utilities import defects as dfx

COLUMNS = [
    "login_id", "db_customer_ref", "global_customer_ref", "customer_email",
    "login_timestamp", "channel", "device_id", "device_type", "os", "browser",
    "login_status", "session_duration_minutes",
]

OS_POOL = ["Android 13", "Android 14", "iOS 16", "iOS 17", "Windows 11", "macOS"]
BROWSER_POOL = ["Chrome", "Safari", "Firefox", "Edge", "Mobile Web"]


def _near_alter(row, rng):
    """Near-duplicate: exactly one domain-safe attribute altered."""
    row["browser"] = rng.choice(BROWSER_POOL)
    return row


def generate(persons, n_masters=None, coverage=None, carry=None, rng=None):
    n_masters = TARGETS["digital_logins"] if n_masters is None else n_masters
    coverage = PERSON_COVERAGE["digital_logins"] if coverage is None else coverage
    carry = GLOBAL_REF_CARRY["digital_logins"] if carry is None else carry
    rng = rng or random.Random(BASE_SEED + SOURCE_SEED_OFFSETS["digital_logins"])

    win_start = date.fromisoformat(EVENT_WINDOW[0])
    win_end = date.fromisoformat(EVENT_WINDOW[1])
    total_minutes = int((win_end - win_start).total_seconds() // 60)

    n_covered = min(round(len(persons) * coverage), n_masters)
    covered = sorted(rng.sample(range(len(persons)), n_covered))
    n_orphan = round(n_masters * DEFECTS["orphan"])
    n_linked = n_masters - n_orphan

    # distribute linked events over covered persons (~6 each, exact total)
    counts = [n_linked // n_covered] * n_covered
    for k in range(n_linked - sum(counts)):
        counts[k] += 1

    seeded_rows = sum(c for pi, c in zip(covered, counts)
                      if persons[pi]["global_ref"])
    carry_p = min(1.0, carry * n_masters / seeded_rows) if seeded_rows else 0.0

    db_ref = {pi: f"DB-{rank + 1:05d}" for rank, pi in enumerate(covered)}

    rows = []
    login = 1_000_001

    def login_row(p, rng, ref=None, email=None, db_ref_value=None):
        ts = (datetime.combine(win_start, time(0, 0))
              + timedelta(minutes=rng.randint(0, total_minutes)))
        status = rng.choices(["SUCCESS", "FAILED"], weights=[0.93, 0.07])[0]
        return {
            "login_id": f"LOG-{login:07d}",
            "db_customer_ref": db_ref_value,
            "global_customer_ref": ref,
            "customer_email": email,
            "login_timestamp": dfx.format_datetime(rng, ts),
            "channel": rng.choices(["WEB", "MOBILE_APP"], weights=[0.50, 0.50])[0],
            "device_id": f"DEV-{rng.randint(10**7, 10**8 - 1)}",
            "device_type": rng.choices(
                ["ANDROID", "IOS", "DESKTOP", "TABLET"],
                weights=[0.40, 0.35, 0.18, 0.07])[0],
            "os": rng.choice(OS_POOL) if rng.random() > 0.10 else None,
            "browser": rng.choice(BROWSER_POOL) if rng.random() > 0.10 else None,
            "login_status": status,
            "session_duration_minutes": (
                rng.randint(1, 180) if status == "SUCCESS" else None),
        }

    for pi, count in zip(covered, counts):
        p = persons[pi]
        for _ in range(count):
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
            rows.append(login_row(p, rng, ref, email, db_ref[pi]))
            login += 1

    for j in range(n_orphan):
        p = persons[j % len(persons)]
        # fabricated native refs in an unused 5-digit range (no collision with
        # real DB refs or the ^DB-\d{5}$ contract: 95,001..96,099 for 1,100 orphans)
        rows.append(login_row(
            p, rng, None,
            f"unmatched{j}@orphan-pool.example"
            if rng.random() < DEFECTS["orphan_email_present"] else None,
            db_ref_value=f"DB-{95_000 + j:05d}"))
        login += 1

    df = pd.DataFrame(rows, columns=COLUMNS)
    ref_col = COLUMNS.index("global_customer_ref")
    allowed = [i for i in range(len(df)) if df.iat[i, ref_col] is not None]
    df = dfx.corrupt_emails_at(df, "customer_email", allowed, rng)
    return dfx.inject_duplicates(df, _near_alter, rng)
