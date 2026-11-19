"""CIS customer master generator (Phase 3).

One row per person in the shared population (12,000 masters). CIS is the
identity master: the 10,000 seeded fraud customers carry their canonical
global refs; the 2,000 new persons carry none (R5, Phase 6)."""
import random

import pandas as pd
from faker import Faker

from src.config.generator_config import BASE_SEED, DEFECTS, SOURCE_SEED_OFFSETS, TARGETS
from src.utilities import defects as dfx

COLUMNS = [
    "cis_customer_id", "global_customer_ref", "first_name", "middle_name",
    "last_name", "date_of_birth", "gender", "email", "phone", "address_line1",
    "city", "country", "postal_code", "registration_date", "kyc_status",
    "customer_segment", "annual_income", "status",
]


def _near_alter(row, rng):
    """Near-duplicate: exactly one attribute altered (email case -- spec example)."""
    if row.get("email"):
        row["email"] = str(row["email"]).swapcase()
    else:
        row["postal_code"] = None
    return row


def generate(persons, n_masters=None, rng=None):
    n_masters = TARGETS["customers"] if n_masters is None else n_masters
    rng = rng or random.Random(BASE_SEED + SOURCE_SEED_OFFSETS["customers"])
    Faker.seed(BASE_SEED + SOURCE_SEED_OFFSETS["customers"])

    subset = persons[:n_masters]
    if len(subset) != n_masters:
        raise ValueError("customers masters must equal the population size")

    rows = []
    for p in subset:
        rows.append({
            "cis_customer_id": f"CIS-{p['person_id'] + 1:05d}",
            "global_customer_ref": p["global_ref"],
            "first_name": p["first_name"],
            "middle_name": p["middle_name"] if rng.random() > 0.30 else None,
            "last_name": p["last_name"],
            "date_of_birth": dfx.format_date(rng, p["dob"]),
            "gender": rng.choices(["M", "F", "U"], weights=[0.48, 0.48, 0.04])[0],
            "email": p["email"] if rng.random() < 0.95 else None,  # ~95% populated
            "phone": dfx.format_phone(rng, p["phone_digits"]),
            "address_line1": p["address_line1"] if rng.random() > 0.05 else None,
            "city": p["city"] if rng.random() > 0.05 else None,
            "country": p["country"],
            "postal_code": p["postal_code"] if rng.random() > 0.08 else None,
            "registration_date": dfx.format_date(rng, p["registration_date"]),
            "kyc_status": rng.choices(
                ["VERIFIED", "PENDING", "REJECTED"], weights=[0.85, 0.12, 0.03])[0],
            "customer_segment": (rng.choices(
                ["RETAIL", "PREMIUM", "MASS_AFFLUENT", "PRIVATE"],
                weights=[0.55, 0.20, 0.20, 0.05])[0]
                if rng.random() > 0.08 else None),
            "annual_income": (round(rng.uniform(180_000, 9_500_000), 2)
                               if rng.random() > 0.07 else None),
            "status": rng.choices(
                ["ACTIVE", "INACTIVE", "CLOSED"], weights=[0.93, 0.05, 0.02])[0],
        })

    df = pd.DataFrame(rows, columns=COLUMNS)
    # ~5% of populated emails invalid (CIS is the master; identity resolution
    # does not depend on CIS email validity for seeded rows -- they match by ref).
    df = dfx.corrupt_emails_at(df, "email", range(len(df)), rng)
    return dfx.inject_duplicates(df, _near_alter, rng)
