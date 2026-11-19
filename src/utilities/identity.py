"""Shared population builder + identity normalization utilities (Phase 3).

Approved email-matching rules (02-source-data-contracts.md, convention 7):
  1. customer_email represents the record's holder.
  2. Emails are normalized (trim, then lowercase) before any matching.
  3. A missing email is never an automatic rejection (fall through to the next
     match rule; product rows without a ref resolve via R2 or the orphan path).
  4. Conflicting email matches are quarantined downstream (Phase 6), never merged.
  5. Email alone never overrides a contradicting higher-priority identifier.
"""
import random
import re
from datetime import date, datetime, timedelta

from faker import Faker

from src.config.generator_config import (
    ANCHOR_DATE, BASE_SEED, COUNTRIES, POPULATION, SOURCE_SEED_OFFSETS,
)

EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
DATE_FORMATS = ("%Y-%m-%d", "%d/%m/%Y", "%m-%d-%Y")


def normalize_email(value):
    """Rule 2: trim whitespace, then lowercase. Invalid -> None (never a match key)."""
    if value is None:
        return None
    v = str(value).strip().lower()
    return v if EMAIL_RE.match(v) else None


def normalize_phone(value):
    """Strip non-digits; keep the last 10 digits; fewer than 7 digits -> None."""
    if value is None:
        return None
    digits = re.sub(r"\D", "", str(value))
    return digits[-10:] if len(digits) >= 7 else None


def normalize_name(value):
    """Upper, strip punctuation, collapse internal whitespace."""
    if not value:
        return None
    v = re.sub(r"[^\w\s]", " ", str(value).upper())
    return re.sub(r"\s+", " ", v).strip()


def parse_date_any(value):
    """Parse the 3 contracted source formats -> date; unparseable -> None."""
    if value is None:
        return None
    v = str(value).strip()
    for fmt in DATE_FORMATS:
        try:
            return datetime.strptime(v, fmt).date()
        except ValueError:
            continue
    return None


def build_population(fraud_rows, n_new=None, n_seeded=None):
    """Build the approved 12,000-person shared population.

    fraud_rows: read-only dicts from fraud_detection.silver.customers
    (customer_id, age, country, registration_date). ALL 10,000 existing fraud
    customers are seeded with canonical IDs preserved (100% integration
    coverage); the 2,000 new persons carry no global_customer_ref and are
    reserved for R5 assignment (CUST-100001+, Phase 6).
    n_seeded/n_new default to the approved sizes; tests may override them with
    small synthetic row sets (never the real table).
    """
    fake = Faker()
    Faker.seed(BASE_SEED + SOURCE_SEED_OFFSETS["population"])
    rng = random.Random(BASE_SEED + SOURCE_SEED_OFFSETS["population"])
    n_new = POPULATION["new_customers"] if n_new is None else n_new
    n_seeded = POPULATION["seeded_fraud_customers"] if n_seeded is None else n_seeded

    rows = sorted(
        list(fraud_rows)[:n_seeded],
        key=lambda r: str(r.get("customer_id")),
    )
    if len(rows) < n_seeded:
        raise ValueError(
            f"Expected {n_seeded} fraud customers, "
            f"found {len(rows)} (read-only source: fraud_detection.silver.customers)"
        )

    anchor = date.fromisoformat(ANCHOR_DATE)
    persons, used_emails = [], set()
    for i, row in enumerate(rows):
        persons.append(_make_person(
            fake, rng, used_emails, i, anchor,
            customer_id=str(row.get("customer_id")),
            age=row.get("age"),
            country=row.get("country"),
            registration_date=row.get("registration_date"),
        ))
    for i in range(n_new):
        persons.append(_make_person(fake, rng, used_emails, len(rows) + i, anchor))
    return persons


def _make_person(fake, rng, used_emails, idx, anchor, customer_id=None, age=None,
                 country=None, registration_date=None):
    first, middle, last = fake.first_name(), fake.first_name(), fake.last_name()
    domain = rng.choice(
        ["gmail.com", "yahoo.com", "outlook.com", "example.org", "mail.com"])
    email = f"{first}.{last}{idx}@{domain}".lower()
    while email in used_emails:  # canonical emails unique per person (rule 1)
        email = f"{first}.{last}{idx}x{rng.randint(1, 999)}@{domain}".lower()
    used_emails.add(email)

    try:
        a = int(age)
        dob = anchor.replace(year=anchor.year - a) if 18 <= a <= 100 else None
    except (TypeError, ValueError):
        dob = None
    if dob is None:
        dob = anchor.replace(year=anchor.year - rng.randint(18, 85))

    reg = parse_date_any(registration_date)
    if reg is None:  # new customers onboarded recently
        reg = anchor - timedelta(days=rng.randint(30, 700))
    if reg > anchor:
        reg = anchor
    country = str(country) if country in COUNTRIES else rng.choice(COUNTRIES)

    return {
        "person_id": idx,
        "global_ref": customer_id,          # None for NEW persons (R5, Phase 6)
        "origin": "FRAUD_SEED" if customer_id else "NEW",
        "first_name": first,
        "middle_name": middle,
        "last_name": last,
        "dob": dob,
        "email": email,
        "phone_digits": "9" + "".join(rng.choice("0123456789") for _ in range(9)),
        "address_line1": fake.street_address(),
        "city": fake.city(),
        "country": country,
        "postal_code": str(rng.randint(1000, 99999)),
        "registration_date": reg,
    }
