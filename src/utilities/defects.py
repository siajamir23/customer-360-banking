"""Defect-injection utilities (Phase 3). All rates come from
generator_config.DEFECTS; every function is driven by a caller-supplied seeded
rng so defects reproduce exactly on rerun."""

import pandas as pd

from src.config.generator_config import DEFECTS


def format_date(rng, d):
    """Render a date in one of the 3 contracted formats (60/25/15 mix)."""
    mix = DEFECTS["date_formats"]
    r = rng.random()
    if r < mix[0]:
        return d.strftime("%Y-%m-%d")
    if r < mix[0] + mix[1]:
        return d.strftime("%d/%m/%Y")
    return d.strftime("%m-%d-%Y")


def format_datetime(rng, dt):
    """Datetime variants of the 3 contracted formats."""
    mix = DEFECTS["date_formats"]
    r = rng.random()
    if r < mix[0]:
        return dt.strftime("%Y-%m-%d %H:%M:%S")
    if r < mix[0] + mix[1]:
        return dt.strftime("%d/%m/%Y %H:%M")
    return dt.strftime("%m-%d-%Y %H:%M")


def format_phone(rng, digits):
    """3 presentation variants of the same digits (normalization recovers them)."""
    r = rng.random()
    if r < 0.34:
        return f"+{digits[0]}{digits[1:4]} {digits[4:7]} {digits[7:]}"
    if r < 0.67:
        return f"{digits[:4]}-{digits[4:7]}-{digits[7:]}"
    return f"({digits[:2]}) {digits[2:6]} {digits[6:]}"


def corrupt_email(rng, value):
    """Invalidate ~1 email: drop @, break domain dot, or inject whitespace."""
    if not value:
        return value
    r = rng.random()
    if r < 0.34:
        return str(value).replace("@", " at ")
    if r < 0.67:
        local, dom = str(value).rsplit("@", 1)
        return f"{local}@{dom.replace('.', '')}"
    return f" {value} "


def corrupt_emails_at(df, email_col, allowed_indices, rng):
    """Corrupt ~invalid_email of the populated email cells in allowed_indices.

    Callers pass only rows that stay resolvable via R1 anyway (rows carrying a
    valid global ref), so an invalid email never makes a row unresolvable
    (approved rule 3: missing/invalid email is never an automatic rejection).
    """
    out = df.copy()
    pos = out.columns.get_loc(email_col)
    for i in allowed_indices:
        if (rng.random() < DEFECTS["invalid_email"]
                and i < len(out) and pd.notna(out.iat[i, pos])):
            out.iat[i, pos] = corrupt_email(rng, out.iat[i, pos])
    return out


def inject_duplicates(df, near_alter, rng):
    """Append exact duplicates (2% of masters) + near duplicates (1.5% of
    masters) and deterministically shuffle. The only rows appended beyond
    masters are these two contracted duplicate types.

    near_alter(row_dict, rng) must alter exactly ONE domain-safe attribute.
    """
    n = len(df)
    n_exact = round(n * DEFECTS["exact_duplicate"])
    n_near = round(n * DEFECTS["near_duplicate"])
    exact = df.iloc[rng.sample(range(n), n_exact)].copy() if n_exact else df.iloc[0:0]
    near_rows = [near_alter(df.iloc[i].to_dict(), rng)
                 for i in rng.sample(range(n), n_near)] if n_near else []
    near = pd.DataFrame(near_rows, columns=df.columns) if near_rows else df.iloc[0:0]
    out = pd.concat([df, exact, near], ignore_index=True)
    order = list(range(len(out)))
    rng.shuffle(order)
    return out.iloc[order].reset_index(drop=True)
