"""Phase 5 standardization primitives (spec section 5) -- pure functions.

Every function takes/returns Spark columns or expressions; none touch a
table. try_to_date / try_to_timestamp / try_cast are invoked through F.expr
with column *names* (never interpolated values), and all regexes use [0-9]
character classes so no SQL string-literal escaping is involved
(Amendment 4; ANSI-mode safe).

These are the only places normalization logic lives; transform.py composes
them per entity, and tests/test_silver_transformation.py exercises each one.
"""

from pyspark.sql import functions as F
from pyspark.sql.types import StringType

from src.config import silver_config as cfg


def missing(colname):
    """A raw STRING business value is missing when NULL or empty after trim."""
    c = F.col(colname)
    return c.isNull() | (F.trim(c) == "")


def present(colname):
    return ~missing(colname)


def pattern_match_count(colname, regexes):
    """Count how many approved patterns match the raw value (Amendment 4:
    the caller requires exactly one for every non-null value)."""
    total = F.lit(0).cast("int")
    for rx in regexes:
        total = total + F.when(F.col(colname).rlike(rx), F.lit(1)).otherwise(F.lit(0))
    return total


def parse_date_expr(colname):
    """coalesce(try_to_date(col, p1), try_to_date(col, p2), try_to_date(col, p3))
    over the approved date patterns (spec section 5.2)."""
    parts = [f"try_to_date({colname}, '{p}')" for p in cfg.DATE_PATTERNS]
    return F.expr("coalesce(" + ", ".join(parts) + ")")


def parse_timestamp_expr(colname):
    """Same cascade for timestamps (spec section 5.2; three approved
    patterns, two precisions)."""
    parts = [f"try_to_timestamp({colname}, '{p}')" for p in cfg.TIMESTAMP_PATTERNS]
    return F.expr("coalesce(" + ", ".join(parts) + ")")


def try_cast_expr(colname, sql_type):
    """try_cast(col AS <sql_type>) -- NULL on malformed input (never raises;
    malformed-but-present values are validated to NUMERIC_UNPARSEABLE)."""
    return F.expr(f"try_cast({colname} AS {sql_type})")


def int_via_decimal_expr(colname):
    """Approved decimal-notation INT cast (A5 csat; A8 session_duration_minutes,
    Decision 1 approved 2026-11-24): parses through DECIMAL(18,3) and accepts
    only INTEGRAL decimal values -- '150.0' -> 150, '4.0' -> 4, NULL -> NULL.
    Genuinely fractional values ('150.7') return NULL and are rejected
    downstream (SESSION_DURATION_INVALID / CSAT_INVALID); they are never
    silently truncated to an integer."""
    dec = F.expr(f"try_cast({colname} AS decimal(18,3))")
    is_fractional = dec != F.floor(dec)
    return (F.when(dec.isNull() | is_fractional, F.lit(None).cast("int"))
            .otherwise(dec.cast("int")))


def strict_int_expr(colname):
    """Review fix 5: strict integer-notation cast for int columns whose
    contract requires integer notation (term_months, session_duration_minutes).
    Accepts optional-sign digits only: '45' -> 45; '45.0'/'45.7'/junk -> NULL
    (rejected downstream as NUMERIC_UNPARSEABLE / SESSION_DURATION_INVALID)."""
    c = F.trim(F.col(colname))
    return (F.when(c.rlike(r"^[+-]?[0-9]+$"),
                   F.expr(f"try_cast({colname} AS int)"))
            .otherwise(F.lit(None).cast("int")))


def normalize_email(colname):
    """lower(trim()); invalid -> NULL (D6: never a rejection). Raw kept in Bronze."""
    c = F.col(colname)
    cleaned = F.lower(F.trim(c))
    return (F.when(missing(colname), F.lit(None).cast(StringType()))
            .when(~cleaned.rlike(cfg.EMAIL_REGEX), F.lit(None).cast(StringType()))
            .otherwise(cleaned))


def email_invalid(colname):
    """Present but malformed -- for processing_audit.invalid_emails_nulled."""
    return present(colname) & ~F.lower(F.trim(F.col(colname))).rlike(cfg.EMAIL_REGEX)


def normalize_phone(colname):
    """Amendment 2: strip non-digits; >=10 digits -> last 10; fewer -> NULL."""
    digits = F.regexp_replace(F.trim(F.col(colname)), "[^0-9]", "")
    return (F.when(missing(colname), F.lit(None).cast(StringType()))
            .when(F.length(digits) >= cfg.PHONE_KEEP_DIGITS,
                  F.substring(digits, -cfg.PHONE_KEEP_DIGITS, cfg.PHONE_KEEP_DIGITS))
            .otherwise(F.lit(None).cast(StringType())))


def phone_invalid(colname):
    """Present but fewer than 10 digits -- for processing_audit counter."""
    digits = F.regexp_replace(F.trim(F.col(colname)), "[^0-9]", "")
    return present(colname) & (F.length(digits) < cfg.PHONE_KEEP_DIGITS)


def normalize_ref(colname):
    """Native refs / PKs: upper(trim) (spec section 5.5)."""
    return F.upper(F.trim(F.col(colname)))


def normalize_gref(colname):
    """global_customer_ref: upper(trim); ^CUST-[0-9]{6}$ else NULL (D6)."""
    cleaned = F.upper(F.trim(F.col(colname)))
    return (F.when(missing(colname), F.lit(None).cast(StringType()))
            .when(~cleaned.rlike(cfg.GREF_REGEX), F.lit(None).cast(StringType()))
            .otherwise(cleaned))


def gref_invalid(colname):
    return present(colname) & ~F.upper(F.trim(F.col(colname))).rlike(cfg.GREF_REGEX)


def _clean_name(colname):
    upper = F.upper(F.trim(F.col(colname)))
    no_punct = F.regexp_replace(upper, "[^A-Z0-9 ]", "")
    return F.regexp_replace(no_punct, " +", " ")


def name_key_expr(first_col, last_col):
    """Identity spec section 1: FIRST~LAST, upper, punctuation removed,
    whitespace collapsed."""
    return F.concat_ws("~", _clean_name(first_col), _clean_name(last_col))


def trim_expr(colname):
    return F.trim(F.col(colname))


# ---------- dedup keys (Amendment 1; review fix 4) ----------

_NULL_SENTINEL = "\u0000"  # literal single NUL char via unicode escape


def _as_string(col):
    return col.cast("string")


def _encode_value(col_str):
    """Length-prefixed encoding, one value at a time:
      NULL   -> NUL sentinel (single \u0000 char)
      value  -> '<len>:<value>'  (empty string -> '0:')
    This is a prefix code: the first character (NUL or digit) determines the
    parse, so values containing ':' or any delimiter cannot shift boundaries,
    and NULL is unambiguously distinct from an empty string."""
    return (F.when(col_str.isNull(), F.lit(_NULL_SENTINEL))
            .otherwise(F.concat(F.length(col_str).cast("string"),
                                F.lit(":"), col_str)))


def serialized_expr(colnames):
    """Deterministic, unambiguous serialization over the given columns
    (review fix 4). Column order is fixed by the caller; every value is
    length-prefixed, NULL-vs-empty is preserved, and no delimiter inside a
    value can collide with another column's boundary."""
    return F.concat(*[_encode_value(_as_string(F.col(c))) for c in colnames])


def canonical_serialized_expr(colnames):
    """Serialization over canonicalized (typed/normalized) business columns --
    used for the SHA-256 survivor hash and the lexicographic tie-break."""
    return serialized_expr(colnames)


def raw_serialized_expr(colnames):
    """Serialization over RAW bronze business columns -- exact/near duplicate
    classification (Amendment 1 criterion 3 compares against the survivor)."""
    return serialized_expr(colnames)


def canonical_hash_expr(colnames):
    """SHA-256 over the canonical serialization -- Amendment 1 criterion 2.
    Deterministic; does NOT establish factual correctness (documented)."""
    return F.sha2(canonical_serialized_expr(colnames), 256)


def null_count_expr(colnames):
    """Completeness: number of NULL business attributes (Amendment 1
    criterion 1 -- computed over canonicalized values)."""
    total = F.lit(0).cast("int")
    for c in colnames:
        total = total + F.when(F.col(c).isNull(), F.lit(1)).otherwise(F.lit(0))
    return total
