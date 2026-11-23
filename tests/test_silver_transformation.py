"""Phase 5 unit tests (spec section 12; Amendments 2-6).

Every test runs on isolated synthetic DataFrames / temp views ONLY -- no
production Bronze, Silver, or quarantine table is read or written
(Amendment 6). Invalid records are constructed in memory and never persisted.

Coverage (per approved testing requirements):
  * all three date formats and all three timestamp formats, both precisions
  * invalid dates / timestamps
  * valid + invalid email normalization
  * phone normalization (Amendment 2: last 10 digits, <10 -> NULL)
  * numeric casting incl. nullable numerics and csat '4.0' (A5)
  * exact duplicates, near duplicates, deterministic survivor selection
  * multiple rejection reasons for a single record
  * distinct-record reconciliation (Amendment 3)
  * every quarantine rule ID in silver_config.RULE_IDS
  * fail-closed idempotency guard behavior
  * remediation regressions: semantically impossible dates/timestamps that
    match an approved regex; identical NULL-PK records stay independent;
    NULL-vs-empty and delimiter-collision serialization; audit-table
    idempotency detection (batch_id column); fractional integer fields;
    deterministic survivor selection under the new serialization; multi-
    invalid, multi-reason reconciliation
  * approved decisions (2026-11-24): Amendment 7 group-wide exact/near
    classification (byte-identical repeat of an earlier group record =
    EXACT, evaluated against all records not only the survivor); A8
    session_duration_minutes integral decimal-notation cast ('150.0' -> 150,
    '150.7' rejected, never silently truncated)

Run: cd <repo root> && python tests/test_silver_transformation.py
"""

import os
import sys
import unittest
from datetime import date, datetime
from decimal import Decimal

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pyspark.sql import Row
from pyspark.sql import functions as F
from pyspark.sql.types import (DateType, StringType, StructField, StructType,
                                TimestampType)

from databricks.sdk.runtime import spark

from src.config import bronze_config as bcfg
from src.config import silver_config as cfg
from src.silver import standardizer as std
from src.silver import transform as tf

RUN_TS = datetime(2026, 11, 21, 12, 0, 0)
BATCH = "UT_BATCH"

BASE = {
    "customers": {
        "cis_customer_id": "CIS-12345", "global_customer_ref": "CUST-012345",
        "first_name": "Juan", "middle_name": "Reyes", "last_name": "Dela",
        "date_of_birth": "1990-01-15", "gender": "M",
        "email": "Juan.Dela@Example.COM", "phone": "+63 917 555 1234",
        "address_line1": "123 Rizal St", "city": "Manila", "country": "PH",
        "postal_code": "1000", "registration_date": "2015-06-01",
        "kyc_status": "VERIFIED", "customer_segment": "RETAIL",
        "annual_income": "50000.00", "status": "ACTIVE",
    },
    "accounts": {
        "account_id": "ACC-12345", "cb_customer_ref": "CB-12345",
        "global_customer_ref": "CUST-012345", "customer_email": "juan@example.com",
        "account_number": "1234567890", "account_type": "SAVINGS",
        "currency": "PHP", "balance": "1000.50", "status": "ACTIVE",
        "open_date": "2021-01-10", "last_activity_date": "2026-01-05",
        "branch_code": "BR-101",
    },
    "cards": {
        "card_id": "CRD-12345", "cc_customer_ref": "CC-12345",
        "global_customer_ref": "CUST-012345", "customer_email": "juan@example.com",
        "card_number_masked": "****-****-****-1234", "card_type": "GOLD",
        "credit_limit": "150000.00", "outstanding_balance": "2500.00",
        "card_status": "ACTIVE", "issue_date": "2023-01-01",
        "expiry_date": "2028-01-01",
    },
    "loans": {
        "loan_id": "LON-12345", "ln_customer_ref": "LN-12345",
        "global_customer_ref": "CUST-012345", "customer_email": "juan@example.com",
        "loan_type": "PERSONAL", "principal_amount": "100000.00",
        "outstanding_balance": "50000.00", "interest_rate": "12.50",
        "term_months": "36", "monthly_installment": "3000.00",
        "start_date": "2024-01-01", "end_date": None,
        "repayment_status": "CURRENT",
    },
    "digital_logins": {
        "login_id": "LOG-1234567", "db_customer_ref": "DB-12345",
        "global_customer_ref": "CUST-012345", "customer_email": "juan@example.com",
        "login_timestamp": "2026-01-15 09:30:00", "channel": "WEB",
        "device_id": "DEV-1234", "device_type": "DESKTOP", "os": "Windows 10",
        "browser": "Chrome", "login_status": "SUCCESS",
        "session_duration_minutes": "45",
    },
    "support_tickets": {
        "ticket_id": "TKT-12345", "cs_customer_ref": "CS-12345",
        "global_customer_ref": "CUST-012345", "customer_email": "juan@example.com",
        "channel": "EMAIL", "category": "BILLING", "priority": "MEDIUM",
        "status": "OPEN", "created_date": "2026-02-01",
        "resolved_date": None, "csat_score": None,
    },
}


def bronze_schema(entity):
    fields = [StructField(c, StringType()) for c in bcfg.BUSINESS_COLUMNS[entity]]
    fields += [
        StructField("_source_system", StringType()),
        StructField("_batch_id", StringType()),
        StructField("_ingestion_timestamp", TimestampType()),
        StructField("_ingestion_date", DateType()),
        StructField("_source_file", StringType()),
    ]
    return StructType(fields)


def mk(entity, rows):
    """rows: list of dicts overriding BASE values (None -> NULL).
    Returns an isolated in-memory bronze-like DataFrame."""
    full = []
    for r in rows:
        merged = dict(BASE[entity])
        merged.update(r)
        merged["_source_system"] = "unit_test"
        merged["_batch_id"] = BATCH
        merged["_ingestion_timestamp"] = RUN_TS
        merged["_ingestion_date"] = RUN_TS.date()
        merged["_source_file"] = "unit_test.csv"
        full.append(Row(**merged))
    return spark.createDataFrame(full, schema=bronze_schema(entity))


def pipeline(entity, rows):
    """Full in-memory pipeline (canonicalize -> dedup -> validate -> build).
    Nothing is written anywhere."""
    bronze = mk(entity, rows)
    canon = tf.canonicalize(bronze, entity)
    tf.assert_unambiguous_formats(canon, entity)
    survivors, dups = tf.dedup(canon, entity, RUN_TS)
    valid, invalid, rejected = tf.validate(survivors, entity, RUN_TS)
    silver = tf.build_silver_rows(valid, entity, RUN_TS)
    counters = tf.entity_counters(valid, invalid, rejected, dups, entity, bronze)
    return bronze, silver, invalid, dups, counters


class TestDateAndTimestampParsing(unittest.TestCase):

    def test_all_three_date_formats(self):
        rows = [
            {"cis_customer_id": "CIS-00001", "date_of_birth": "1990-01-15"},
            {"cis_customer_id": "CIS-00002", "date_of_birth": "15/01/1990"},
            {"cis_customer_id": "CIS-00003", "date_of_birth": "01-15-1990"},
        ]
        _, silver, invalid, _, _ = pipeline("customers", rows)
        self.assertEqual(silver.count(), 3)
        self.assertEqual(invalid.count(), 0)
        dobs = {r["cis_customer_id"]: r["date_of_birth"] for r in silver.collect()}
        for i in ("CIS-00001", "CIS-00002", "CIS-00003"):
            self.assertEqual(dobs[i], date(1990, 1, 15))

    def test_invalid_date_quarantined(self):
        rows = [{"cis_customer_id": "CIS-00001", "date_of_birth": "15.01.1990"}]
        _, silver, invalid, _, counters = pipeline("customers", rows)
        self.assertEqual(silver.count(), 0)
        reasons = {r["_rejection_reason"] for r in invalid.collect()}
        self.assertEqual(reasons, {"INVALID_DATE_FORMAT"})
        self.assertEqual(counters["rejected_distinct_rows"], 1)

    def test_all_three_timestamp_formats_both_precisions(self):
        rows = [
            {"login_id": "LOG-1000001", "login_timestamp": "2026-01-15 09:30:00"},  # ISO w/ seconds
            {"login_id": "LOG-1000002", "login_timestamp": "15/01/2026 09:30"},   # dd/MM HH:mm
            {"login_id": "LOG-1000003", "login_timestamp": "01-15-2026 09:30"},   # MM-dd HH:mm
        ]
        _, silver, invalid, _, _ = pipeline("digital_logins", rows)
        self.assertEqual(silver.count(), 3)
        self.assertEqual(invalid.count(), 0)
        ts = {r["login_id"]: r["login_timestamp"] for r in silver.collect()}
        expected = datetime(2026, 1, 15, 9, 30, 0)
        for i in ("LOG-1000001", "LOG-1000002", "LOG-1000003"):
            self.assertEqual(ts[i], expected)

    def test_invalid_timestamp_quarantined(self):
        rows = [{"login_id": "LOG-1000001", "login_timestamp": "2026/01/15 09:30"}]
        _, silver, invalid, _, _ = pipeline("digital_logins", rows)
        self.assertEqual(silver.count(), 0)
        reasons = {r["_rejection_reason"] for r in invalid.collect()}
        self.assertEqual(reasons, {"INVALID_TIMESTAMP_FORMAT"})


class TestSemanticDateTimestampValidity(unittest.TestCase):
    """Review fix 1: a present value whose parse returns NULL is rejected even
    when exactly one approved regex matches (impossible calendar dates)."""

    def test_impossible_dates_matching_regex_rejected(self):
        rows = [
            {"cis_customer_id": "CIS-00001", "date_of_birth": "2026-02-31"},  # ISO, Feb 31
            {"cis_customer_id": "CIS-00002", "date_of_birth": "31/02/2026"},  # dd/MM, Feb 31
            {"cis_customer_id": "CIS-00003", "date_of_birth": "02-31-2026"},  # MM-dd, Feb 31
            {"cis_customer_id": "CIS-00004", "date_of_birth": "29/02/2025"},  # non-leap Feb 29
        ]
        _, silver, invalid, _, counters = pipeline("customers", rows)
        self.assertEqual(silver.count(), 0)
        self.assertEqual({r["_rejection_reason"] for r in invalid.collect()},
                         {"INVALID_DATE_FORMAT"})
        self.assertEqual(counters["rejected_distinct_rows"], 4)

    def test_impossible_timestamp_matching_regex_rejected(self):
        rows = [
            {"login_id": "LOG-1000001", "login_timestamp": "2026-02-31 09:30:00"},
            {"login_id": "LOG-1000002", "login_timestamp": "31/02/2026 09:30"},
        ]
        _, silver, invalid, _, _ = pipeline("digital_logins", rows)
        self.assertEqual(silver.count(), 0)
        self.assertEqual({r["_rejection_reason"] for r in invalid.collect()},
                         {"INVALID_TIMESTAMP_FORMAT"})


class TestNormalization(unittest.TestCase):

    def test_email_valid_normalized_invalid_nulled(self):
        rows = [
            {"cis_customer_id": "CIS-00001", "email": "  Juan.Dela@Example.COM  "},
            {"cis_customer_id": "CIS-00002", "email": "not-an-email"},
        ]
        bronze, silver, invalid, _, counters = pipeline("customers", rows)
        emails = {r["cis_customer_id"]: r["email"] for r in silver.collect()}
        self.assertEqual(emails["CIS-00001"], "juan.dela@example.com")
        self.assertIsNone(emails["CIS-00002"])       # invalid -> NULL, NOT rejected
        self.assertEqual(invalid.count(), 0)         # never an independent rejection
        self.assertEqual(counters["invalid_emails_nulled"], 1)

    def test_gref_invalid_nulled_not_rejected(self):
        rows = [{"cis_customer_id": "CIS-00001", "global_customer_ref": "GLOB-123"}]
        _, silver, invalid, _, counters = pipeline("customers", rows)
        self.assertEqual(silver.count(), 1)
        self.assertIsNone(silver.collect()[0]["global_customer_ref"])
        self.assertEqual(counters["invalid_refs_nulled"], 1)

    def test_phone_normalization_amendment_2(self):
        rows = [
            {"cis_customer_id": "CIS-00001", "phone": "+63 917 123 4567"},
            {"cis_customer_id": "CIS-00002", "phone": "(02) 8123 4567"},
            {"cis_customer_id": "CIS-00003", "phone": "0917-123-45"},   # 8 digits -> NULL
            {"cis_customer_id": "CIS-00004", "phone": None},             # nullable
        ]
        bronze, silver, invalid, _, counters = pipeline("customers", rows)
        phones = {r["cis_customer_id"]: r["phone"] for r in silver.collect()}
        self.assertEqual(phones["CIS-00001"], "9171234567")
        self.assertEqual(phones["CIS-00002"], "0281234567")
        self.assertIsNone(phones["CIS-00003"])       # <10 digits -> NULL
        self.assertIsNone(phones["CIS-00004"])
        self.assertEqual(invalid.count(), 0)           # never rejected for phone alone
        self.assertEqual(counters["invalid_phones_nulled"], 1)

    def test_identifier_normalization(self):
        rows = [{"cis_customer_id": " cis-12345 ", "global_customer_ref": " cust-012345 "}]
        _, silver, _, _, _ = pipeline("customers", rows)
        r = silver.collect()[0]
        self.assertEqual(r["cis_customer_id"], "CIS-12345")
        self.assertEqual(r["global_customer_ref"], "CUST-012345")

    def test_name_key_construction(self):
        _, silver, _, _, _ = pipeline("customers", [{}])
        self.assertEqual(silver.collect()[0]["name_key"], "JUAN~DELA")


class TestNumericCasting(unittest.TestCase):

    def test_decimal_cast_and_nullable(self):
        rows = [
            {"account_id": "ACC-00001", "balance": "1000.50"},
            {"account_id": "ACC-00002", "balance": None},   # nullable (A1)
            {"account_id": "ACC-00003", "balance": "-25.75"},  # overdraft valid
        ]
        _, silver, invalid, _, _ = pipeline("accounts", rows)
        self.assertEqual(silver.count(), 3)
        self.assertEqual(invalid.count(), 0)
        bal = {r["account_id"]: r["balance"] for r in silver.collect()}
        self.assertEqual(bal["ACC-00001"], Decimal("1000.50"))
        self.assertIsNone(bal["ACC-00002"])
        self.assertEqual(bal["ACC-00003"], Decimal("-25.75"))

    def test_unparseable_numeric_quarantined(self):
        rows = [{"account_id": "ACC-00001", "balance": "abc"}]
        _, silver, invalid, _, _ = pipeline("accounts", rows)
        self.assertEqual(silver.count(), 0)
        self.assertEqual({r["_rejection_reason"] for r in invalid.collect()},
                         {"NUMERIC_UNPARSEABLE"})

    def test_csat_decimal_notation_int_cast(self):
        rows = [{"ticket_id": "TKT-00001", "status": "RESOLVED",
                "resolved_date": "2026-02-05", "csat_score": "4.0"}]
        _, silver, invalid, _, _ = pipeline("support_tickets", rows)
        self.assertEqual(silver.count(), 1)
        self.assertEqual(silver.collect()[0]["csat_score"], 4)   # A5
        self.assertEqual(invalid.count(), 0)


class TestStrictIntegerCasting(unittest.TestCase):
    """Integer cast policy (review fix 5 + Decision 1/A8): term_months
    requires strict integer notation; csat_score (A5) and
    session_duration_minutes (A8) use the integral decimal-notation path
    ('150.0' -> 150; genuinely fractional values are never silently
    truncated)."""

    def test_fractional_term_months_rejected(self):
        rows = [dict(BASE["loans"], loan_id="LON-00001", term_months="36.5"),
                dict(BASE["loans"], loan_id="LON-00002", term_months="36.0"),
                dict(BASE["loans"], loan_id="LON-00003", term_months="36")]
        _, silver, invalid, _, _ = pipeline("loans", rows)
        self.assertEqual(silver.count(), 1)                       # only clean '36'
        self.assertEqual(silver.collect()[0]["term_months"], 36)
        self.assertEqual(invalid.count(), 2)                     # one reason row each
        self.assertEqual({r["_rejection_reason"] for r in invalid.collect()},
                         {"NUMERIC_UNPARSEABLE"})

    def test_fractional_session_duration_rejected(self):
        rows = [dict(BASE["digital_logins"], login_id="LOG-1000001",
                     session_duration_minutes="45.7")]
        _, silver, invalid, _, _ = pipeline("digital_logins", rows)
        self.assertEqual(silver.count(), 0)
        self.assertEqual({r["_rejection_reason"] for r in invalid.collect()},
                         {"SESSION_DURATION_INVALID"})

    def test_session_duration_decimal_notation_cast(self):
        # Decision 1 / A8: '150.0' -> 150, '65.0' -> 65, NULL -> NULL
        rows = [
            dict(BASE["digital_logins"], login_id="LOG-1000001",
                 login_status="SUCCESS", session_duration_minutes="150.0"),
            dict(BASE["digital_logins"], login_id="LOG-1000002",
                 login_status="SUCCESS", session_duration_minutes="65.0"),
            dict(BASE["digital_logins"], login_id="LOG-1000003",
                 login_status="FAILED", session_duration_minutes=None),
        ]
        _, silver, invalid, _, _ = pipeline("digital_logins", rows)
        self.assertEqual(silver.count(), 3)
        self.assertEqual(invalid.count(), 0)
        sd = {r["login_id"]: r["session_duration_minutes"] for r in silver.collect()}
        self.assertEqual(sd["LOG-1000001"], 150)
        self.assertEqual(sd["LOG-1000002"], 65)
        self.assertIsNone(sd["LOG-1000003"])       # NULL on FAILED stays valid

    def test_session_duration_genuinely_fractional_rejected(self):
        # Decision 1: '150.7' must NOT silently become 150
        rows = [dict(BASE["digital_logins"], login_id="LOG-1000001",
                     login_status="SUCCESS", session_duration_minutes="150.7")]
        _, silver, invalid, _, _ = pipeline("digital_logins", rows)
        self.assertEqual(silver.count(), 0)
        self.assertEqual({r["_rejection_reason"] for r in invalid.collect()},
                         {"SESSION_DURATION_INVALID"})

    def test_csat_genuinely_fractional_not_silently_truncated(self):
        # '4.5' is outside the approved A5 domain ('1.0'-'5.0') and must be
        # rejected, not silently truncated to 4
        rows = [dict(BASE["support_tickets"], ticket_id="TKT-00001",
                     status="RESOLVED", resolved_date="2026-02-05",
                     csat_score="4.5")]
        _, silver, invalid, _, _ = pipeline("support_tickets", rows)
        self.assertEqual(silver.count(), 0)
        self.assertEqual({r["_rejection_reason"] for r in invalid.collect()},
                         {"CSAT_INVALID"})

    def test_csat_keeps_approved_decimal_notation_path(self):
        # approved A5 handling: '4.0' -> 4 (int via decimal), NOT rejected
        rows = [dict(BASE["support_tickets"], ticket_id="TKT-00001",
                     status="RESOLVED", resolved_date="2026-02-05",
                     csat_score="4.0")]
        _, silver, invalid, _, _ = pipeline("support_tickets", rows)
        self.assertEqual(silver.count(), 1)
        self.assertEqual(silver.collect()[0]["csat_score"], 4)
        self.assertEqual(invalid.count(), 0)


class TestDeduplication(unittest.TestCase):

    def test_exact_duplicates(self):
        rows = [dict(BASE["customers"], cis_customer_id="CIS-00001"),
                dict(BASE["customers"], cis_customer_id="CIS-00001"),  # identical
                dict(BASE["customers"], cis_customer_id="CIS-00002")]
        _, silver, invalid, dups, counters = pipeline("customers", rows)
        self.assertEqual(silver.count(), 2)
        self.assertEqual(dups.count(), 1)
        d = dups.collect()[0]
        self.assertEqual(d["_dedup_reason"], "EXACT_DUPLICATE")
        self.assertEqual(d["_record_pk"], "CIS-00001")
        self.assertEqual(d["_survivor_pk"], "CIS-00001")
        self.assertEqual(counters["exact_duplicate_rows"], 1)
        self.assertEqual(counters["near_duplicate_rows"], 0)

    def test_near_duplicates(self):
        rows = [
            dict(BASE["cards"], card_id="CRD-00001"),
            dict(BASE["cards"], card_id="CRD-00001", outstanding_balance="2600.00"),
        ]
        _, silver, invalid, dups, counters = pipeline("cards", rows)
        self.assertEqual(silver.count(), 1)
        self.assertEqual(dups.count(), 1)
        self.assertEqual(dups.collect()[0]["_dedup_reason"], "NEAR_DUPLICATE")
        self.assertEqual(counters["near_duplicate_rows"], 1)

    def test_canonical_identical_raw_differing_classified_near(self):
        # email differs only by case -> canonically identical, raw differs -> NEAR
        rows = [
            dict(BASE["customers"], cis_customer_id="CIS-00001"),
            dict(BASE["customers"], cis_customer_id="CIS-00001",
                 email="JUAN.DELA@EXAMPLE.COM"),
        ]
        _, silver, invalid, dups, _ = pipeline("customers", rows)
        self.assertEqual(silver.count(), 1)
        self.assertEqual(silver.collect()[0]["email"], "juan.dela@example.com")
        self.assertEqual(dups.collect()[0]["_dedup_reason"], "NEAR_DUPLICATE")

    def test_survivor_fewest_nulls(self):
        rows = [
            dict(BASE["customers"], cis_customer_id="CIS-00001", email=None),
            dict(BASE["customers"], cis_customer_id="CIS-00001"),  # complete
        ]
        _, silver, _, dups, _ = pipeline("customers", rows)
        self.assertEqual(silver.count(), 1)
        self.assertEqual(silver.collect()[0]["email"], "juan.dela@example.com")

    def test_survivor_selection_deterministic_and_order_independent(self):
        # equal completeness -> lowest canonical hash; result must not depend
        # on input order (NO recency criterion anywhere -- Amendment 1)
        row_a = dict(BASE["customers"], cis_customer_id="CIS-00001", city="Manila")
        row_b = dict(BASE["customers"], cis_customer_id="CIS-00001", city="Cebu")
        _, silver1, _, _, _ = pipeline("customers", [row_a, row_b])
        _, silver2, _, _, _ = pipeline("customers", [row_b, row_a])
        self.assertEqual(silver1.collect()[0]["city"], silver2.collect()[0]["city"])

    def test_null_pk_rows_are_not_deduped_against_each_other(self):
        rows = [
            dict(BASE["customers"], cis_customer_id=None, first_name="A"),
            dict(BASE["customers"], cis_customer_id=None, first_name="B"),
        ]
        _, silver, invalid, dups, counters = pipeline("customers", rows)
        self.assertEqual(dups.count(), 0)          # distinct records, not dupes
        self.assertEqual(invalid.count(), 2)       # both quarantined PK_NULL
        self.assertEqual(counters["rejected_distinct_rows"], 2)
        self.assertEqual({r["_rejection_reason"] for r in invalid.collect()},
                         {"PK_NULL"})


class TestGroupWideClassification(unittest.TestCase):
    """Amendment 7 (approved 2026-11-24; literal semantics reaffirmed
    2026-11-24): exact/near classification is evaluated against ALL records
    in the PK group, not only the survivor. A loser is EXACT iff
    byte-identical to at least one OTHER record anywhere in the group --
    no ordering requirement; the first member of a byte-identical subset is
    NOT near when another identical record exists."""

    def test_byte_identical_losers_survivor_has_different_casing(self):
        # three canonically-identical rows; raw email casing differs on one.
        # The deterministic order selects the UPPERCASE-raw row as survivor
        # (lexicographically smaller). Both lowercase rows are losers and
        # each is byte-identical to the OTHER LOSER (another record in the
        # group) -> BOTH are EXACT under the literal A7 definition.
        # (vs-survivor classification would have called both NEAR.)
        rows = [dict(BASE["customers"], cis_customer_id="CIS-00001"),
                dict(BASE["customers"], cis_customer_id="CIS-00001"),
                dict(BASE["customers"], cis_customer_id="CIS-00001",
                     email="JUAN.DELA@EXAMPLE.COM")]
        _, silver, invalid, dups, counters = pipeline("customers", rows)
        self.assertEqual(silver.count(), 1)
        self.assertEqual(dups.count(), 2)
        self.assertEqual({r["_dedup_reason"] for r in dups.collect()},
                         {"EXACT_DUPLICATE"})
        self.assertEqual(counters["exact_duplicate_rows"], 2)
        self.assertEqual(counters["near_duplicate_rows"], 0)
        self.assertEqual(silver.collect()[0]["email"], "juan.dela@example.com")

    def test_three_byte_identical_records_in_group(self):
        # three fully identical rows: survivor + 2 losers, each loser
        # byte-identical to at least one other record -> 2 EXACT, 0 NEAR
        rows = [dict(BASE["customers"], cis_customer_id="CIS-00001"),
                dict(BASE["customers"], cis_customer_id="CIS-00001"),
                dict(BASE["customers"], cis_customer_id="CIS-00001")]
        _, silver, invalid, dups, counters = pipeline("customers", rows)
        self.assertEqual(silver.count(), 1)
        self.assertEqual(dups.count(), 2)
        self.assertEqual(counters["exact_duplicate_rows"], 2)
        self.assertEqual(counters["near_duplicate_rows"], 0)
        self.assertEqual(counters["bronze_rows"], 3)

    def test_two_identical_records_plus_one_distinct_variant(self):
        # two identical rows + one distinct variant that LOSES on
        # completeness (extra NULL): the survivor is a twin, the loser twin
        # is byte-identical to it -> EXACT; the distinct variant is not
        # identical to any record in the group -> NEAR
        rows = [dict(BASE["customers"], cis_customer_id="CIS-00001"),
                dict(BASE["customers"], cis_customer_id="CIS-00001"),
                dict(BASE["customers"], cis_customer_id="CIS-00001",
                     city="Cebu", middle_name=None)]
        _, silver, invalid, dups, counters = pipeline("customers", rows)
        self.assertEqual(silver.count(), 1)
        self.assertEqual(dups.count(), 2)
        self.assertEqual(counters["exact_duplicate_rows"], 1)
        self.assertEqual(counters["near_duplicate_rows"], 1)

    def test_group_with_multiple_byte_identical_subsets(self):
        # two byte-identical subsets: {A, A} (Manila) and {B1, B1} (Cebu).
        # One record survives; all three losers are byte-identical to
        # another record in the group -> 3 EXACT, 0 NEAR
        rows = [dict(BASE["customers"], cis_customer_id="CIS-00001", city="Manila"),
                dict(BASE["customers"], cis_customer_id="CIS-00001", city="Manila"),
                dict(BASE["customers"], cis_customer_id="CIS-00001", city="Cebu"),
                dict(BASE["customers"], cis_customer_id="CIS-00001", city="Cebu")]
        _, silver, invalid, dups, counters = pipeline("customers", rows)
        self.assertEqual(silver.count(), 1)
        self.assertEqual(dups.count(), 3)
        self.assertEqual(counters["exact_duplicate_rows"], 3)
        self.assertEqual(counters["near_duplicate_rows"], 0)
        self.assertEqual(counters["bronze_rows"], 4)          # identity

    def test_multiple_byte_identical_copies_in_one_group(self):
        # three identical rows + one distinct variant: 2 EXACT (redundant
        # repeats) + 1 NEAR, regardless of which record survives
        rows = [dict(BASE["cards"], card_id="CRD-00001"),
                dict(BASE["cards"], card_id="CRD-00001"),
                dict(BASE["cards"], card_id="CRD-00001"),
                dict(BASE["cards"], card_id="CRD-00001",
                     outstanding_balance="99.00")]
        _, silver, invalid, dups, counters = pipeline("cards", rows)
        self.assertEqual(silver.count(), 1)
        self.assertEqual(dups.count(), 3)
        self.assertEqual(counters["exact_duplicate_rows"], 2)
        self.assertEqual(counters["near_duplicate_rows"], 1)
        self.assertEqual(counters["bronze_rows"], 4)          # identity

    def test_near_duplicates_differing_from_every_record(self):
        # three distinct byte-sets: both losers are unique in the group
        rows = [dict(BASE["customers"], cis_customer_id="CIS-00001", city="Manila"),
                dict(BASE["customers"], cis_customer_id="CIS-00001", city="Cebu"),
                dict(BASE["customers"], cis_customer_id="CIS-00001", city="Davao")]
        _, silver, invalid, dups, counters = pipeline("customers", rows)
        self.assertEqual(silver.count(), 1)
        self.assertEqual(dups.count(), 2)
        self.assertEqual(counters["exact_duplicate_rows"], 0)
        self.assertEqual(counters["near_duplicate_rows"], 2)

    def test_group_with_both_exact_and_near_survivor_is_twin(self):
        # survivor = one of the identical twins (most complete); the other
        # twin is EXACT (identical to the survivor AND to another record);
        # the distinct variant is NEAR -> 1/1
        rows = [dict(BASE["customers"], cis_customer_id="CIS-00001"),
                dict(BASE["customers"], cis_customer_id="CIS-00001"),
                dict(BASE["customers"], cis_customer_id="CIS-00001",
                     middle_name=None)]
        _, silver, invalid, dups, counters = pipeline("customers", rows)
        self.assertEqual(silver.count(), 1)
        self.assertEqual(dups.count(), 2)
        self.assertEqual(counters["exact_duplicate_rows"], 1)
        self.assertEqual(counters["near_duplicate_rows"], 1)

    def test_classification_deterministic_across_input_orders(self):
        rows_a = [dict(BASE["customers"], cis_customer_id="CIS-00001"),
                  dict(BASE["customers"], cis_customer_id="CIS-00001"),
                  dict(BASE["customers"], cis_customer_id="CIS-00001",
                       email="JUAN.DELA@EXAMPLE.COM")]
        rows_b = list(reversed(rows_a))
        _, _, _, dups_a, c_a = pipeline("customers", rows_a)
        _, _, _, dups_b, c_b = pipeline("customers", rows_b)
        self.assertEqual((c_a["exact_duplicate_rows"], c_a["near_duplicate_rows"]),
                         (c_b["exact_duplicate_rows"], c_b["near_duplicate_rows"]))
        self.assertEqual(sorted(r["_dedup_reason"] for r in dups_a.collect()),
                         sorted(r["_dedup_reason"] for r in dups_b.collect()))


class TestNullPkIndependence(unittest.TestCase):
    """Review fix 2: records with a NULL/empty PK are never deduplicated --
    byte-identical null-PK rows must each independently reach PK_NULL."""

    def test_identical_null_pk_records_stay_independent(self):
        rows = [dict(BASE["customers"], cis_customer_id=None),
                dict(BASE["customers"], cis_customer_id=None)]  # byte-identical
        _, silver, invalid, dups, counters = pipeline("customers", rows)
        self.assertEqual(dups.count(), 0)               # never deduplicated
        self.assertEqual(invalid.count(), 2)            # each independently PK_NULL
        self.assertEqual(counters["rejected_distinct_rows"], 2)  # identity holds
        self.assertEqual(silver.count(), 0)

    def test_empty_pk_records_stay_independent(self):
        rows = [dict(BASE["customers"], cis_customer_id=""),
                dict(BASE["customers"], cis_customer_id="")]
        _, silver, invalid, dups, counters = pipeline("customers", rows)
        self.assertEqual(dups.count(), 0)
        self.assertEqual(counters["rejected_distinct_rows"], 2)
        self.assertEqual({r["_rejection_reason"] for r in invalid.collect()},
                         {"PK_NULL"})


class TestUnambiguousSerialization(unittest.TestCase):
    """Review fix 4: NULL != empty; values containing the old '||'
    delimiter must not collide with other columns' boundaries."""

    def test_null_vs_empty_not_exact_duplicate(self):
        rows = [dict(BASE["customers"], cis_customer_id="CIS-00001", middle_name=None),
                dict(BASE["customers"], cis_customer_id="CIS-00001", middle_name="")]
        _, silver, invalid, dups, _ = pipeline("customers", rows)
        self.assertEqual(silver.count(), 1)
        self.assertEqual(dups.count(), 1)
        self.assertEqual(dups.collect()[0]["_dedup_reason"], "NEAR_DUPLICATE")

    def test_delimiter_collision_not_exact_duplicate(self):
        # under the old '||' concat_ws encoding these two rows serialized
        # identically ('a||b||c') and were wrongly called EXACT duplicates;
        # the length-prefixed encoding keeps them distinct -> NEAR
        rows = [dict(BASE["customers"], cis_customer_id="CIS-00001",
                     address_line1="a||b", city="c"),
                dict(BASE["customers"], cis_customer_id="CIS-00001",
                     address_line1="a", city="b||c")]
        _, silver, invalid, dups, _ = pipeline("customers", rows)
        self.assertEqual(silver.count(), 1)
        self.assertEqual(dups.count(), 1)
        self.assertEqual(dups.collect()[0]["_dedup_reason"], "NEAR_DUPLICATE")

    def test_colon_delimiter_values_do_not_collide(self):
        rows = [dict(BASE["customers"], cis_customer_id="CIS-00001",
                     address_line1="1:2", city="3"),
                dict(BASE["customers"], cis_customer_id="CIS-00001",
                     address_line1="1", city="2:3")]
        _, silver, invalid, dups, _ = pipeline("customers", rows)
        self.assertEqual(dups.collect()[0]["_dedup_reason"], "NEAR_DUPLICATE")


class TestValidation(unittest.TestCase):

    def test_multiple_rejection_reasons_single_record(self):
        rows = [{"cis_customer_id": "CIS-00001", "gender": "X",
                 "date_of_birth": "15.01.1990"}]
        _, silver, invalid, dups, counters = pipeline("customers", rows)
        self.assertEqual(invalid.count(), 2)             # one row per reason
        reasons = {r["_rejection_reason"] for r in invalid.collect()}
        self.assertEqual(reasons, {"ENUM_VIOLATION", "INVALID_DATE_FORMAT"})
        self.assertEqual(counters["rejected_distinct_rows"], 1)   # counted once
        self.assertEqual(counters["rejection_reason_rows"], 2)

    def test_required_field_null(self):
        rows = [{"cis_customer_id": "CIS-00001", "first_name": None}]
        _, silver, invalid, _, _ = pipeline("customers", rows)
        self.assertEqual({r["_rejection_reason"] for r in invalid.collect()},
                         {"REQUIRED_FIELD_NULL"})

    def test_pk_invalid_format(self):
        rows = [{"cis_customer_id": "CIS-abcde"}]
        _, _, invalid, _, _ = pipeline("customers", rows)
        self.assertEqual({r["_rejection_reason"] for r in invalid.collect()},
                         {"PK_INVALID_FORMAT"})

    def test_dob_after_registration(self):
        rows = [{"cis_customer_id": "CIS-00001", "date_of_birth": "2020-01-01",
                 "registration_date": "2015-06-01"}]
        _, _, invalid, _, _ = pipeline("customers", rows)
        self.assertEqual({r["_rejection_reason"] for r in invalid.collect()},
                         {"DOB_AFTER_REGISTRATION"})

    def test_all_quarantine_rule_ids_covered(self):
        cases = {
            "customers": [
                ({"cis_customer_id": None}, "PK_NULL"),
                ({"cis_customer_id": "CIS-xxxx"}, "PK_INVALID_FORMAT"),
                ({"first_name": None}, "REQUIRED_FIELD_NULL"),
                ({"date_of_birth": "15.01.1990"}, "INVALID_DATE_FORMAT"),
                ({"gender": "X"}, "ENUM_VIOLATION"),
                ({"first_name": "Juan123"}, "NAME_INVALID"),
                ({"postal_code": "AB1"}, "POSTAL_INVALID"),
                ({"annual_income": "99999999.00"}, "INCOME_OUT_OF_RANGE"),
                ({"date_of_birth": "2020-01-01"}, "DOB_AFTER_REGISTRATION"),
            ],
            "accounts": [
                ({"cb_customer_ref": "CBX-123"}, "NATIVE_REF_INVALID_FORMAT"),
                ({"account_number": "12345"}, "ACCOUNT_NUMBER_INVALID"),
                ({"branch_code": "BRANCHX"}, "BRANCH_CODE_INVALID"),
                ({"balance": "abc"}, "NUMERIC_UNPARSEABLE"),
                ({"currency": "EUR"}, "ENUM_VIOLATION"),
                ({"open_date": "2021-01-10",
                  "last_activity_date": "2019-01-01"}, "LAST_ACTIVITY_BEFORE_OPEN"),
            ],
            "cards": [
                ({"card_number_masked": "1234-5678-9012-1234"}, "MASKED_PAN_INVALID"),
                ({"expiry_date": "2022-01-01"}, "EXPIRY_NOT_AFTER_ISSUE"),
                ({"credit_limit": "0.00"}, "AMOUNT_INVALID"),
                ({"card_type": "DIAMOND"}, "ENUM_VIOLATION"),
            ],
            "loans": [
                ({"interest_rate": "30.00"}, "RATE_OUT_OF_RANGE"),
                ({"term_months": "400"}, "TERM_OUT_OF_RANGE"),
                ({"principal_amount": "0.00"}, "AMOUNT_INVALID"),
                ({"end_date": "2023-01-01"}, "END_BEFORE_START"),
                ({"repayment_status": "PAID_OFF", "end_date": None},
                   "END_DATE_REQUIRED"),
                ({"loan_type": "SACRED"}, "ENUM_VIOLATION"),
            ],
            "digital_logins": [
                ({"login_timestamp": "2026/01/15 09:30"}, "INVALID_TIMESTAMP_FORMAT"),
                ({"session_duration_minutes": "-5"}, "SESSION_DURATION_INVALID"),
                ({"login_status": "FAILED",
                  "session_duration_minutes": "10"}, "SESSION_DURATION_INVALID"),
                ({"login_status": "SUCCESS",
                  "session_duration_minutes": None}, "SESSION_DURATION_INVALID"),
                ({"channel": "TELEPATHY"}, "ENUM_VIOLATION"),
                ({"db_customer_ref": "DB-123456"}, "NATIVE_REF_INVALID_FORMAT"),
                ({"login_id": "LOG-123"}, "PK_INVALID_FORMAT"),
            ],
            "support_tickets": [
                ({"status": "RESOLVED", "resolved_date": "2026-01-01",
                  "created_date": "2026-02-01"}, "RESOLVED_BEFORE_CREATED"),
                ({"status": "RESOLVED", "resolved_date": None,
                  "csat_score": "4.0"}, "RESOLVED_DATE_REQUIRED"),
                ({"status": "OPEN", "csat_score": "4.0"}, "CSAT_INVALID"),
                ({"status": "RESOLVED", "resolved_date": "2026-02-05",
                  "csat_score": "6.0"}, "CSAT_INVALID"),
                ({"status": "RESOLVED", "resolved_date": "2026-02-05",
                  "csat_score": None}, "CSAT_INVALID"),
                ({"priority": "URGENT"}, "ENUM_VIOLATION"),
            ],
        }
        seen = set()
        # unique VALID pk series per entity so case rows never collide in
        # dedup (digit counts must match each pk_regex)
        uniq_fmt = {"customers": "CIS-{:05d}", "accounts": "ACC-{:05d}",
                    "cards": "CRD-{:05d}", "loans": "LON-{:05d}",
                    "digital_logins": "LOG-{:07d}", "support_tickets": "TKT-{:05d}"}
        for entity, case_rows in cases.items():
            rows = [dict(BASE[entity], **overrides) for overrides, _ in case_rows]
            pk = cfg.ENTITIES[entity]["pk"]
            for i, (r, (overrides, _)) in enumerate(zip(rows, case_rows)):
                if pk not in overrides:   # cases that target the PK itself keep it
                    r[pk] = uniq_fmt[entity].format(10000 + i)
            _, silver, invalid, dups, _ = pipeline(entity, rows)
            produced = {r["_rejection_reason"] for r in invalid.collect()}
            expected = {rule for _, rule in case_rows}
            self.assertEqual(produced, expected,
                             f"{entity}: produced {produced} != expected {expected}")
            seen |= produced
        missing = set(cfg.RULE_IDS) - seen
        self.assertEqual(missing, set(),
                         f"rule IDs never exercised: {missing}")


class TestReconciliation(unittest.TestCase):

    def test_distinct_record_reconciliation_identity(self):
        # the near-dupe row also carries a NULL so the survivor outcome is
        # forced by precedence criterion 1 (fewest nulls), NOT by the hash --
        # the hash order legitimately changed with the remediated inputs
        rows = [
            dict(BASE["customers"], cis_customer_id="CIS-00001"),            # survivor
            dict(BASE["customers"], cis_customer_id="CIS-00001"),            # exact dupe
            dict(BASE["customers"], cis_customer_id="CIS-00001", city="Cebu",
                 email=None),                                               # near
            dict(BASE["customers"], cis_customer_id="CIS-00002",
                 gender="X", date_of_birth="15.01.1990"),  # invalid, 2 reasons
        ]
        bronze, silver, invalid, dups, c = pipeline("customers", rows)
        accounted = (c["silver_rows"] + c["exact_duplicate_rows"]
                     + c["near_duplicate_rows"] + c["rejected_distinct_rows"])
        self.assertEqual(accounted, c["bronze_rows"])       # Amendment 3
        self.assertEqual(c["bronze_rows"], 4)
        self.assertEqual(c["silver_rows"], 1)
        self.assertEqual(c["exact_duplicate_rows"], 1)
        self.assertEqual(c["near_duplicate_rows"], 1)
        self.assertEqual(c["rejected_distinct_rows"], 1)
        self.assertEqual(c["rejection_reason_rows"], 2)

    def test_reconciliation_multiple_invalid_records_and_reasons(self):
        # two invalid records (one with two reasons, one with one) + duplicates
        rows = [
            dict(BASE["customers"], cis_customer_id="CIS-00001"),            # survivor
            dict(BASE["customers"], cis_customer_id="CIS-00001"),            # exact dupe
            dict(BASE["customers"], cis_customer_id="CIS-00001", city="Cebu",
                 email=None),                                               # near
            dict(BASE["customers"], cis_customer_id="CIS-00002",
                 gender="X", date_of_birth="15.01.1990"),   # invalid, 2 reasons
            dict(BASE["customers"], cis_customer_id="CIS-00003", postal_code="AB1"),  # 1
        ]
        bronze, silver, invalid, dups, c = pipeline("customers", rows)
        self.assertEqual(c["bronze_rows"], 5)
        self.assertEqual(c["silver_rows"], 1)
        self.assertEqual(c["exact_duplicate_rows"], 1)
        self.assertEqual(c["near_duplicate_rows"], 1)
        self.assertEqual(c["rejected_distinct_rows"], 2)       # records, not reasons
        self.assertEqual(c["rejection_reason_rows"], 3)        # 2 + 1 reason rows
        self.assertEqual(c["silver_rows"] + c["exact_duplicate_rows"]
                         + c["near_duplicate_rows"] + c["rejected_distinct_rows"],
                         c["bronze_rows"])                     # Amendment 3


class TestIdempotencyGuard(unittest.TestCase):

    def test_guard_detects_existing_batch(self):
        spark.createDataFrame([Row(_batch_id="B1", x=1)]) \
            .createOrReplaceTempView("ut_guard_hit")
        hits = tf.batch_already_present(spark, "B1",
                                        custom_targets=["ut_guard_hit"])
        self.assertEqual(hits, [("ut_guard_hit", "_batch_id", 1)])

    def test_guard_detects_batch_in_processing_audit(self):
        # review fix 3: the audit table identifies batches by `batch_id`
        spark.createDataFrame([Row(batch_id="B1", table_name="customers")]) \
            .createOrReplaceTempView("ut_guard_audit")
        hits = tf.batch_already_present(spark, "B1",
                                        custom_targets=[("ut_guard_audit", "batch_id")])
        self.assertEqual(hits, [("ut_guard_audit", "batch_id", 1)])

    def test_default_guard_targets_use_correct_identifier_columns(self):
        targets = dict(tf._default_guard_targets())
        self.assertEqual(targets[cfg.AUDIT_TABLE], "batch_id")   # not _batch_id
        for t in cfg.ENTITY_ORDER:
            self.assertEqual(targets[cfg.silver_fqn(t)], "_batch_id")
            self.assertEqual(targets[cfg.invalid_fqn(t)], "_batch_id")
        self.assertEqual(targets[cfg.DUPLICATE_TABLE], "_batch_id")
        self.assertEqual(len(targets), 14)                      # 6+6+2 targets

    def test_guard_allows_new_batch(self):
        spark.createDataFrame([Row(_batch_id="B1", x=1)]) \
            .createOrReplaceTempView("ut_guard_miss")
        self.assertEqual(tf.batch_already_present(spark, "B2",
                                                  custom_targets=["ut_guard_miss"]), [])
        # a missing table is not an error (first run: tables do not exist yet)
        self.assertEqual(tf.batch_already_present(spark, "B1",
                                                  custom_targets=["ut_no_such_table"]), [])


class TestSpecInvariants(unittest.TestCase):

    def test_expected_config_matches_spec(self):
        for entity, exp in cfg.EXPECTED_BATCH_001.items():
            self.assertEqual(exp["bronze_rows"],
                             exp["silver_rows"] + exp["exact_dup"]
                             + exp["near_dup"] + exp["rejected_distinct"],
                             f"{entity}: spec expectations violate the identity")

    def test_lineage_and_processing_timestamp_on_silver_rows(self):
        _, silver, _, _, _ = pipeline("customers", [dict(BASE["customers"])])
        cols = set(silver.columns)
        for c in cfg.LINEAGE_COLUMNS + [cfg.SILVER_TS_COL]:
            self.assertIn(c, cols)
        r = silver.collect()[0]
        self.assertEqual(r["_batch_id"], BATCH)
        self.assertEqual(r["_source_system"], "unit_test")
        self.assertEqual(r[cfg.SILVER_TS_COL], RUN_TS)

    def test_silver_column_order_matches_spec(self):
        _, silver, _, _, _ = pipeline("customers", [dict(BASE["customers"])])
        expected = [n for n, _ in cfg.TYPED_COLUMNS["customers"]] \
            + cfg.LINEAGE_COLUMNS + [cfg.SILVER_TS_COL]
        self.assertEqual(silver.columns, expected)

    def test_no_unified_customer_id_no_identity_resolution(self):
        for entity in cfg.ENTITY_ORDER:
            _, silver, _, _, _ = pipeline(entity, [dict(BASE[entity])])
            self.assertNotIn("unified_customer_id", silver.columns)


if __name__ == "__main__":
    unittest.main(verbosity=2)
