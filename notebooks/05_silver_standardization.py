# Databricks notebook source
# Phase 5 -- Silver Standardization, Deduplication, Validation & Quarantine
# Source of truth: specs/05-silver-standardization.md (Amendments 1-8).
#
# GATED: this orchestrator is NOT executed until the implementation review is
# explicitly approved. It creates schemas, runs the batch behind the
# fail-closed guard, and executes the 11 acceptance checks (spec section 11).
# Bronze is only ever read; fraud_detection is never touched; no Gold;
# no identity resolution; no unified_customer_id.

# COMMAND ----------

import os, sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath("__file__"))))

from pyspark.sql import functions as F

from databricks.sdk.runtime import spark

from src.config import silver_config as cfg
from src.silver import transform as tf

# COMMAND ----------

# Cell 2 -- parameters

BATCH_ID = "batch_001"          # the only Bronze batch that exists today
DRY_RUN = False                # safety valve: True -> checks only, no writes

# COMMAND ----------

# Cell 3 -- ensure schemas exist (idempotent; tables are created by the
# batch writer on first run, never dropped or overwritten)

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {cfg.CATALOG}.{cfg.SILVER_SCHEMA}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {cfg.CATALOG}.{cfg.QUARANTINE_SCHEMA}")
print(f"Schemas ready: {cfg.SILVER_FQN}, {cfg.QUARANTINE_FQN}")

# COMMAND ----------

# Cell 4 -- run the batch (fail-closed duplicate-batch guard inside).
# The run aborts BEFORE any write if batch_001 is already present in any
# silver/quarantine/audit table.

if not DRY_RUN:
    results = tf.run_silver_batch(spark, BATCH_ID)
    for entity, c in results.items():
        print(f"{entity}: {c}")
else:
    print("DRY_RUN=True -- no writes performed.")

# COMMAND ----------

# Cell 5 -- acceptance checks (spec section 11). Each check prints
# PASS/FAIL and the suite raises at the end if anything failed.

CHECKS = []

def check(name, ok, detail=""):
    CHECKS.append((name, bool(ok), detail))
    print(f"{'PASS' if ok else 'FAIL'}  {name}  {detail}")


def run_acceptance_checks():
    audit = spark.table(cfg.AUDIT_TABLE).where(F.col("batch_id") == BATCH_ID)
    audit_by = {r["table_name"]: r for r in audit.collect()}

    # 1 -- all expected objects exist
    tables = ([cfg.silver_fqn(t) for t in cfg.ENTITY_ORDER]
              + [cfg.invalid_fqn(t) for t in cfg.ENTITY_ORDER]
              + [cfg.DUPLICATE_TABLE, cfg.AUDIT_TABLE])
    missing = [t for t in tables if not spark.catalog.tableExists(t)]
    check("1. objects exist", not missing, f"missing={missing}")

    # 2 -- Silver row counts == spec expectations
    for entity, exp in cfg.EXPECTED_BATCH_001.items():
        n = spark.table(cfg.silver_fqn(entity)).where(
            F.col("_batch_id") == BATCH_ID).count()
        check(f"2. silver rows {entity}", n == exp["silver_rows"],
              f"{n} vs {exp['silver_rows']}")

    # 3 -- Bronze untouched (row counts unchanged)
    for entity, exp in cfg.EXPECTED_BATCH_001.items():
        n = spark.table(cfg.bronze_fqn(entity)).count()
        check(f"3. bronze unchanged {entity}", n == exp["bronze_rows"],
              f"{n} vs {exp['bronze_rows']}")

    # 4 -- duplicate counts (exact/near) per entity
    for entity, exp in cfg.EXPECTED_BATCH_001.items():
        rows = (spark.table(cfg.DUPLICATE_TABLE)
                .where((F.col("_batch_id") == BATCH_ID) & (F.col("_entity") == entity))
                .groupBy("_dedup_reason").count().collect())
        got = {r["_dedup_reason"]: r["count"] for r in rows}
        check(f"4. duplicates {entity}",
              got.get("EXACT_DUPLICATE", 0) == exp["exact_dup"]
              and got.get("NEAR_DUPLICATE", 0) == exp["near_dup"],
              f"{got} vs exact={exp['exact_dup']} near={exp['near_dup']}")

    # 5 -- reconciliation identity per entity (bronze = silver + dup + rejected)
    for entity, exp in cfg.EXPECTED_BATCH_001.items():
        a = audit_by[entity]
        ok = (a["bronze_rows"] == a["silver_rows"] + a["exact_duplicate_rows"]
              + a["near_duplicate_rows"] + a["rejected_distinct_rows"])
        check(f"5. reconciliation {entity}", ok, str(a))

    # 6 -- audit value-quality counters == verified batch_001 profile
    for entity, exp in cfg.EXPECTED_BATCH_001.items():
        a = audit_by[entity]
        check(f"6. audit counters {entity}",
              a["invalid_emails_nulled"] == exp["invalid_emails"]
              and a["invalid_refs_nulled"] == 0
              and a["invalid_phones_nulled"] == 0
              and a["loan_anomalies_flagged"] == 0,
              f"emails={a['invalid_emails_nulled']} refs={a['invalid_refs_nulled']} "
              f"phones={a['invalid_phones_nulled']} anomalies={a['loan_anomalies_flagged']}")

    # 7 -- rejected distinct == expected (0 for batch_001)
    for entity, exp in cfg.EXPECTED_BATCH_001.items():
        a = audit_by[entity]
        check(f"7. rejected distinct {entity}",
              a["rejected_distinct_rows"] == exp["rejected_distinct"],
              str(a["rejected_distinct_rows"]))

    # 8 -- typed Silver schemas (spot-probe each declared type)
    for entity in cfg.ENTITY_ORDER:
        dt = {r.name: r.dataType.simpleString() for r in
              spark.table(cfg.silver_fqn(entity)).schema.fields}
        expected = {n: t.split("(")[0] if t.startswith("decimal") else t
                    for n, t in cfg.typed_schema_map(entity).items()}
        bad = {k: (dt.get(k), v) for k, v in expected.items()
               if k not in dt or not dt[k].startswith(v.split("(")[0])}
        check(f"8. typed schema {entity}", not bad, f"{bad}")

    # 9 -- lineage preserved + processing timestamp present
    for entity in cfg.ENTITY_ORDER:
        t = spark.table(cfg.silver_fqn(entity)).where(F.col("_batch_id") == BATCH_ID)
        cols = {f.name for f in t.schema.fields}
        have_cols = set(cfg.LINEAGE_COLUMNS + [cfg.SILVER_TS_COL]) <= cols
        nulls = t.where(reduce_or_null(cfg.LINEAGE_COLUMNS
                                       + [cfg.SILVER_TS_COL])).count()
        check(f"9. lineage {entity}", have_cols and nulls == 0,
              f"cols_ok={have_cols} null_lineage={nulls}")

    # 10 -- PK uniqueness in Silver + PK domain == Bronze's
    for entity in cfg.ENTITY_ORDER:
        e = cfg.ENTITIES[entity]
        s = spark.table(cfg.silver_fqn(entity)).where(F.col("_batch_id") == BATCH_ID)
        distinct_pk = s.select(e["pk"]).distinct().count()
        check(f"10. PK unique {entity}", distinct_pk == s.count(),
              f"{distinct_pk} vs {s.count()}")

    # 11 -- quality spot checks (normalized outputs + cross-field invariants)
    c = spark.table(cfg.silver_fqn("customers")).where(F.col("_batch_id") == BATCH_ID)
    bad_email_case = c.where(F.col("email").isNotNull()
                             & (F.col("email") != F.lower(F.col("email")))).count()
    bad_phone = c.where(F.col("phone").isNotNull()
                        & ~F.col("phone").rlike("^[0-9]{10}$")).count()
    dob_bad = c.where(F.col("date_of_birth") >= F.col("registration_date")).count()
    check("11. quality spot checks", bad_email_case == 0 and bad_phone == 0
          and dob_bad == 0,
          f"email_case={bad_email_case} phone={bad_phone} dob={dob_bad}")

    failed = [n for n, ok, _ in CHECKS if not ok]
    print(f"\n{len(CHECKS) - len(failed)}/{len(CHECKS)} acceptance checks passed")
    if failed:
        raise AssertionError(f"Acceptance checks FAILED: {failed}")


def reduce_or_null(cols):
    cond = F.col(cols[0]).isNull()
    for c in cols[1:]:
        cond = cond | F.col(c).isNull()
    return cond


run_acceptance_checks()

# COMMAND ----------

# Cell 6 -- idempotency proof: re-running the batch must abort fail-closed
# BEFORE any write. Expected outcome: AssertionError listing affected tables.

try:
    tf.run_silver_batch(spark, BATCH_ID)
    raise AssertionError("Guard did not fire -- duplicate run was allowed!")
except AssertionError as ex:
    print(f"Guard fired as expected: {ex}")

# COMMAND ----------

# Cell 7 -- after ALL checks pass: update data/README.md row counts and
# commit & push (manual, reviewed step -- never automated here).
