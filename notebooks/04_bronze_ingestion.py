"""Phase 4 orchestrator -- Bronze ingestion (spec 04-bronze-ingestion.md).

Thin, per project convention: creates ONLY the approved schema
(customer_360.bronze), runs the loader (which performs every pre-check and
the duplicate-batch guard before any write), then runs the seven acceptance
checks from spec section 8 and prints the run report.

No Silver/Gold/Quarantine tables. Source CSVs are read-only (acceptance check
6 re-hashes them against the manifest). fraud_detection is never touched.
"""
import json
import sys
from pathlib import Path

try:
    REPO_ROOT = Path(__file__).resolve().parents[1]
except NameError:
    REPO_ROOT = Path("/Workspace/Users/sia.jamir23@gmail.com/customer-360-banking")
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from databricks.sdk.runtime import spark

from src.config import bronze_config as cfg
from src.ingestion import bronze_loader as loader


def ensure_schema():
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {cfg.SCHEMA_FQN}")


def acceptance_checks(batch_id):
    """Spec section 8: all seven criteria, fail-closed. Returns checks list."""
    from pyspark.sql import functions as F
    manifest = loader.load_manifest()
    checks = []

    def check(num, name, ok, detail=""):
        checks.append({"check": f"{num} {name}", "passed": bool(ok), "detail": detail})

    # 1. exactly six tables, all Delta, managed locations recorded
    tables = sorted(
        r["tableName"] for r in spark.sql(
            f"SHOW TABLES IN {cfg.SCHEMA_FQN}").collect())
    check(1, "six Delta tables exist",
          tables == sorted(cfg.SOURCES), f"tables: {tables}")
    locations = {}
    for table in cfg.SOURCES:
        fqn = cfg.table_fqn(table)
        detail = spark.sql(f"DESCRIBE DETAIL {fqn}").collect()[0].asDict()
        is_delta = detail.get("format", "delta") == "delta"
        locations[table] = detail.get("location")
        check(1, f"{table} is Delta", is_delta,
              f"location: {detail.get('location')}")

    # 2. schema exact: names, order, types (business STRING + 5 metadata)
    expected_meta_types = dict(cfg.METADATA_COLUMNS)
    for table in cfg.SOURCES:
        fqn = cfg.table_fqn(table)
        schema = spark.table(fqn).schema
        expected = cfg.BUSINESS_COLUMNS[table] + list(expected_meta_types)
        actual = [f.name for f in schema.fields]
        order_ok = actual == expected
        # business columns must be STRING; metadata columns must match their
        # contracted types (three of them are also STRING -- that is expected)
        types_ok = (
            all(f.dataType.simpleString() == "string"
                for f in schema.fields
                if f.name in cfg.BUSINESS_COLUMNS[table])
            and all(f.dataType.simpleString() == expected_meta_types[f.name]
                    for f in schema.fields
                    if f.name in expected_meta_types))
        check(2, f"schema exact {table}",
              order_ok and types_ok,
              f"{len(actual)} cols, order={order_ok}, types={types_ok}")

    # 3. row counts == manifest
    for table in cfg.SOURCES:
        key = "support_cases" if table == "support_tickets" else table
        expected = manifest["row_counts"][key]
        n = spark.table(cfg.table_fqn(table)).count()
        check(3, f"row count {table}", n == expected,
              f"{n} rows (manifest {expected})")

    # 4. metadata completeness
    for table in cfg.SOURCES:
        df = spark.table(cfg.table_fqn(table))
        batches = [r[0] for r in df.select("_batch_id").distinct().collect()]
        one_batch = batches == [batch_id]
        pop = df.agg(
            F.count(F.when(
                df["_source_system"].isNull()
                | df["_batch_id"].isNull()
                | df["_ingestion_timestamp"].isNull()
                | df["_ingestion_date"].isNull()
                | df["_source_file"].isNull(), 1)).alias("missing"),
            F.countDistinct("_source_system").alias("src_sys"),
            F.countDistinct("_source_file").alias("src_file"),
            F.count(
                F.when(F.to_date("_ingestion_timestamp")
                       != F.col("_ingestion_date"), 1)).alias("ts_date_mismatch"),
        ).collect()[0]
        ok = (one_batch and pop["missing"] == 0 and pop["src_sys"] == 1
              and pop["src_file"] == 1 and pop["ts_date_mismatch"] == 0)
        check(4, f"metadata complete {table}", ok,
              f"batch={batches}, missing_meta={pop['missing']}, "
              f"ts_date_mismatch={pop['ts_date_mismatch']}")

    # 5. fidelity: full-row duplicates equal source-file duplicates;
    #    spot-check sample rows byte-equal on all business columns
    for table in cfg.SOURCES:
        filename, _ = cfg.SOURCES[table]
        file_uri = f"{cfg.batch_dir(batch_id)}/{filename}"
        src = (spark.read.option("header", True)
               .schema(_business_schema(table)).csv(file_uri))
        src_dups = src.count() - src.distinct().count()
        brz = spark.table(cfg.table_fqn(table))
        brz_dups = brz.count() - brz.select(
            *cfg.BUSINESS_COLUMNS[table]).distinct().count()
        dup_ok = src_dups == brz_dups

        cols = cfg.BUSINESS_COLUMNS[table]
        sample_bronze = [tuple(str(v) for v in list(r)) for r in brz.select(
            *cols).orderBy(cols[0]).limit(3).collect()]
        sample_src = [tuple(str(v) for v in list(r)) for r in src.orderBy(
            cols[0]).limit(3).collect()]
        spot_ok = sample_bronze == sample_src
        check(5, f"fidelity {table}", dup_ok and spot_ok,
              f"dupes bronze={brz_dups} vs source={src_dups}; "
              f"spot-check 3 rows byte-equal={spot_ok}")

    # 6. source CSVs untouched (SHA-256 vs manifest)
    sha_ok = True
    sha_detail = []
    for key, digest in manifest["sha256"].items():
        table = "support_tickets" if key == "support_cases" else key
        filename, _ = cfg.SOURCES[table]
        actual = loader.sha256_file(f"{cfg.batch_dir(batch_id)}/{filename}")
        match = actual == digest
        sha_ok &= match
        sha_detail.append(f"{key}: {'ok' if match else 'CHANGED'}")
    check(6, "source CSVs untouched (SHA-256)", sha_ok, ", ".join(sha_detail))

    # 7. idempotency negative test: a re-ingest attempt must abort, write nothing
    try:
        loader.run_ingestion(spark, batch_id)
        reingest_ok, note = False, "re-ingest DID NOT abort"
    except AssertionError as e:
        already = any(cfg.table_fqn(t) in str(e) for t in cfg.SOURCES)
        reingest_ok = already
        note = f"aborted as required: {str(e)[:90]}..."
    check(7, "duplicate batch aborts (negative test)", reingest_ok, note)

    return checks, locations


def _business_schema(table):
    from pyspark.sql.types import StringType, StructField, StructType
    return StructType(
        [StructField(c, StringType(), True) for c in cfg.BUSINESS_COLUMNS[table]])


def main():
    ensure_schema()
    report = loader.run_ingestion(spark)
    checks, locations = acceptance_checks(report["batch_id"])
    failed = [c for c in checks if not c["passed"]]
    for c in checks:
        print(f"{'PASS' if c['passed'] else 'FAIL'}  {c['check']}  --  {c['detail']}")
    print(f"\n{len(checks) - len(failed)}/{len(checks)} acceptance checks passed")
    if failed:
        raise AssertionError(
            f"{len(failed)} acceptance checks failed: "
            + "; ".join(c["check"] for c in failed))
    print("\nRun report:", json.dumps(report, indent=2, default=str))
    print("Storage locations:")
    for t, loc in sorted(locations.items()):
        print(f"  {cfg.table_fqn(t)}  ->  {loc}")


if __name__ == "__main__":
    main()
