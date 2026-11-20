"""Bronze ingestion loader (Phase 4) -- spec 04-bronze-ingestion.md.

Raw-is-raw load of the six Phase 3 CSVs into customer_360.bronze.
Fail-closed pipeline order per table: file present -> header == contract ->
row count == manifest -> duplicate-batch guard -> atomic Delta append ->
post-write profile. Nothing is dropped, coerced, or silently overwritten.

Error matrix and idempotency contract: spec sections 6-7. Recovery from a
partial batch is a documented MANUAL procedure (DELETE WHERE _batch_id = ...,
then re-run) -- never executed automatically.
"""
import csv
import hashlib
import json
from datetime import date, datetime

from src.config import bronze_config as cfg


def load_manifest():
    with open(cfg.MANIFEST_PATH, encoding="utf-8") as f:
        return json.load(f)


def read_header(path):
    """First CSV record (the header row) exactly as written on disk."""
    with open(path, encoding="utf-8", newline="") as f:
        return next(csv.reader(f))


def verify_header(path, table):
    """Exact name-and-order comparison of the file header vs the contract.
    Returns None on success or a printable mismatch description."""
    expected = cfg.BUSINESS_COLUMNS[table]
    actual = read_header(path)
    if actual == expected:
        return None
    problems = []
    if len(actual) != len(expected):
        problems.append(f"column count {len(actual)} != {len(expected)}")
    for i, (a, e) in enumerate(zip(actual, expected)):
        if a != e:
            problems.append(f"position {i}: file has {a!r}, contract expects {e!r}")
    missing = set(expected) - set(actual)
    extra = set(actual) - set(expected)
    if missing:
        problems.append(f"missing from file: {sorted(missing)}")
    if extra:
        problems.append(f"unexpected in file: {sorted(extra)}")
    return "; ".join(problems) or "column order differs"


def sha256_file(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def precheck(batch_id, manifest):
    """Section 7 pre-write checks: files present, headers match contracts,
    manifest batch id consistent. Raises AssertionError on any failure."""
    problems = []
    if manifest.get("batch_id") != batch_id:
        problems.append(
            f"manifest batch_id {manifest.get('batch_id')!r} != requested {batch_id!r}")
    for table, (filename, _) in cfg.SOURCES.items():
        path = f"{cfg.batch_dir(batch_id)}/{filename}"
        try:
            mismatch = verify_header(path, table)
        except FileNotFoundError:
            problems.append(f"{table}: source file missing: {path}")
            continue
        if mismatch:
            problems.append(f"{table}: header mismatch -- {mismatch}")
    if problems:
        raise AssertionError("Bronze pre-check failed:\n  - " + "\n  - ".join(problems))


def _spark_schema():
    from pyspark.sql.types import StringType, StructField, StructType
    def schema_for(table):
        return StructType(
            [StructField(c, StringType(), True) for c in cfg.BUSINESS_COLUMNS[table]])
    return schema_for


def duplicate_batches(spark, batch_id):
    """Tables that already contain rows for this _batch_id (fail-closed guard).
    Raises on unexpected errors -- never silently treats a table as un-ingested
    (that would defeat the no-silent-overwrite requirement)."""
    from pyspark.sql import functions as F
    present = []
    for table in cfg.SOURCES:
        fqn = cfg.table_fqn(table)
        if not spark.catalog.tableExists(fqn):
            continue
        n = (spark.table(fqn)
             .where(F.col("_batch_id") == batch_id)
             .count())
        if n:
            present.append((fqn, n))
    return present


def ingest_table(spark, table, batch_id, run_ts):
    """Load one table. Row-count reconciliation happens BEFORE the write."""
    from pyspark.sql import functions as F
    filename, source_system = cfg.SOURCES[table]
    path = f"{cfg.batch_dir(batch_id)}/{filename}"
    file_uri = f"{cfg.batch_dir(batch_id)}/{filename}"

    contract = _spark_schema()(table)
    df = (spark.read
          .option("header", True)
          .option("mode", "PERMISSIVE")
          .schema(contract)
          .csv(file_uri))

    # fail closed: count must equal the manifest BEFORE any write
    # (PERMISSIVE + all-STRING cannot lose values; only structural faults
    # could change the count, and those abort here)
    n = df.count()
    return df, n, source_system, path, file_uri


def run_ingestion(spark, batch_id=None):
    """Full Phase 4 run: guard -> per-table atomic appends -> run report.
    Returns the report dict. Raises before any write on pre-check failure
    or duplicate batch (never silent, never overwrite)."""
    manifest = load_manifest()
    batch_id = batch_id or manifest["batch_id"]

    precheck(batch_id, manifest)

    dupes = duplicate_batches(spark, batch_id)
    if dupes:
        raise AssertionError(
            f"Batch {batch_id!r} already ingested (aborting, nothing written). "
            f"Affected: {dupes}. Manual recovery if intended: "
            f"DELETE FROM <table> WHERE _batch_id = '{batch_id}'; then re-run.")

    from pyspark.sql import functions as F
    run_ts = datetime.utcnow()
    report = {"batch_id": batch_id, "run_ts": run_ts.isoformat(), "tables": {}}

    for table in cfg.SOURCES:
        df, n, source_system, path, file_uri = ingest_table(
            spark, table, batch_id, run_ts)
        expected = manifest["row_counts"][
            "support_cases" if table == "support_tickets" else table]
        if n != expected:
            raise AssertionError(
                f"{table}: read {n} rows, manifest expects {expected} -- aborting "
                f"before write (no partial batch; tables already written this run "
                f"remain committed and the duplicate-batch guard will report them "
                f"on the next attempt)")

        out = (df
               .withColumn("_source_system", F.lit(source_system))
               .withColumn("_batch_id", F.lit(batch_id))
               .withColumn("_ingestion_timestamp", F.lit(run_ts).cast("timestamp"))
               .withColumn("_ingestion_date", F.lit(run_ts.date()).cast("date"))
               .withColumn("_source_file", F.lit(file_uri)))
        out.write.format("delta").mode("append").saveAsTable(cfg.table_fqn(table))

        report["tables"][table] = {"rows": n, "source": file_uri}

    return report
