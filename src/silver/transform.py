"""Phase 5 Silver transformation pipelines (spec sections 4-10).

Per entity, in fixed order: canonicalize -> dedup (Amendment 1) -> validate
survivors -> build Silver rows. Everything before run_silver_batch() is a
pure DataFrame->DataFrame function (unit-testable with synthetic data,
Amendment 6); only run_silver_batch() writes, and only behind the fail-closed
duplicate-batch guard (spec section 10). No silent exception handling; no
overwrite modes; every write traceable by _batch_id.

Reconciliation (Amendment 3): bronze_rows = silver_rows + duplicate_rows +
rejected_distinct, where rejected_distinct counts DISTINCT source records
(coalesce(pk, raw bytes)) -- multi-reason rows are stored per reason but
counted once.
"""

from datetime import datetime

from pyspark.sql import DataFrame, functions as F
from pyspark.sql.types import (StringType, StructField, StructType, TimestampType,
                                IntegerType)
from pyspark.sql.window import Window

from src.config import silver_config as cfg
from src.silver import standardizer as std


def _raw(col):
    return f"raw_{col}"


# ------------------------------------------------------------------
# canonicalize: raw bronze business columns (STRING) -> typed/normalized
# ------------------------------------------------------------------
def canonicalize(df, entity):
    """Input: bronze DataFrame with raw business columns + lineage.
    Output: adds typed canonical columns (final silver names), raw_* copies,
    and per date/timestamp column a _m_<col> pattern-match-count helper."""
    e = cfg.ENTITIES[entity]
    business = cfg.bronze_business_columns(entity)

    # 1. keep raw copies (quarantine + dedup classification need originals)
    out = df
    for c in business:
        out = out.withColumn(_raw(c), F.col(c))

    # 2. typed/normalized canonical columns (spec section 5)
    for name, typ in cfg.TYPED_COLUMNS[entity]:
        r = _raw(name)
        if name == e["pk"] or name in e.get("native_refs", {}):
            expr = std.normalize_ref(r)
        elif name == "global_customer_ref":
            expr = std.normalize_gref(r)
        elif name == e.get("email_col"):
            expr = std.normalize_email(r)
        elif name == e.get("phone_col"):
            expr = std.normalize_phone(r)
        elif name == "name_key":
            expr = std.name_key_expr(_raw(e["name_cols"][0]), _raw(e["name_cols"][1]))
        elif typ == "date":
            expr = std.parse_date_expr(r)
        elif typ == "timestamp":
            expr = std.parse_timestamp_expr(r)
        elif typ.startswith("decimal"):
            expr = std.try_cast_expr(r, typ)
        elif typ == "int":
            # review fix 5: strict integer notation for every int column
            # except the approved csat decimal-notation path (A5)
            if name in cfg.INT_CAST_VIA_DECIMAL:
                expr = std.int_via_decimal_expr(r)
            else:
                expr = std.strict_int_expr(r)
        else:  # plain string
            expr = std.trim_expr(r)
        out = out.withColumn(name, expr)

    # 3. pattern-match-count helpers (Amendment 4 exactly-one verification)
    for col, _required in e.get("dates", {}).items():
        out = out.withColumn(f"_m_{col}", std.pattern_match_count(_raw(col),
                                                                  cfg.DATE_REGEXES))
    for col, _required in e.get("timestamps", {}).items():
        out = out.withColumn(f"_m_{col}", std.pattern_match_count(_raw(col),
                                                                  cfg.TIMESTAMP_REGEXES))
    return out


def assert_unambiguous_formats(df, entity):
    """Amendment 4: if any non-null date/timestamp value matches MORE than
    one approved pattern, the batch fails closed (systemic ambiguity; a
    parser is never silently selected). Zero-match rows are record-level
    INVALID_*_FORMAT, handled by validation."""
    e = cfg.ENTITIES[entity]
    checks = [(c, cfg.DATE_REGEXES) for c in e.get("dates", {})] + \
             [(c, cfg.TIMESTAMP_REGEXES) for c in e.get("timestamps", {})]
    for col, _ in checks:
        n = df.where(std.present(_raw(col)) & (F.col(f"_m_{col}") > 1)).count()
        if n:
            raise AssertionError(
                f"{entity}.{col}: {n} values match more than one approved date/"
                f"timestamp pattern -- batch fails closed (no silent parser "
                f"selection; spec 05 section 5.2 rule 2)")


# ------------------------------------------------------------------
# dedup (Amendment 1)
# ------------------------------------------------------------------
def dedup(df, entity, run_ts):
    """Deterministic within-batch dedup by PK group.
    Returns (survivors_df, duplicates_df).

    Survivor precedence (Amendment 1; recency is NOT a criterion):
      1. fewest NULL canonical business attributes
      2. lowest SHA-256 over canonicalized business columns
      3. lexicographic serialized tie-break (stable; fires only when 1-2 tie)

    Review fix 2: rows with a NULL/empty PK are NEVER deduplicated against
    any other row (unique group key per row) -- each independently reaches
    PK_NULL quarantine, even when raw business values are identical.
    Review fix 4: serialization is length-prefixed (NULL distinct from empty,
    no delimiter collisions) for both hash input and classification.
    Review fix 6: hash/completeness inputs are the canonicalized SOURCE
    business columns; derived columns (name_key) are excluded (spec 05
    section 6 -- name_key is a function of first/last name).
    Amendment 7 (approved 2026-11-24, literal semantics reaffirmed
    2026-11-24): exact/near classification is evaluated against ALL records
    in the PK group, not only the survivor -- a loser is EXACT_DUPLICATE iff
    its raw serialized bytes are byte-identical to at least one OTHER record
    anywhere in the group (no ordering requirement; the first member of a
    byte-identical subset is NOT near when another identical record exists);
    otherwise NEAR_DUPLICATE. Survivor selection (Amendment 1) is unchanged."""
    e = cfg.ENTITIES[entity]
    business = cfg.bronze_business_columns(entity)
    derived = set(cfg.DERIVED_SILVER_COLUMNS.get(entity, []))
    key_cols = [n for n, _ in cfg.TYPED_COLUMNS[entity] if n not in derived]
    raw_cols = [_raw(c) for c in business]

    out = (df
           .withColumn("_row_id", F.monotonically_increasing_id())
           .withColumn("_nulls", std.null_count_expr(key_cols))
           .withColumn("_hash", std.canonical_hash_expr(key_cols))
           .withColumn("_canon_bytes", std.canonical_serialized_expr(key_cols))
           .withColumn("_raw_bytes", std.raw_serialized_expr(raw_cols)))

    pk_raw = _raw(e["pk"])
    # null/empty PK -> unique per-row key (never groups with any other row)
    group_key = (F.when(std.missing(pk_raw),
                        F.concat(F.lit("__NULL__"),
                                 F.col("_row_id").cast("string")))
                 .otherwise(F.col(e["pk"])))
    out = out.withColumn("_group_key", group_key)

    # Deterministic group order = Amendment 1 survivor precedence, then the
    # Bronze lineage columns and _row_id so that byte-identical rows (fully
    # tied on the precedence keys) also get a fully deterministic order.
    # Extending the tie-break does NOT change which business record
    # survives -- rows tied through _raw_bytes differ only in lineage
    # (documented immaterial, spec 05 section 4) -- it only pins WHICH
    # identical copy is retained, making survivor selection fully
    # deterministic (criterion 3).
    order_cols = [F.col("_nulls").asc(), F.col("_hash").asc(),
                  F.col("_canon_bytes").asc(), F.col("_raw_bytes").asc()] \
        + [F.col(c).asc() for c in cfg.LINEAGE_COLUMNS] \
        + [F.col("_row_id").asc()]

    w_sel = Window.partitionBy("_group_key").orderBy(*order_cols)
    out = out.withColumn("_rn", F.row_number().over(w_sel))

    w_first = Window.partitionBy("_group_key").orderBy("_rn")
    out = out.withColumn("_survivor_pk", F.first(e["pk"]).over(w_first))

    # Amendment 7 (approved 2026-11-24, literal semantics reaffirmed
    # 2026-11-24): classification is evaluated against ALL records in the PK
    # group, not only the survivor, with NO ordering requirement. A loser is
    # EXACT_DUPLICATE iff its raw serialized bytes are byte-identical to at
    # least one OTHER record anywhere in the group -- i.e. it belongs to a
    # byte-set of size >= 2 within the group (counting the loser itself);
    # otherwise NEAR_DUPLICATE. The first member of a byte-identical subset
    # is therefore NOT classified NEAR when another identical record exists.
    # Survivor selection (Amendment 1 precedence) is unchanged.
    w_bytes = Window.partitionBy("_group_key", "_raw_bytes")
    out = out.withColumn("_same_bytes_in_group", F.count(F.lit(1)).over(w_bytes))

    survivors = out.where(F.col("_rn") == 1).drop("_rn", "_survivor_pk",
                                                    "_row_id",
                                                    "_same_bytes_in_group")

    losers = out.where(F.col("_rn") > 1)
    reason = (F.when(F.col("_same_bytes_in_group") >= 2, F.lit("EXACT_DUPLICATE"))
              .otherwise(F.lit("NEAR_DUPLICATE")))
    row_json = F.to_json(F.struct(*[F.col(_raw(c)).alias(c) for c in business]))
    duplicates = losers.select(
        F.lit(entity).alias("_entity"),
        F.col(e["pk"]).alias("_record_pk"),
        reason.alias("_dedup_reason"),
        F.col("_survivor_pk").alias("_survivor_pk"),
        row_json.alias("_source_row_json"),
        *[F.col(c) for c in cfg.LINEAGE_COLUMNS],
        F.lit(run_ts).cast("timestamp").alias("_quarantined_timestamp"),
    )
    return survivors, duplicates


# ------------------------------------------------------------------
# validation (spec section 7) -- survivors only (dedup runs first)
# ------------------------------------------------------------------
def _special_reason(df, entity, rule_id):
    """Returns a boolean Column for one entity-specific rule, or None if the
    rule does not apply to this entity."""
    e = cfg.ENTITIES[entity]

    if rule_id == "DOB_AFTER_REGISTRATION":
        return (F.col("date_of_birth").isNotNull()
                & F.col("registration_date").isNotNull()
                & (F.col("date_of_birth") > F.col("registration_date")))

    if rule_id == "LAST_ACTIVITY_BEFORE_OPEN":
        return (F.col("last_activity_date").isNotNull()
                & F.col("open_date").isNotNull()
                & (F.col("last_activity_date") < F.col("open_date")))

    if rule_id == "EXPIRY_NOT_AFTER_ISSUE":
        return (F.col("expiry_date").isNotNull()
                & F.col("issue_date").isNotNull()
                & (F.col("expiry_date") <= F.col("issue_date")))

    if rule_id == "END_BEFORE_START":
        return (F.col("end_date").isNotNull()
                & F.col("start_date").isNotNull()
                & (F.col("end_date") <= F.col("start_date")))

    if rule_id == "END_DATE_REQUIRED":
        return ((F.col("repayment_status") == "PAID_OFF")
                & std.missing(_raw("end_date")))

    if rule_id == "RESOLVED_BEFORE_CREATED":
        return (F.col("resolved_date").isNotNull()
                & F.col("created_date").isNotNull()
                & (F.col("resolved_date") < F.col("created_date")))

    if rule_id == "RESOLVED_DATE_REQUIRED":
        return (F.col("status").isin("RESOLVED", "CLOSED")
                & std.missing(_raw("resolved_date")))

    if rule_id == "CSAT_INVALID":
        # unparseable, out of 1-5, populated while unresolved, or missing while
        # resolved/closed (contract: NULL allowed only when unresolved)
        csat_raw, csat = _raw("csat_score"), F.col("csat_score")
        unresolved = F.col("status").isin("OPEN", "IN_PROGRESS")
        resolved = F.col("status").isin("RESOLVED", "CLOSED")
        return (std.present(csat_raw) & csat.isNull()) \
            | (csat.isNotNull() & ~csat.between(1, 5)) \
            | (std.present(csat_raw) & unresolved) \
            | (resolved & std.missing(csat_raw))

    if rule_id == "SESSION_DURATION_INVALID":
        # negative, unparseable, populated on FAILED, or missing on SUCCESS
        # (contract: NULL allowed only when login_status = FAILED)
        sd_raw, sd = _raw("session_duration_minutes"), F.col("session_duration_minutes")
        return (std.present(sd_raw) & sd.isNull()) \
            | (sd.isNotNull() & (sd < 0)) \
            | (std.present(sd_raw) & (F.col("login_status") == "FAILED")) \
            | ((F.col("login_status") == "SUCCESS") & std.missing(sd_raw))

    return None


def _reason_columns(df, entity):
    """One F.when(...) column per (rule_id, condition). All approved rules;
    conditions reference raw_* for presence/format and typed columns for
    values/cross-field comparisons."""
    e = cfg.ENTITIES[entity]
    reasons = []

    def add(rule_id, cond):
        reasons.append(F.when(cond, F.lit(rule_id)).otherwise(
            F.lit(None).cast(StringType())))

    pk_raw = _raw(e["pk"])
    # PK_NULL / PK_INVALID_FORMAT
    add("PK_NULL", std.missing(pk_raw))
    add("PK_INVALID_FORMAT", std.present(pk_raw)
        & ~F.col(e["pk"]).rlike(e["pk_regex"]))

    # mandatory native refs (customers: PK is the native ref, covered above)
    for ref, rx in e.get("native_refs", {}).items():
        add("NATIVE_REF_INVALID_FORMAT", std.present(_raw(ref))
            & ~F.col(ref).rlike(rx))

    # required fields (contract-NotNull business columns; PK handled above)
    req = [std.missing(_raw(c)) for c in e.get("required", []) if _raw(c) != pk_raw]
    if req:
        add("REQUIRED_FIELD_NULL", functools_reduce_or(req))

    # dates/timestamps (review fix 1): reject any PRESENT value whose
    # parse returns NULL -- covers both zero-regex-match values and
    # semantically impossible values (e.g. 2026-02-31) that match exactly
    # one approved regex. (>1 matches still fails the whole batch, per
    # assert_unambiguous_formats.)
    for col, _req in e.get("dates", {}).items():
        add("INVALID_DATE_FORMAT", std.present(_raw(col))
            & F.col(col).isNull())
    for col, _req in e.get("timestamps", {}).items():
        add("INVALID_TIMESTAMP_FORMAT", std.present(_raw(col))
            & F.col(col).isNull())

    # numerics: present but unparseable; then range rules
    for col, spec in e.get("numerics", {}).items():
        present = std.present(_raw(col))
        typed = F.col(col)
        add("NUMERIC_UNPARSEABLE", present & typed.isNull())
        if "rule" in spec:
            lo, hi = spec.get("min"), spec.get("max")
            bad = F.lit(False)
            if lo is not None:
                bad = bad | (typed <= F.lit(lo) if spec.get("exclusive_min")
                             else typed < F.lit(lo))
            if hi is not None:
                bad = bad | (typed > F.lit(hi))
            add(spec["rule"], present & typed.isNotNull() & bad)

    # enum domains
    for col, domain in e.get("enums", {}).items():
        add("ENUM_VIOLATION", std.present(_raw(col))
            & ~F.col(col).isin(*domain))

    # names: letters-only after trim (customers)
    for col in e.get("name_cols", []):
        add("NAME_INVALID", std.present(_raw(col))
            & ~F.trim(F.col(_raw(col))).rlike(cfg.NAME_LETTERS_REGEX))

    # postal code length 4-6
    if e.get("postal_col"):
        p = e["postal_col"]
        add("POSTAL_INVALID", std.present(_raw(p))
            & ~F.length(F.trim(F.col(_raw(p)))).between(4, 6))

    # fixed-format columns (account_number, branch_code, masked PAN)
    for col, spec in e.get("formats", {}).items():
        cond = std.present(_raw(col)) & ~F.trim(F.col(_raw(col))).rlike(spec["regex"])
        add(spec["rule"], cond)

    # entity-specific cross-field / conditional rules
    for rule_id in e.get("special_rules", []):
        cond = _special_reason(df, entity, rule_id)
        if cond is not None:
            add(rule_id, cond)

    return reasons


def functools_reduce_or(conds):
    """OR-reduce a list of boolean Columns (no builtin-func ambiguity)."""
    out = conds[0]
    for c in conds[1:]:
        out = out | c
    return out


def validate(df, entity, run_ts):
    """Validate survivors. Returns (valid_df, invalid_rows_df, rejected_distinct).
    invalid_rows: raw business columns (original names) + one row per
    distinct failure reason (_rejection_reason) + lineage + timestamp.
    rejected_distinct: number of deduplicated SURVIVOR RECORDS failing
    validation -- each survivor is one source record (Amendment 3), so this
    is counted before the reason explosion; multi-reason records are stored
    per reason but counted once. Identical null-PK survivors count separately
    (they are independent records, review fix 2)."""
    e = cfg.ENTITIES[entity]
    business = cfg.bronze_business_columns(entity)

    reasons = _reason_columns(df, entity)
    with_reasons = df.withColumn("_reasons",
                                 F.filter(F.array(*reasons), lambda x: x.isNotNull()))

    invalid = with_reasons.where(F.size("_reasons") > 0)
    rejected_distinct = invalid.count()
    invalid_rows = invalid.select(
        *[F.col(_raw(c)).alias(c) for c in business],
        F.explode("_reasons").alias("_rejection_reason"),
        *[F.col(c) for c in cfg.LINEAGE_COLUMNS],
        F.lit(run_ts).cast("timestamp").alias("_quarantined_timestamp"),
    )

    valid = with_reasons.where(F.size("_reasons") == 0).drop("_reasons")
    return valid, invalid_rows, rejected_distinct


# ------------------------------------------------------------------
# Silver row assembly (typed columns + lineage + processing timestamp)
# ------------------------------------------------------------------
def build_silver_rows(valid_df, entity, run_ts):
    cols = [n for n, _ in cfg.TYPED_COLUMNS[entity]] + cfg.LINEAGE_COLUMNS
    helpers = {f"_m_{c}" for c in cfg.ENTITIES[entity].get("dates", {})} | \
              {f"_m_{c}" for c in cfg.ENTITIES[entity].get("timestamps", {})}
    out = valid_df.select(
        *[F.col(c) for c in cols],
        F.lit(run_ts).cast("timestamp").alias(cfg.SILVER_TS_COL),
    )
    # raw_* copies and match-count helpers never leave the pipeline
    out = out.select([c for c in out.columns if not c.startswith("raw_")
                      and c not in helpers and c not in
                      ("_nulls", "_hash", "_canon_bytes", "_raw_bytes", "_group_key")])
    return out


def audit_row_schema():
    return StructType([
        StructField("table_name", StringType()),
        StructField("batch_id", StringType()),
        StructField("run_timestamp", TimestampType()),
        StructField("bronze_rows", IntegerType()),
        StructField("silver_rows", IntegerType()),
        StructField("exact_duplicate_rows", IntegerType()),
        StructField("near_duplicate_rows", IntegerType()),
        StructField("rejected_distinct_rows", IntegerType()),
        StructField("rejection_reason_rows", IntegerType()),
        StructField("invalid_emails_nulled", IntegerType()),
        StructField("invalid_refs_nulled", IntegerType()),
        StructField("invalid_phones_nulled", IntegerType()),
        StructField("loan_anomalies_flagged", IntegerType()),
        StructField("checks_passed", IntegerType()),
        StructField("checks_total", IntegerType()),
        StructField("notes", StringType()),
    ])


def entity_counters(valid, invalid_rows, rejected_distinct, duplicates, entity,
                    bronze_df):
    """Amendment 3 counters. rejected_distinct comes from validate()
    (deduplicated survivor records failing validation -- one per source
    record; never reason rows, and never collapsed across identical
    null-PK records, review fix 2)."""
    e = cfg.ENTITIES[entity]
    business = cfg.bronze_business_columns(entity)

    bronze_rows = bronze_df.count()
    dup_counts = {r["_dedup_reason"]: r["n"] for r in
                 duplicates.groupBy("_dedup_reason").count()
                 .withColumnRenamed("count", "n").collect()}

    # batch-level value-quality profile (spec section 9.3 counters;
    # bronze-side counts as verified in EXPECTED_BATCH_001)
    email_col = e.get("email_col")
    phone_col = e.get("phone_col")
    prof = bronze_df
    if email_col:
        prof = prof.withColumn("_bad_email", std.email_invalid(email_col))
    else:
        prof = prof.withColumn("_bad_email", F.lit(False))
    if phone_col:
        prof = prof.withColumn("_bad_phone", std.phone_invalid(phone_col))
    else:
        prof = prof.withColumn("_bad_phone", F.lit(False))
    if "global_customer_ref" in business:
        prof = prof.withColumn("_bad_gref", std.gref_invalid("global_customer_ref"))
    else:
        prof = prof.withColumn("_bad_gref", F.lit(False))
    p = prof.agg(F.sum(F.when(F.col("_bad_email"), 1).otherwise(0)).alias("em"),
                 F.sum(F.when(F.col("_bad_phone"), 1).otherwise(0)).alias("ph"),
                 F.sum(F.when(F.col("_bad_gref"), 1).otherwise(0)).alias("gr")
                 ).collect()[0]

    anomaly = 0
    if entity == "loans":
        anomaly = valid.where(F.col("outstanding_balance") >
                              F.col("principal_amount")).count()

    return {
        "bronze_rows": bronze_rows,
        "silver_rows": valid.count(),
        "exact_duplicate_rows": dup_counts.get("EXACT_DUPLICATE", 0),
        "near_duplicate_rows": dup_counts.get("NEAR_DUPLICATE", 0),
        "rejected_distinct_rows": int(rejected_distinct),
        "rejection_reason_rows": invalid_rows.count(),
        "invalid_emails_nulled": p["em"],
        "invalid_phones_nulled": p["ph"],
        "invalid_refs_nulled": p["gr"],
        "loan_anomalies_flagged": anomaly,
    }


# ------------------------------------------------------------------
# production run (behind the fail-closed guard)
# ------------------------------------------------------------------
def _default_guard_targets():
    """All 14 batch-carrying targets with their batch identifier column:
    silver/quarantine tables use the _batch_id lineage column;
    processing_audit uses batch_id (review fix 3)."""
    return ([(cfg.silver_fqn(t), "_batch_id") for t in cfg.ENTITY_ORDER]
            + [(cfg.invalid_fqn(t), "_batch_id") for t in cfg.ENTITY_ORDER]
            + [(cfg.DUPLICATE_TABLE, "_batch_id"),
               (cfg.AUDIT_TABLE, cfg.AUDIT_BATCH_COLUMN)])


def batch_already_present(spark, batch_id, custom_targets=None):
    """Fail-closed duplicate-batch guard (spec section 10). Explicit checks,
    no swallowed exceptions. Returns list of (fqn, id_column, rows) already
    carrying the batch; run_silver_batch raises on any hit.
    custom_targets: fqn strings (id column assumed '_batch_id') or
    (fqn, id_column) tuples."""
    raw = custom_targets if custom_targets is not None else _default_guard_targets()
    targets = [t if isinstance(t, tuple) else (t, "_batch_id") for t in raw]
    present = []
    for fqn, col in targets:
        if not spark.catalog.tableExists(fqn):
            continue
        n = spark.table(fqn).where(F.col(col) == batch_id).count()
        if n:
            present.append((fqn, col, n))
    return present


def run_silver_batch(spark, batch_id):
    """Full Phase 5 run for one batch. Raises before any write if the batch
    is already present anywhere. Returns the per-entity counters (also
    written to silver.processing_audit)."""
    dupes = batch_already_present(spark, batch_id)
    if dupes:
        affected = "; ".join(f"{fqn} (rows={n}, id col={col})"
                             for fqn, col, n in dupes)
        raise AssertionError(
            f"Batch {batch_id!r} already processed for Silver (aborting, "
            f"nothing written). Affected: {affected}. Manual recovery, for "
            f"EACH affected table: DELETE FROM <table> WHERE <id col> = "
            f"'{batch_id}' -- the id col is _batch_id for silver and "
            f"quarantine tables and {cfg.AUDIT_BATCH_COLUMN} for "
            f"{cfg.AUDIT_TABLE}. Only tables listed above contain this "
            f"batch (a partial prior run leaves only some tables populated). "
            f"Then re-run.")

    run_ts = datetime.utcnow()
    results = {}

    for entity in cfg.ENTITY_ORDER:
        e = cfg.ENTITIES[entity]
        bronze_df = (spark.table(cfg.bronze_fqn(entity))
                     .where(F.col("_batch_id") == batch_id))

        canon = canonicalize(bronze_df, entity)
        assert_unambiguous_formats(canon, entity)          # Amendment 4, fail closed
        survivors, duplicates = dedup(canon, entity, run_ts)  # Amendment 1
        valid, invalid_rows, rejected_distinct = validate(survivors, entity, run_ts)

        counters = entity_counters(valid, invalid_rows, rejected_distinct,
                                   duplicates, entity, bronze_df)
        # reconciliation identity (Amendment 3) verified BEFORE writes
        total = (counters["silver_rows"] + counters["exact_duplicate_rows"]
                 + counters["near_duplicate_rows"]
                 + counters["rejected_distinct_rows"])
        if total != counters["bronze_rows"]:
            raise AssertionError(
                f"{entity}: reconciliation identity failed before write -- "
                f"{total} accounted vs {counters['bronze_rows']} bronze rows")

        build_silver_rows(valid, entity, run_ts).write \
            .format("delta").mode("append").saveAsTable(cfg.silver_fqn(entity))
        # review fix 7: per spec section 9 every quarantine target exists
        # after a run, even when empty -- an empty append creates the table
        # with the approved schema (batch_001 expects empty invalid_* tables)
        invalid_rows.write.format("delta").mode("append") \
            .saveAsTable(cfg.invalid_fqn(entity))
        duplicates.write.format("delta").mode("append") \
            .saveAsTable(cfg.DUPLICATE_TABLE)

        results[entity] = counters

    # audit rows (one per entity; checks filled by the orchestrator)
    audit_df = spark.createDataFrame(
        [{"table_name": t,
          "batch_id": batch_id,
          "run_timestamp": run_ts,
          **{k: v for k, v in results[t].items()},
          "checks_passed": 0, "checks_total": 0,
          "notes": "counts verified; acceptance checks run by orchestrator"}
         for t in cfg.ENTITY_ORDER],
        schema=audit_row_schema())
    audit_df.write.format("delta").mode("append").saveAsTable(cfg.AUDIT_TABLE)
    return results
