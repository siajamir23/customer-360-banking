# MAGIC %pip install faker
"""Phase 3 orchestrator -- thin: imports the src/ modules and runs them
top-to-bottom. No generation logic lives here (approved modular structure).

Creates ONLY the approved Unity Catalog objects, idempotently:
  catalog customer_360, schema customer_360.default, volume raw_data.
No Bronze/Silver/Gold/Quarantine tables. fraud_detection is read-only.

Determinism: fixed base_seed=360, fixed event window (no now()); identical
CONFIG produces byte-identical CSVs, proven by SHA-256 (checks 11 and 12).
"""
import hashlib
import importlib
import json
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path

try:
    REPO_ROOT = Path(__file__).resolve().parents[1]
except NameError:
    REPO_ROOT = Path("/Workspace/Users/sia.jamir23@gmail.com/customer-360-banking")
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from src.config import generator_config as cfg
from src.utilities.identity import build_population
from src.utilities.validation import append_orchestrator_checks, validate

GENERATORS = {
    "customers": "src.generators.customers",
    "accounts": "src.generators.accounts",
    "cards": "src.generators.cards",
    "loans": "src.generators.loans",
    "digital_logins": "src.generators.digital_banking",
    "support_cases": "src.generators.support",
}


def ensure_environment():
    """Idempotent creation of the three approved objects only."""
    from databricks.sdk.runtime import spark
    spark.sql(f"CREATE CATALOG IF NOT EXISTS {cfg.CATALOG}")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {cfg.CATALOG}.{cfg.SCHEMA}")
    spark.sql(f"CREATE VOLUME IF NOT EXISTS {cfg.CATALOG}.{cfg.SCHEMA}.{cfg.VOLUME}")


def read_fraud_customers():
    """Read-only seed input: ALL 10,000 existing fraud customers."""
    from databricks.sdk.runtime import spark
    rows = spark.table(cfg.FRAUD_CUSTOMERS_TABLE).orderBy("customer_id").collect()
    if len(rows) < cfg.POPULATION["seeded_fraud_customers"]:
        raise ValueError(f"Expected 10,000 fraud customers, found {len(rows)}")
    return [r.asDict() for r in rows]


def generate_all(persons, targets_override=None):
    """Run the six modular generators (each independently seeded)."""
    raw = {}
    for source, module_name in GENERATORS.items():
        kwargs = {"n_masters": targets_override[source]} if (
            targets_override and source in targets_override) else {}
        raw[source] = importlib.import_module(module_name).generate(persons, **kwargs)
    return raw


def _sha256(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def write_outputs(raw, out_dir):
    """Write the six CSVs (UTF-8, header, newline-terminated) + hash each file."""
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)
    hashes = {}
    for source, df in raw.items():
        p = out / cfg.SOURCE_FILES[source]
        df.to_csv(p, index=False, lineterminator="\n", encoding="utf-8")
        hashes[source] = _sha256(p)
    return hashes


# Serverless + UC only permits local-filesystem access under /Workspace, so
# staging dirs for the volume copy must live there (not /tmp).
SCRATCH_ROOT = Path("/Workspace/Users/sia.jamir23@gmail.com/.c360_scratch")


def _scratch(prefix):
    SCRATCH_ROOT.mkdir(parents=True, exist_ok=True)
    return tempfile.mkdtemp(prefix=prefix, dir=str(SCRATCH_ROOT))


def run(write_to_volume=True, keep_local=None):
    """Full Phase 3 pipeline: setup -> seed read -> generate -> validate ->
    determinism proof -> write to the approved volume -> manifest."""
    ensure_environment()
    fraud_rows = read_fraud_customers()
    persons = build_population(fraud_rows)

    # Primary generation
    raw = generate_all(persons)
    local = keep_local or _scratch("c360_batch_")
    hashes = write_outputs(raw, local)

    # Check 11: determinism -- full regeneration must be byte-identical
    raw_b = generate_all(persons)
    local_b = _scratch("c360_verify_")
    hashes_b = write_outputs(raw_b, local_b)
    determinism_ok = hashes == hashes_b

    # Check 12: seed independence -- change ONE source's size; only that file
    # re-hashes (accounts target +100)
    override = {"accounts": cfg.TARGETS["accounts"] + 100}
    raw_c = generate_all(persons, override)
    local_c = _scratch("c360_seed_")
    hashes_c = write_outputs(raw_c, local_c)
    seed_ok = (hashes_c["accounts"] != hashes["accounts"]
               and all(hashes_c[s] == hashes[s]
                       for s in hashes if s != "accounts"))

    # Checks 0-10 + 13, then append 11-12
    checks, report = validate(raw, persons)
    checks = append_orchestrator_checks(
        checks, determinism_ok, seed_ok,
        detail="second full regeneration compared by SHA-256")

    result = {
        "persons": persons, "raw": raw, "checks": checks, "report": report,
        "hashes": hashes, "local_dir": str(local),
        "manifest": {
            "batch_id": cfg.BATCH_ID,
            "base_seed": cfg.BASE_SEED,
            "anchor_date": cfg.ANCHOR_DATE,
            "population": {"seeded_fraud_customers": 10_000, "new_customers": 2_000},
            "row_counts": {s: len(df) for s, df in raw.items()},
            "sha256": hashes,
            "checks_passed": int(checks["passed"].sum()),
            "checks_total": len(checks),
            "run_timestamp_utc": datetime.now(timezone.utc).isoformat(),
        },
    }

    if write_to_volume:
        import shutil
        from databricks.sdk.runtime import dbutils
        # manifest written into the staging dir first, then ONE recursive copy
        # transfers all six CSVs + manifest (avoids the second single-file cp,
        # which throws FileNotFoundException on serverless)
        (Path(local) / "_manifest.json").write_text(
            json.dumps(result["manifest"], indent=2), encoding="utf-8")
        dbutils.fs.mkdirs(cfg.BATCH_DIR)
        dbutils.fs.cp(f"file:{local}", cfg.BATCH_DIR, recurse=True)
        # serverless quirk: the recursive cp may skip the manifest; write it
        # directly through the /Volumes FUSE path and confirm it landed
        manifest_vol = Path(cfg.BATCH_DIR) / "_manifest.json"
        if not manifest_vol.exists():
            shutil.copy(
                str(Path(local) / "_manifest.json"), str(manifest_vol))
        if not manifest_vol.exists():
            raise AssertionError("_manifest.json was not delivered to the volume")

    return result


if __name__ == "__main__":
    result = run()
    try:
        display(result["report"])       # noqa: F821  (display exists in notebooks)
        display(result["checks"])       # noqa: F821
    except NameError:
        print(result["report"].to_string(index=False))
        print(result["checks"].to_string(index=False))
    print("Six CSVs written to", cfg.BATCH_DIR)
    print("Manifest:", json.dumps(result["manifest"], indent=2))
