"""Phase 4 unit tests -- config contracts and header validation.

Spark-dependent acceptance checks run in the orchestrator notebook
(spec section 8); these tests cover the pure functions and contracts.
Runnable with pytest or standalone: python tests/test_bronze_ingestion.py
"""
import sys
import traceback
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from src.config import bronze_config as cfg
from src.ingestion import bronze_loader as loader


def test_six_sources_defined():
    assert sorted(cfg.SOURCES) == [
        "accounts", "cards", "customers", "digital_logins", "loans",
        "support_tickets"]
    assert len(cfg.BUSINESS_COLUMNS) == 6


def test_column_counts_match_verified_headers():
    # verified against actual batch_001 headers on 2026-11-20
    assert {t: len(cols) for t, cols in cfg.BUSINESS_COLUMNS.items()} == {
        "customers": 18, "accounts": 12, "cards": 11, "loans": 13,
        "digital_logins": 12, "support_tickets": 11,
    }


def test_metadata_columns():
    assert [c for c, _ in cfg.METADATA_COLUMNS] == [
        "_source_system", "_batch_id", "_ingestion_timestamp",
        "_ingestion_date", "_source_file"]
    assert dict(cfg.METADATA_COLUMNS)["_ingestion_timestamp"] == "timestamp"
    assert dict(cfg.METADATA_COLUMNS)["_ingestion_date"] == "date"


def test_full_schema_business_then_metadata():
    full = cfg.full_schema("accounts")
    assert full[:12] == cfg.BUSINESS_COLUMNS["accounts"]
    assert full[12:] == [c for c, _ in cfg.METADATA_COLUMNS]


def test_support_tickets_maps_to_support_cases_file():
    # D1: table renamed; source FILE name unchanged
    assert cfg.SOURCES["support_tickets"][0] == "support_cases_raw.csv"
    assert cfg.SOURCES["support_tickets"][1] == "support_system"


def test_table_fqn():
    assert cfg.table_fqn("customers") == "customer_360.bronze.customers"


def test_header_verification(tmp_path=None):
    import tempfile, os
    d = tempfile.mkdtemp()
    p = os.path.join(d, "x.csv")
    with open(p, "w", encoding="utf-8") as f:
        f.write("account_id,cb_customer_ref\nACC-1,CB-1\n")
    assert loader.verify_header(p, "accounts") is not None  # wrong column set
    # exact match passes
    hdr = ",".join(cfg.BUSINESS_COLUMNS["loans"])
    with open(p, "w", encoding="utf-8") as f:
        f.write(hdr + "\nLON-1,LN-1\n")
    assert loader.verify_header(p, "loans") is None
    # reordered header fails with a mismatch description
    cols = cfg.BUSINESS_COLUMNS["loans"]
    swapped = [cols[1], cols[0]] + cols[2:]
    with open(p, "w", encoding="utf-8") as f:
        f.write(",".join(swapped) + "\nLON-1,LN-1\n")
    msg = loader.verify_header(p, "loans")
    assert msg and "position" in msg


def test_manifest_loadable():
    m = loader.load_manifest()
    assert m["batch_id"] == "batch_001"
    assert m["row_counts"]["customers"] == 12420


if __name__ == "__main__":
    failed = 0
    for name, fn in sorted(
            (n, f) for n, f in globals().items()
            if n.startswith("test_") and callable(f)):
        try:
            fn(); print(f"PASS  {name}")
        except Exception:
            failed += 1; print(f"FAIL  {name}"); traceback.print_exc()
    print(f"{failed} failed")
    sys.exit(1 if failed else 0)
