"""Contract tests for Phase 3 data generation.

Runnable with pytest, or standalone:  python tests/test_data_generation.py
(the orchestrator additionally exercises the full 13-check validation suite
against the real population and fraud registry)."""
import sys
import traceback
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from src.config import generator_config as cfg
from src.generators import (
    accounts, cards, customers, digital_banking, loans, support,
)
from src.utilities import identity as idn


def _fake_fraud_rows(n):
    """Synthetic stand-in for fraud_detection.silver.customers (read-only in
    production; tests never touch the real table)."""
    return [
        {"customer_id": f"CUST-{i + 1:06d}", "age": 25 + (i % 50),
         "country": ["PH", "US", "SG", "AU", "GB"][i % 5],
         "registration_date": f"{2015 + (i % 10)}-0{1 + (i % 9)}-15"}
        for i in range(n)
    ]


def test_normalize_email():
    assert idn.normalize_email("  Alice@Example.COM ") == "alice@example.com"
    assert idn.normalize_email("no-at-sign") is None
    assert idn.normalize_email("a b@x.y") is None
    assert idn.normalize_email(None) is None


def test_normalize_phone():
    # rule: strip non-digits, keep the LAST 10 (drops country/area prefixes)
    assert idn.normalize_phone("+63 917 123 4567") == "9171234567"
    assert idn.normalize_phone("(02) 8123 4567") == "0281234567"  # exactly 10 digits
    assert idn.normalize_phone("0917-123-4567") == "9171234567"   # 11 digits -> last 10
    assert idn.normalize_phone("123") is None


def test_normalize_name():
    assert idn.normalize_name("  O'Brien  Jr. ") == "O BRIEN JR"
    assert idn.normalize_name(None) is None


def test_parse_date_any():
    assert idn.parse_date_any("2026-11-30").isoformat() == "2026-11-30"
    assert idn.parse_date_any("30/11/2026").isoformat() == "2026-11-30"
    assert idn.parse_date_any("11-30-2026").isoformat() == "2026-11-30"
    assert idn.parse_date_any("garbage") is None


def test_raw_row_formula():
    # documented formula: masters x (1 + 2% + 1.5%) = masters x 1.035
    assert cfg.EXPECTED_RAW == {
        "customers": 12420, "accounts": 14490, "cards": 6831,
        "loans": 2898, "digital_logins": 56925, "support_cases": 9315,
    }


def test_population_seed_and_r5_domain():
    persons = idn.build_population(_fake_fraud_rows(10), n_new=5, n_seeded=10)
    assert len(persons) == 15
    seeded = [p for p in persons if p["origin"] == "FRAUD_SEED"]
    new = [p for p in persons if p["origin"] == "NEW"]
    assert len(seeded) == 10 and len(new) == 5
    # canonical IDs preserved
    assert {p["global_ref"] for p in seeded} == {
        f"CUST-{i + 1:06d}" for i in range(10)}
    # new persons: no global ref (R5 assigns CUST-100001+ in Phase 6 only)
    assert all(p["global_ref"] is None for p in new)
    assert all(p["email"] for p in persons)


def _tiny_persons():
    return idn.build_population(_fake_fraud_rows(200), n_new=50, n_seeded=200)


GEN_CASES = [
    ("customers", customers, {}),
    ("accounts", accounts, {"n_masters": 300}),
    ("cards", cards, {"n_masters": 150}),
    ("loans", loans, {"n_masters": 70}),
    ("digital_logins", digital_banking, {"n_masters": 400}),
    ("support_cases", support, {"n_masters": 200}),
]


def test_generators_deterministic_and_contract_shaped():
    persons = _tiny_persons()
    for name, mod, kw in GEN_CASES:
        # CIS covers ALL persons; product sources use explicit feasible sizes
        kwargs = {"n_masters": len(persons)} | kw
        a = mod.generate(persons, **kwargs)
        b = mod.generate(persons, **kwargs)
        assert a.equals(b), f"{name} not deterministic"
        assert list(a.columns) == mod.COLUMNS, f"{name} column contract"
        masters = kwargs["n_masters"]
        assert len(a) == cfg.raw_rows(masters), (
            f"{name} row count {len(a)} != {cfg.raw_rows(masters)}")


def test_orphans_and_conflicts_engineered():
    persons = _tiny_persons()  # 250 persons -> feasible accounts size
    df = accounts.generate(persons, n_masters=300)
    email_set = {p["email"] for p in persons}
    nem = df["customer_email"].map(idn.normalize_email)
    orphan_n = int(((df["global_customer_ref"].isna())
                    & (~nem.isin(email_set))).sum())
    assert orphan_n == round(300 * cfg.DEFECTS["orphan"])
    # conflicts: dual-key rows where email belongs to a different person
    ref_email = {p["global_ref"]: p["email"] for p in persons if p["global_ref"]}
    dual = df[df["global_customer_ref"].notna() & nem.notna()]
    conflicts = sum(
        1 for r, e in zip(dual["global_customer_ref"], dual["customer_email"])
        if ref_email.get(r) != idn.normalize_email(e))
    assert conflicts > 0


if __name__ == "__main__":
    failed = 0
    for name, fn in sorted(
            (n, f) for n, f in globals().items()
            if n.startswith("test_") and callable(f)):
        try:
            fn()
            print(f"PASS {name}")
        except Exception:
            failed += 1
            print(f"FAIL {name}")
            traceback.print_exc()
    print(f"{failed} failed")
    sys.exit(1 if failed else 0)
