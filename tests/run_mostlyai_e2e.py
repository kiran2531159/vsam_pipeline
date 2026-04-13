#!/usr/bin/env python3
"""
End-to-end MostlyAI integration test script.

Runs the full pipeline: CSV sample → MostlyAI train → generate → copybook format → VSAM files.

Usage:
    python tests/run_mostlyai_e2e.py              # Single-table customer test
    python tests/run_mostlyai_e2e.py --multi      # Multi-table linked test
    python tests/run_mostlyai_e2e.py --all        # Both single + multi-table
"""

import argparse
import csv
import json
import os
import sys
import tempfile
import time
import traceback

# Add project root to path
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, BASE_DIR)

from vsam_gen.parser.copybook_parser import parse_copybook_file
from vsam_gen.generator.mostlyai_engine import MostlyAIEngine
from vsam_gen.writer.vsam_writer import VsamWriter
from vsam_gen.pipeline import VsamPipeline
from vsam_gen.models import GenerationConfig, EngineType, PicType

# ── Paths ─────────────────────────────────────────────────────────────────────

SAMPLE_DATA = os.path.join(BASE_DIR, "sample_data")
SAMPLE_COPYBOOKS = os.path.join(BASE_DIR, "sample_copybooks")

CUSTOMER_CSV = os.path.join(SAMPLE_DATA, "customers.csv")
ACCOUNT_CSV = os.path.join(SAMPLE_DATA, "accounts.csv")
TRANSACTION_CSV = os.path.join(SAMPLE_DATA, "transactions.csv")

CUSTOMER_CPY = os.path.join(SAMPLE_COPYBOOKS, "customer_mai.cpy")
ACCOUNT_CPY = os.path.join(SAMPLE_COPYBOOKS, "account_mai.cpy")
TRANSACTION_CPY = os.path.join(SAMPLE_COPYBOOKS, "transaction_mai.cpy")
COMBINED_CPY = os.path.join(SAMPLE_COPYBOOKS, "combined_mai.cpy")


def timed(label):
    """Context manager to time a block."""
    class Timer:
        def __init__(self):
            self.start = 0
            self.elapsed = 0
        def __enter__(self):
            self.start = time.time()
            print(f"\n{'─'*60}")
            print(f"▶ {label}")
            print(f"{'─'*60}")
            return self
        def __exit__(self, *args):
            self.elapsed = time.time() - self.start
            print(f"  ✓ {label} completed in {self.elapsed:.1f}s")
    return Timer()


def assert_eq(actual, expected, msg=""):
    if actual != expected:
        raise AssertionError(f"{msg}: expected {expected}, got {actual}")


def assert_true(condition, msg=""):
    if not condition:
        raise AssertionError(f"Assertion failed: {msg}")


# ═══════════════════════════════════════════════════════════════════════════════
# Test 1: Single-table customer CSV → VSAM
# ═══════════════════════════════════════════════════════════════════════════════

def test_single_table_customer():
    """CSV customers.csv → MostlyAI train → generate 10 records → VSAM files."""
    print("\n" + "=" * 70)
    print("TEST 1: Single-Table Customer CSV → MostlyAI → VSAM")
    print("=" * 70)

    layout = parse_copybook_file(CUSTOMER_CPY)
    print(f"  Copybook: {layout.name}, {len(layout.data_fields)} fields, "
          f"record_length={layout.record_length}")

    num_records = 10

    with tempfile.TemporaryDirectory() as tmpdir:
        outfile = os.path.join(tmpdir, "customer.dat")
        config = GenerationConfig(
            num_records=num_records,
            output_file=outfile,
            engine=EngineType.MOSTLYAI,
            seed=42,
            key_field="CUST-ID",
            training_data_path=CUSTOMER_CSV,
            mostlyai_max_training_time=1,
        )

        with timed("MostlyAI Engine generate()"):
            engine = MostlyAIEngine(layout, config)
            records = engine.generate()

        # ── Validate records ──────────────────────────────────────────
        print(f"\n  Generated {len(records)} records")
        assert_eq(len(records), num_records, "Record count")

        for i, rec in enumerate(records):
            for field in layout.data_fields:
                assert_true(field.name in rec, f"Record {i}: missing {field.name}")
                assert_eq(
                    len(rec[field.name]), field.length,
                    f"Record {i}, {field.name} length"
                )

        print("  ✓ All records have correct fields and lengths")

        # ── Write VSAM files ──────────────────────────────────────────
        with timed("Write VSAM files"):
            writer = VsamWriter(layout, config)
            dat_file = writer.write(records)
            csv_file = writer.write_csv(records)
            json_file = writer.write_json(records)

        # ── Validate DAT ──────────────────────────────────────────────
        dat_size = os.path.getsize(dat_file)
        expected_size = num_records * layout.record_length
        assert_eq(dat_size, expected_size, "DAT file size")
        print(f"  ✓ DAT: {dat_file} ({dat_size} bytes, {num_records} × {layout.record_length})")

        # ── Validate CSV ──────────────────────────────────────────────
        with open(csv_file) as f:
            reader = csv.reader(f)
            header = next(reader)
            rows = list(reader)
        assert_eq(len(rows), num_records, "CSV row count")
        for field in layout.data_fields:
            assert_true(field.name in header, f"CSV header missing {field.name}")
        print(f"  ✓ CSV: {csv_file} ({len(rows)} rows, {len(header)} columns)")

        # ── Validate JSON ─────────────────────────────────────────────
        with open(json_file) as f:
            data = json.load(f)
        assert_eq(len(data), num_records, "JSON record count")
        print(f"  ✓ JSON: {json_file} ({len(data)} records)")

        # ── Validate data quality ─────────────────────────────────────
        statuses = {rec["CUST-STATUS"].strip() for rec in records}
        print(f"  Data quality:")
        print(f"    - Status values: {statuses}")

        # Verify numeric fields are digits
        for rec in records:
            for field in layout.data_fields:
                if field.pic_type == PicType.NUMERIC:
                    val = rec[field.name]
                    assert_true(val.isdigit(), f"{field.name}='{val}' should be digits")
        print(f"    - ✓ All numeric fields contain only digits")

        # Print sample records
        print(f"\n  Sample records:")
        for i in range(min(3, len(records))):
            r = records[i]
            print(f"    [{i}] ID={r['CUST-ID']} NAME={r['CUST-FIRST-NAME'].strip()} "
                  f"{r['CUST-LAST-NAME'].strip()} STATUS={r['CUST-STATUS'].strip()}")

    print("\n  ✅ TEST 1 PASSED")
    return True


# ═══════════════════════════════════════════════════════════════════════════════
# Test 2: Multi-table linked generation
# ═══════════════════════════════════════════════════════════════════════════════

def test_multi_table():
    """3 linked CSVs → MostlyAI multi-table train → generate → VSAM files."""
    print("\n" + "=" * 70)
    print("TEST 2: Multi-Table Linked Generation (Customer→Account→Transaction)")
    print("=" * 70)

    with tempfile.TemporaryDirectory() as tmpdir:
        config = GenerationConfig(
            engine=EngineType.MOSTLYAI,
            seed=42,
            mostlyai_max_training_time=1,
        )

        with timed("Pipeline setup"):
            pipeline = VsamPipeline(config)
            pipeline.load_copybook(CUSTOMER_CPY)
            pipeline.load_copybook(ACCOUNT_CPY)
            pipeline.load_copybook(TRANSACTION_CPY)

            pipeline.add_foreign_key(
                "CUSTOMER-RECORD", "CUST-ID",
                "ACCOUNT-RECORD", "ACCT-CUST-ID",
            )
            pipeline.add_foreign_key(
                "ACCOUNT-RECORD", "ACCT-NUMBER",
                "TRANSACTION-RECORD", "TXN-ACCT-NO",
            )

            print(f"  Loaded {len(pipeline.layouts)} layouts")
            for name, layout in pipeline.layouts.items():
                print(f"    - {name}: {len(layout.data_fields)} fields, "
                      f"record_length={layout.record_length}")

        copybook_configs = {
            "CUSTOMER-RECORD": {
                "training_data": CUSTOMER_CSV,
                "key_field": "CUST-ID",
                "primary_key": "CUST-ID",
            },
            "ACCOUNT-RECORD": {
                "training_data": ACCOUNT_CSV,
                "key_field": "ACCT-NUMBER",
                "primary_key": "ACCT-NUMBER",
                "foreign_keys": [{
                    "column": "ACCT-CUST-ID",
                    "referenced_table": "CUSTOMER-RECORD",
                    "is_context": True,
                }],
            },
            "TRANSACTION-RECORD": {
                "training_data": TRANSACTION_CSV,
                "key_field": "TXN-ID",
                "foreign_keys": [{
                    "column": "TXN-ACCT-NO",
                    "referenced_table": "ACCOUNT-RECORD",
                    "is_context": True,
                }],
            },
        }

        with timed("MostlyAI multi-table train + generate"):
            results = pipeline.generate_all_mostlyai(
                copybook_configs=copybook_configs,
                output_dir=tmpdir,
                parent_records=5,
                child_ratio=2.0,
            )

        # ── Validate results ──────────────────────────────────────────
        print(f"\n  Generated {len(results)} tables:")
        for name, result in results.items():
            layout = result["layout"]
            count = result["stats"]["record_count"]
            print(f"    - {name}: {count} records, {len(layout.data_fields)} fields")

            # Check DAT file
            assert_true(os.path.exists(result["files"]["dat"]),
                        f"{name}: DAT file not found")
            expected_bytes = count * layout.record_length
            actual_bytes = os.path.getsize(result["files"]["dat"])
            assert_eq(actual_bytes, expected_bytes, f"{name} DAT size")

            # Check field lengths
            for rec in result["records"]:
                for field in layout.data_fields:
                    assert_true(field.name in rec, f"{name}: missing {field.name}")
                    assert_eq(
                        len(rec[field.name]), field.length,
                        f"{name}.{field.name} length"
                    )

        assert_true("CUSTOMER-RECORD" in results, "Customer results missing")
        assert_true("ACCOUNT-RECORD" in results, "Account results missing")
        assert_true("TRANSACTION-RECORD" in results, "Transaction results missing")

        # Customer count should match parent_records
        cust_count = results["CUSTOMER-RECORD"]["stats"]["record_count"]
        assert_eq(cust_count, 5, "Customer record count (parent_records)")

        print(f"\n  ✓ All 3 tables generated with correct field lengths")
        print(f"  ✓ DAT files sizes are consistent")

        # ══════════════════════════════════════════════════════════════
        # Combined VSAM merge
        # ══════════════════════════════════════════════════════════════
        with timed("Merge to combined VSAM"):
            record_type_map = {
                "CUSTOMER-RECORD": "CU",
                "ACCOUNT-RECORD": "AC",
                "TRANSACTION-RECORD": "TX",
            }
            combined = pipeline.merge_to_combined_vsam(
                results=results,
                output_dir=tmpdir,
                combined_copybook=COMBINED_CPY,
                record_type_map=record_type_map,
            )

        total_combined = combined["stats"]["total_records"]
        combined_rec_len = combined["stats"]["combined_record_length"]
        type_counts = combined["stats"]["record_type_counts"]
        print(f"\n  Combined VSAM:")
        print(f"    Total records: {total_combined}")
        print(f"    Record length: {combined_rec_len} bytes")
        print(f"    Type counts:   {type_counts}")

        # Validate combined DAT
        combined_dat = combined["files"]["dat"]
        assert_true(os.path.exists(combined_dat), "Combined DAT not found")
        expected_size = total_combined * combined_rec_len
        actual_size = os.path.getsize(combined_dat)
        assert_eq(actual_size, expected_size, "Combined DAT size")
        print(f"    ✓ DAT: {combined_dat} ({actual_size:,} bytes)")

        # Validate combined CSV
        combined_csv = combined["files"]["csv"]
        with open(combined_csv) as f:
            reader = csv.reader(f)
            header = next(reader)
            rows = list(reader)
        assert_eq(len(rows), total_combined, "Combined CSV row count")
        assert_true("REC-TYPE" in header, "CSV missing REC-TYPE column")
        print(f"    ✓ CSV: {combined_csv} ({len(rows)} rows)")

        # Validate combined JSON
        combined_json = combined["files"]["json"]
        with open(combined_json) as f:
            jdata = json.load(f)
        assert_eq(len(jdata), total_combined, "Combined JSON record count")
        # Check record types present
        json_types = {r["REC-TYPE"] for r in jdata}
        assert_true("CU" in json_types, "JSON missing CU records")
        assert_true("AC" in json_types, "JSON missing AC records")
        assert_true("TX" in json_types, "JSON missing TX records")
        print(f"    ✓ JSON: {combined_json} ({len(jdata)} records, types: {json_types})")

        # Verify hierarchical ordering: CU should come before its AC children
        # and AC before its TX children
        last_type_order = {"CU": 0, "AC": 1, "TX": 2}
        for i, entry in enumerate(combined["combined_records"]):
            rec = entry["_record"]
            rtype = rec["REC-TYPE"].strip()
            assert_true(rtype in ("CU", "AC", "TX"),
                        f"Record {i}: unexpected type '{rtype}'")

        # Count matches: sum of per-table records should equal combined total
        sum_individual = sum(r["stats"]["record_count"] for r in results.values())
        assert_eq(total_combined, sum_individual,
                  "Combined total should equal sum of individual tables")
        print(f"    ✓ Record count: {total_combined} = sum of individual tables")

        # Verify CU count in combined matches
        assert_eq(type_counts.get("CU", 0), cust_count, "CU count in combined")
        print(f"    ✓ Hierarchical interleaving verified")

    print("\n  ✅ TEST 2 PASSED (including combined VSAM merge)")
    return True


# ═══════════════════════════════════════════════════════════════════════════════
# Main
# ═══════════════════════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(description="MostlyAI E2E Integration Tests")
    parser.add_argument("--multi", action="store_true", help="Run multi-table test only")
    parser.add_argument("--all", action="store_true", help="Run all tests")
    args = parser.parse_args()

    t0 = time.time()
    results = {}

    try:
        if args.multi:
            results["multi"] = test_multi_table()
        elif args.all:
            results["single"] = test_single_table_customer()
            results["multi"] = test_multi_table()
        else:
            results["single"] = test_single_table_customer()
    except Exception as e:
        print(f"\n  ❌ FAILED: {e}")
        traceback.print_exc()
        results["error"] = str(e)

    total = time.time() - t0
    print(f"\n{'='*70}")
    print(f"SUMMARY (total: {total:.0f}s / {total/60:.1f}min)")
    print(f"{'='*70}")
    for name, passed in results.items():
        status = "✅ PASSED" if passed is True else f"❌ FAILED: {passed}"
        print(f"  {name}: {status}")

    return all(v is True for v in results.values())


if __name__ == "__main__":
    sys.exit(0 if main() else 1)
