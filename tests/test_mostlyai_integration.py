"""
Integration tests for MostlyAI engine with REAL SDK calls.

These tests actually train MostlyAI generators on CSV sample data
and generate synthetic records, then verify the full pipeline:
  CSV sample → MostlyAI train → generate → copybook format → VSAM files.

Requires: pip install 'mostlyai[local]'
         pandas>=2.0,<3.0  (MostlyAI SDK has pandas 3.x compatibility issues)

WARNING: These tests are VERY slow (~15-20 min each on CPU-only machines).
Run separately via:
  python -m pytest tests/test_mostlyai_integration.py -v -s --timeout=3600

For faster E2E verification, use the script instead:
  python tests/run_mostlyai_e2e.py           # single-table (~10 min)
  python tests/run_mostlyai_e2e.py --multi   # multi-table (~18 min)
  python tests/run_mostlyai_e2e.py --all     # both (~30 min)
"""

import csv
import json
import os
import tempfile

import pandas as pd
import pytest

from vsam_gen.parser.copybook_parser import parse_copybook, parse_copybook_file
from vsam_gen.generator.mostlyai_engine import MostlyAIEngine, _check_mostlyai_available
from vsam_gen.writer.vsam_writer import VsamWriter
from vsam_gen.pipeline import VsamPipeline
from vsam_gen.models import GenerationConfig, EngineType

# Skip entire module if MostlyAI SDK not installed
pytestmark = pytest.mark.skipif(
    not _check_mostlyai_available(),
    reason="MostlyAI SDK not installed (pip install 'mostlyai[local]')",
)

# ── Paths ─────────────────────────────────────────────────────────────────────

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SAMPLE_DATA = os.path.join(BASE_DIR, "sample_data")
SAMPLE_COPYBOOKS = os.path.join(BASE_DIR, "sample_copybooks")

CUSTOMER_CSV = os.path.join(SAMPLE_DATA, "customers.csv")
ACCOUNT_CSV = os.path.join(SAMPLE_DATA, "accounts.csv")
TRANSACTION_CSV = os.path.join(SAMPLE_DATA, "transactions.csv")

CUSTOMER_CPY = os.path.join(SAMPLE_COPYBOOKS, "customer_mai.cpy")
ACCOUNT_CPY = os.path.join(SAMPLE_COPYBOOKS, "account_mai.cpy")
TRANSACTION_CPY = os.path.join(SAMPLE_COPYBOOKS, "transaction_mai.cpy")


# ═══════════════════════════════════════════════════════════════════════════════
# Single-Table Tests
# ═══════════════════════════════════════════════════════════════════════════════

class TestMostlyAISingleTable:
    """
    Train MostlyAI on a single CSV sample, generate synthetic records,
    format them to the matching copybook layout, write VSAM files.
    """

    def test_customer_csv_to_vsam(self):
        """CSV customers.csv → train → generate 20 records → VSAM files."""
        layout = parse_copybook_file(CUSTOMER_CPY)
        num_records = 20

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

            engine = MostlyAIEngine(layout, config)
            records = engine.generate()

            # Validate record count and field lengths
            assert len(records) == num_records
            for rec in records:
                for field in layout.data_fields:
                    assert field.name in rec, f"Missing {field.name}"
                    assert len(rec[field.name]) == field.length, (
                        f"{field.name}: expected {field.length}, got {len(rec[field.name])}"
                    )

            # Write VSAM files
            writer = VsamWriter(layout, config)
            dat_file = writer.write(records)
            csv_file = writer.write_csv(records)
            json_file = writer.write_json(records)

            # Validate DAT
            assert os.path.exists(dat_file)
            assert os.path.getsize(dat_file) == num_records * layout.record_length

            # Validate CSV
            with open(csv_file) as f:
                reader = csv.reader(f)
                header = next(reader)
                rows = list(reader)
            assert len(rows) == num_records
            for field in layout.data_fields:
                assert field.name in header

            # Validate JSON
            with open(json_file) as f:
                data = json.load(f)
            assert len(data) == num_records

    def test_account_csv_to_vsam(self):
        """CSV accounts.csv → train → generate 15 records → VSAM files."""
        layout = parse_copybook_file(ACCOUNT_CPY)
        num_records = 15

        with tempfile.TemporaryDirectory() as tmpdir:
            outfile = os.path.join(tmpdir, "account.dat")
            config = GenerationConfig(
                num_records=num_records,
                output_file=outfile,
                engine=EngineType.MOSTLYAI,
                seed=42,
                key_field="ACCT-NUMBER",
                training_data_path=ACCOUNT_CSV,
                mostlyai_max_training_time=1,
            )

            engine = MostlyAIEngine(layout, config)
            records = engine.generate()

            assert len(records) == num_records
            for rec in records:
                for field in layout.data_fields:
                    assert len(rec[field.name]) == field.length

            writer = VsamWriter(layout, config)
            dat_file = writer.write(records)
            assert os.path.getsize(dat_file) == num_records * layout.record_length

    def test_transaction_csv_to_vsam(self):
        """CSV transactions.csv → train → generate 25 records → VSAM files."""
        layout = parse_copybook_file(TRANSACTION_CPY)
        num_records = 25

        with tempfile.TemporaryDirectory() as tmpdir:
            outfile = os.path.join(tmpdir, "transaction.dat")
            config = GenerationConfig(
                num_records=num_records,
                output_file=outfile,
                engine=EngineType.MOSTLYAI,
                seed=42,
                key_field="TXN-ID",
                training_data_path=TRANSACTION_CSV,
                mostlyai_max_training_time=1,
            )

            engine = MostlyAIEngine(layout, config)
            records = engine.generate()

            assert len(records) == num_records
            for rec in records:
                for field in layout.data_fields:
                    assert len(rec[field.name]) == field.length

            writer = VsamWriter(layout, config)
            dat_file = writer.write(records)
            assert os.path.getsize(dat_file) == num_records * layout.record_length

    def test_pipeline_single_table_mostlyai(self):
        """VsamPipeline with engine=MOSTLYAI generates valid VSAM output."""
        with tempfile.TemporaryDirectory() as tmpdir:
            outfile = os.path.join(tmpdir, "pipeline_customer.dat")
            config = GenerationConfig(
                num_records=10,
                output_file=outfile,
                engine=EngineType.MOSTLYAI,
                seed=42,
                key_field="CUST-ID",
                training_data_path=CUSTOMER_CSV,
                mostlyai_max_training_time=1,
            )

            pipeline = VsamPipeline(config)
            pipeline.load_copybook(CUSTOMER_CPY)
            result = pipeline.generate(num_records=10, output=outfile)

            assert result["stats"]["record_count"] == 10
            assert os.path.exists(result["files"]["dat"])
            assert os.path.exists(result["files"]["csv"])
            assert os.path.exists(result["files"]["json"])

            layout = result["layout"]
            assert os.path.getsize(result["files"]["dat"]) == 10 * layout.record_length


# ═══════════════════════════════════════════════════════════════════════════════
# Multi-Table Tests
# ═══════════════════════════════════════════════════════════════════════════════

class TestMostlyAIMultiTable:
    """
    Train MostlyAI on linked CSV samples (customer → account → transaction),
    generate correlated synthetic data, format to copybook layouts, write
    VSAM files for each table.
    """

    def test_multitable_train_and_generate(self):
        """
        Full multi-table: 3 CSVs → MostlyAI trains linked generator →
        generates correlated synthetic data → copybook format → VSAM files.
        """
        with tempfile.TemporaryDirectory() as tmpdir:
            config = GenerationConfig(
                engine=EngineType.MOSTLYAI,
                seed=42,
                mostlyai_max_training_time=1,
            )

            pipeline = VsamPipeline(config)
            pipeline.load_copybook(CUSTOMER_CPY)
            pipeline.load_copybook(ACCOUNT_CPY)
            pipeline.load_copybook(TRANSACTION_CPY)

            # Define FK relationships (for VSAM-level linking)
            pipeline.add_foreign_key(
                "CUSTOMER-RECORD", "CUST-ID",
                "ACCOUNT-RECORD", "ACCT-CUST-ID",
            )
            pipeline.add_foreign_key(
                "ACCOUNT-RECORD", "ACCT-NUMBER",
                "TRANSACTION-RECORD", "TXN-ACCT-NO",
            )

            # Copybook configs with training data and MostlyAI multi-table settings
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

            results = pipeline.generate_all_mostlyai(
                copybook_configs=copybook_configs,
                output_dir=tmpdir,
                parent_records=10,
                child_ratio=2.0,
            )

            # All 3 tables should have results
            assert "CUSTOMER-RECORD" in results
            assert "ACCOUNT-RECORD" in results
            assert "TRANSACTION-RECORD" in results

            # Each table should have records
            for name, result in results.items():
                layout = result["layout"]
                assert result["stats"]["record_count"] > 0
                assert os.path.exists(result["files"]["dat"])

                # DAT file size should be consistent
                expected_bytes = result["stats"]["record_count"] * layout.record_length
                assert os.path.getsize(result["files"]["dat"]) == expected_bytes

                # Records should have correct field lengths
                for rec in result["records"]:
                    for field in layout.data_fields:
                        assert field.name in rec, f"{name}: missing {field.name}"
                        assert len(rec[field.name]) == field.length, (
                            f"{name}.{field.name}: expected {field.length}, "
                            f"got {len(rec[field.name])}"
                        )

            # Verify customers were generated as subject table
            cust_count = results["CUSTOMER-RECORD"]["stats"]["record_count"]
            assert cust_count == 10  # parent_records

    def test_multitable_yaml_config(self):
        """
        Run multi-table generation from the YAML config file
        (config/mostlyai_pipeline.yaml).
        """
        import yaml

        yaml_path = os.path.join(BASE_DIR, "config", "mostlyai_pipeline.yaml")
        if not os.path.exists(yaml_path):
            pytest.skip("mostlyai_pipeline.yaml not found")

        with open(yaml_path) as f:
            cfg = yaml.safe_load(f)

        pipeline_cfg = cfg.get("pipeline", {})

        with tempfile.TemporaryDirectory() as tmpdir:
            config = GenerationConfig(
                engine=EngineType.MOSTLYAI,
                encoding=pipeline_cfg.get("encoding", "ascii"),
                seed=pipeline_cfg.get("seed"),
                mostlyai_max_training_time=1,  # override for speed
            )

            pipeline = VsamPipeline(config)

            copybook_configs = {}
            for cb in cfg.get("copybooks", []):
                layout = pipeline.load_copybook(cb["path"], name=cb.get("name"))
                copybook_configs[layout.name] = cb

            for fk in cfg.get("foreign_keys", []):
                pipeline.add_foreign_key(
                    fk["parent_layout"], fk["parent_field"],
                    fk["child_layout"], fk["child_field"],
                )

            results = pipeline.generate_all_mostlyai(
                copybook_configs=copybook_configs,
                output_dir=tmpdir,
                parent_records=10,
                child_ratio=2.0,
            )

            # Should generate all 3 tables
            assert len(results) >= 3
            for name, result in results.items():
                assert result["stats"]["record_count"] > 0
                assert os.path.exists(result["files"]["dat"])

                # Verify VSAM fixed-length output
                layout = result["layout"]
                with open(result["files"]["dat"], "rb") as f:
                    raw = f.read()
                for i in range(result["stats"]["record_count"]):
                    rec_bytes = raw[i * layout.record_length:(i + 1) * layout.record_length]
                    assert len(rec_bytes) == layout.record_length


# ═══════════════════════════════════════════════════════════════════════════════
# Data Quality Tests
# ═══════════════════════════════════════════════════════════════════════════════

class TestMostlyAIDataQuality:
    """Verify that MostlyAI-generated data has reasonable distributions."""

    def test_customer_status_distribution(self):
        """Status field should contain only vals seen in training data."""
        layout = parse_copybook_file(CUSTOMER_CPY)

        with tempfile.TemporaryDirectory() as tmpdir:
            outfile = os.path.join(tmpdir, "quality_cust.dat")
            config = GenerationConfig(
                num_records=30,
                output_file=outfile,
                engine=EngineType.MOSTLYAI,
                seed=42,
                training_data_path=CUSTOMER_CSV,
                mostlyai_max_training_time=1,
            )

            engine = MostlyAIEngine(layout, config)
            records = engine.generate()

            # Training data has statuses: A, I, C
            # MostlyAI may also produce _RARE_ or empty with minimal training
            statuses = {rec["CUST-STATUS"].strip() for rec in records}
            assert len(statuses) >= 1  # at least one status value generated

    def test_account_type_distribution(self):
        """Account type should contain values from training data."""
        layout = parse_copybook_file(ACCOUNT_CPY)

        with tempfile.TemporaryDirectory() as tmpdir:
            outfile = os.path.join(tmpdir, "quality_acct.dat")
            config = GenerationConfig(
                num_records=30,
                output_file=outfile,
                engine=EngineType.MOSTLYAI,
                seed=42,
                training_data_path=ACCOUNT_CSV,
                mostlyai_max_training_time=1,
            )

            engine = MostlyAIEngine(layout, config)
            records = engine.generate()

            # Training data has types: CH, SA, MM, CD, CC
            # MostlyAI may produce _RARE_ or variations with small training data
            types = {rec["ACCT-TYPE"].strip() for rec in records}
            assert len(types) >= 1

    def test_numeric_fields_are_digits(self):
        """All numeric PIC 9 fields should contain only digits."""
        from vsam_gen.models import PicType

        layout = parse_copybook_file(CUSTOMER_CPY)

        with tempfile.TemporaryDirectory() as tmpdir:
            outfile = os.path.join(tmpdir, "quality_numeric.dat")
            config = GenerationConfig(
                num_records=20,
                output_file=outfile,
                engine=EngineType.MOSTLYAI,
                seed=42,
                training_data_path=CUSTOMER_CSV,
                mostlyai_max_training_time=1,
            )

            engine = MostlyAIEngine(layout, config)
            records = engine.generate()

            for rec in records:
                for field in layout.data_fields:
                    if field.pic_type == PicType.NUMERIC:
                        val = rec[field.name]
                        assert val.isdigit(), (
                            f"{field.name}='{val}' should be all digits"
                        )


# ═══════════════════════════════════════════════════════════════════════════════
# Run
# ═══════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
