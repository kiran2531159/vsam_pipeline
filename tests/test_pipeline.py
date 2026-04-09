"""
Test suite for the VSAM Generation Pipeline.
"""

import json
import os
import tempfile
import pytest

from vsam_gen.parser.copybook_parser import parse_copybook, parse_copybook_file
from vsam_gen.generator.ai_inferrer import infer_semantic_type, infer_all_fields
from vsam_gen.generator.synthetic_engine import SyntheticEngine
from vsam_gen.writer.vsam_writer import VsamWriter
from vsam_gen.pipeline import VsamPipeline
from vsam_gen.models import CopybookField, GenerationConfig, PicType, VsamOrgType, EngineType


# ── Sample copybook for tests ───────────────────────────────────────────────

SAMPLE_COPYBOOK = """
       01  TEST-RECORD.
           05  TEST-ID                    PIC 9(08).
           05  TEST-NAME                  PIC X(20).
           05  TEST-AMOUNT                PIC S9(07)V99.
           05  TEST-DATE                  PIC 9(08).
           05  TEST-STATUS                PIC X(01).
           05  TEST-CODE                  PIC X(03).
           05  FILLER                     PIC X(10).
"""

CUSTOMER_COPYBOOK = """
       01  CUSTOMER-RECORD.
           05  CUST-ID                    PIC 9(10).
           05  CUST-FIRST-NAME            PIC X(25).
           05  CUST-LAST-NAME             PIC X(30).
           05  CUST-DOB                   PIC 9(08).
           05  CUST-SSN                   PIC 9(09).
           05  CUST-CITY                  PIC X(25).
           05  CUST-STATE                 PIC X(02).
           05  CUST-ZIP-CODE              PIC 9(05).
           05  CUST-PHONE                 PIC 9(10).
           05  CUST-STATUS                PIC X(01).
"""


# ═══════════════════════════════════════════════════════════════════════════════
# Parser Tests
# ═══════════════════════════════════════════════════════════════════════════════

class TestCopybookParser:

    def test_parse_basic_copybook(self):
        layout = parse_copybook(SAMPLE_COPYBOOK, "TEST")
        assert layout.name == "TEST"
        assert layout.record_length > 0

    def test_parse_field_count(self):
        layout = parse_copybook(SAMPLE_COPYBOOK)
        data_fields = layout.data_fields
        # Should have: TEST-ID, TEST-NAME, TEST-AMOUNT, TEST-DATE, TEST-STATUS, TEST-CODE
        assert len(data_fields) == 6

    def test_parse_numeric_field(self):
        layout = parse_copybook(SAMPLE_COPYBOOK)
        id_field = next(f for f in layout.fields if f.name == "TEST-ID")
        assert id_field.pic_type == PicType.NUMERIC
        assert id_field.length == 8

    def test_parse_alphanumeric_field(self):
        layout = parse_copybook(SAMPLE_COPYBOOK)
        name_field = next(f for f in layout.fields if f.name == "TEST-NAME")
        assert name_field.pic_type == PicType.ALPHANUMERIC
        assert name_field.length == 20

    def test_parse_signed_decimal(self):
        layout = parse_copybook(SAMPLE_COPYBOOK)
        amt_field = next(f for f in layout.fields if f.name == "TEST-AMOUNT")
        assert amt_field.pic_type == PicType.SIGNED_NUMERIC
        assert amt_field.decimal_places == 2
        assert amt_field.length == 9  # 7 integer + 2 decimal

    def test_parse_record_length(self):
        layout = parse_copybook(SAMPLE_COPYBOOK)
        # 8 + 20 + 9 + 8 + 1 + 3 + 10 = 59
        assert layout.record_length == 59

    def test_parse_offsets_sequential(self):
        layout = parse_copybook(SAMPLE_COPYBOOK)
        flat = layout.flat_fields
        for i in range(1, len(flat)):
            if flat[i].redefines is None:
                assert flat[i].offset >= flat[i - 1].offset

    def test_parse_filler_detected(self):
        layout = parse_copybook(SAMPLE_COPYBOOK)
        fillers = [f for f in layout.fields if f.is_filler]
        assert len(fillers) == 1
        assert fillers[0].length == 10

    def test_parse_group_level(self):
        layout = parse_copybook(SAMPLE_COPYBOOK)
        groups = [f for f in layout.fields if f.is_group]
        assert len(groups) == 1  # 01 TEST-RECORD

    def test_parse_from_file(self):
        with tempfile.NamedTemporaryFile(mode="w", suffix=".cpy", delete=False) as f:
            f.write(SAMPLE_COPYBOOK)
            f.flush()
            layout = parse_copybook_file(f.name)
        os.unlink(f.name)
        assert layout.record_length > 0


# ═══════════════════════════════════════════════════════════════════════════════
# AI Inferrer Tests
# ═══════════════════════════════════════════════════════════════════════════════

class TestAIInferrer:

    def _make_field(self, name, pic_type=PicType.ALPHANUMERIC, length=10, decimal=0):
        return CopybookField(level=5, name=name, pic_type=pic_type,
                             length=length, decimal_places=decimal)

    def test_infer_first_name(self):
        f = self._make_field("CUST-FIRST-NAME")
        assert infer_semantic_type(f) == "first_name"

    def test_infer_last_name(self):
        f = self._make_field("EMP-LAST-NAME")
        assert infer_semantic_type(f) == "last_name"

    def test_infer_ssn(self):
        f = self._make_field("CUST-SSN", PicType.NUMERIC, 9)
        assert infer_semantic_type(f) == "ssn"

    def test_infer_date(self):
        f = self._make_field("TRANS-DATE", PicType.NUMERIC, 8)
        assert infer_semantic_type(f) == "date_recent"

    def test_infer_city(self):
        f = self._make_field("CUST-CITY")
        assert infer_semantic_type(f) == "city"

    def test_infer_phone(self):
        f = self._make_field("CUST-PHONE", PicType.NUMERIC, 10)
        assert infer_semantic_type(f) == "phone_number"

    def test_infer_status(self):
        f = self._make_field("ACCT-STATUS", PicType.ALPHANUMERIC, 1)
        assert infer_semantic_type(f) == "status_code"

    def test_infer_amount(self):
        f = self._make_field("TXN-AMOUNT", PicType.SIGNED_NUMERIC, 9, 2)
        assert infer_semantic_type(f) == "currency_amount"

    def test_infer_all_fields(self):
        layout = parse_copybook(CUSTOMER_COPYBOOK)
        infer_all_fields(layout.fields)
        inferred = [f for f in layout.data_fields if f.semantic_type is not None]
        assert len(inferred) > 0

    def test_fallback_alpha(self):
        f = self._make_field("SOME-RANDOM-FIELD", PicType.ALPHANUMERIC, 15)
        sem = infer_semantic_type(f)
        assert sem in ("alpha_text", "text")


# ═══════════════════════════════════════════════════════════════════════════════
# Synthetic Engine Tests
# ═══════════════════════════════════════════════════════════════════════════════

class TestSyntheticEngine:

    def test_generate_records(self):
        layout = parse_copybook(SAMPLE_COPYBOOK)
        config = GenerationConfig(num_records=10, seed=42)
        engine = SyntheticEngine(layout, config)
        records = engine.generate()
        assert len(records) == 10

    def test_record_has_all_fields(self):
        layout = parse_copybook(SAMPLE_COPYBOOK)
        config = GenerationConfig(num_records=1, seed=42)
        engine = SyntheticEngine(layout, config)
        records = engine.generate()
        record = records[0]
        for f in layout.data_fields:
            assert f.name in record

    def test_field_length_respected(self):
        layout = parse_copybook(SAMPLE_COPYBOOK)
        config = GenerationConfig(num_records=5, seed=42)
        engine = SyntheticEngine(layout, config)
        records = engine.generate()
        for record in records:
            for f in layout.data_fields:
                val = str(record[f.name])
                assert len(val) == f.length, f"{f.name}: expected {f.length}, got {len(val)}"

    def test_unique_keys(self):
        layout = parse_copybook(SAMPLE_COPYBOOK)
        config = GenerationConfig(num_records=100, seed=42, key_field="TEST-ID")
        engine = SyntheticEngine(layout, config)
        records = engine.generate()
        keys = [r["TEST-ID"] for r in records]
        assert len(set(keys)) == 100

    def test_reproducible_with_seed(self):
        layout = parse_copybook(SAMPLE_COPYBOOK)
        config1 = GenerationConfig(num_records=5, seed=123)
        config2 = GenerationConfig(num_records=5, seed=123)
        r1 = SyntheticEngine(layout, config1).generate()
        r2 = SyntheticEngine(layout, config2).generate()
        for a, b in zip(r1, r2):
            assert a == b

    def test_field_override(self):
        layout = parse_copybook(SAMPLE_COPYBOOK)
        config = GenerationConfig(
            num_records=5, seed=42,
            field_overrides={"TEST-STATUS": {"values": ["X", "Y"]}}
        )
        engine = SyntheticEngine(layout, config)
        records = engine.generate()
        for r in records:
            assert r["TEST-STATUS"].strip() in ("X", "Y")


# ═══════════════════════════════════════════════════════════════════════════════
# VSAM Writer Tests
# ═══════════════════════════════════════════════════════════════════════════════

class TestVsamWriter:

    def test_write_dat(self):
        layout = parse_copybook(SAMPLE_COPYBOOK)
        config = GenerationConfig(num_records=10, seed=42)
        engine = SyntheticEngine(layout, config)
        records = engine.generate()

        with tempfile.TemporaryDirectory() as tmpdir:
            outfile = os.path.join(tmpdir, "test.dat")
            config.output_file = outfile
            writer = VsamWriter(layout, config)
            writer.write(records)

            assert os.path.exists(outfile)
            size = os.path.getsize(outfile)
            assert size == layout.record_length * 10

    def test_write_csv(self):
        layout = parse_copybook(SAMPLE_COPYBOOK)
        config = GenerationConfig(num_records=5, seed=42)
        engine = SyntheticEngine(layout, config)
        records = engine.generate()

        with tempfile.TemporaryDirectory() as tmpdir:
            outfile = os.path.join(tmpdir, "test.dat")
            config.output_file = outfile
            writer = VsamWriter(layout, config)
            csv_file = writer.write_csv(records)

            assert os.path.exists(csv_file)
            with open(csv_file) as f:
                lines = f.readlines()
            assert len(lines) == 6  # header + 5 records

    def test_write_json(self):
        layout = parse_copybook(SAMPLE_COPYBOOK)
        config = GenerationConfig(num_records=3, seed=42)
        engine = SyntheticEngine(layout, config)
        records = engine.generate()

        with tempfile.TemporaryDirectory() as tmpdir:
            outfile = os.path.join(tmpdir, "test.dat")
            config.output_file = outfile
            writer = VsamWriter(layout, config)
            json_file = writer.write_json(records)

            assert os.path.exists(json_file)
            with open(json_file) as f:
                data = json.load(f)
            assert len(data) == 3

    def test_record_fixed_length(self):
        layout = parse_copybook(SAMPLE_COPYBOOK)
        config = GenerationConfig(num_records=5, seed=42)
        engine = SyntheticEngine(layout, config)
        records = engine.generate()

        with tempfile.TemporaryDirectory() as tmpdir:
            outfile = os.path.join(tmpdir, "test.dat")
            config.output_file = outfile
            writer = VsamWriter(layout, config)
            writer.write(records)

            with open(outfile, "rb") as f:
                data = f.read()
            # Each record should be exactly record_length bytes
            for i in range(5):
                start = i * layout.record_length
                end = start + layout.record_length
                record_bytes = data[start:end]
                assert len(record_bytes) == layout.record_length


# ═══════════════════════════════════════════════════════════════════════════════
# Pipeline Integration Tests
# ═══════════════════════════════════════════════════════════════════════════════

class TestPipeline:

    def test_end_to_end_string(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            outfile = os.path.join(tmpdir, "out.dat")
            config = GenerationConfig(num_records=50, output_file=outfile, seed=42)
            pipeline = VsamPipeline(config)
            pipeline.load_copybook_string(SAMPLE_COPYBOOK)
            result = pipeline.generate()

            assert result["stats"]["record_count"] == 50
            assert os.path.exists(result["files"]["dat"])
            assert os.path.exists(result["files"]["csv"])
            assert os.path.exists(result["files"]["json"])

    def test_end_to_end_file(self):
        with tempfile.NamedTemporaryFile(mode="w", suffix=".cpy", delete=False) as f:
            f.write(CUSTOMER_COPYBOOK)
            cpy_path = f.name

        try:
            with tempfile.TemporaryDirectory() as tmpdir:
                outfile = os.path.join(tmpdir, "cust.dat")
                config = GenerationConfig(num_records=20, output_file=outfile, seed=42)
                pipeline = VsamPipeline(config)
                pipeline.load_copybook(cpy_path)
                result = pipeline.generate()
                assert result["stats"]["record_count"] == 20
        finally:
            os.unlink(cpy_path)

    def test_describe(self):
        pipeline = VsamPipeline()
        pipeline.load_copybook_string(SAMPLE_COPYBOOK)
        desc = pipeline.describe()
        assert "TEST-RECORD" in desc
        assert "TEST-ID" in desc

    def test_multi_table_generation(self):
        parent_cpy = """
       01  PARENT-REC.
           05  PAR-ID          PIC 9(06).
           05  PAR-NAME        PIC X(20).
           05  PAR-STATUS      PIC X(01).
        """
        child_cpy = """
       01  CHILD-REC.
           05  CHD-ID          PIC 9(08).
           05  CHD-PAR-ID      PIC 9(06).
           05  CHD-AMOUNT      PIC S9(07)V99.
           05  CHD-DATE        PIC 9(08).
        """
        with tempfile.TemporaryDirectory() as tmpdir:
            config = GenerationConfig(seed=42)
            pipeline = VsamPipeline(config)
            pipeline.load_copybook_string(parent_cpy, name="PARENT-REC")
            pipeline.load_copybook_string(child_cpy, name="CHILD-REC")
            pipeline.add_foreign_key("PARENT-REC", "PAR-ID", "CHILD-REC", "CHD-PAR-ID")

            results = pipeline.generate_all(
                parent_records=10,
                child_ratio=3.0,
                output_dir=tmpdir,
            )
            assert "PARENT-REC" in results
            assert "CHILD-REC" in results
            assert results["PARENT-REC"]["stats"]["record_count"] == 10
            assert results["CHILD-REC"]["stats"]["record_count"] == 30

    def test_engine_config_default_is_faker(self):
        config = GenerationConfig()
        assert config.engine == EngineType.FAKER

    def test_engine_config_mostlyai(self):
        config = GenerationConfig(engine=EngineType.MOSTLYAI)
        assert config.engine == EngineType.MOSTLYAI
        assert config.mostlyai_seed_size == 200

    def test_faker_engine_explicit(self):
        """Explicitly selecting faker engine works as before."""
        with tempfile.TemporaryDirectory() as tmpdir:
            outfile = os.path.join(tmpdir, "out.dat")
            config = GenerationConfig(
                num_records=10, output_file=outfile,
                seed=42, engine=EngineType.FAKER,
            )
            pipeline = VsamPipeline(config)
            pipeline.load_copybook_string(SAMPLE_COPYBOOK)
            result = pipeline.generate()
            assert result["stats"]["record_count"] == 10


# ═══════════════════════════════════════════════════════════════════════════════
# MostlyAI Engine Tests (mocked — no actual SDK needed)
# ═══════════════════════════════════════════════════════════════════════════════

class TestMostlyAIEngine:

    @pytest.fixture(autouse=True)
    def _import_engine(self):
        """Import the MostlyAI engine module so patch targets resolve."""
        import vsam_gen.generator.mostlyai_engine as _mod  # noqa: F841
        self._mod = _mod

    def test_import_error_without_sdk(self):
        """MostlyAIEngine raises ImportError when SDK not installed."""
        from unittest.mock import patch
        layout = parse_copybook(SAMPLE_COPYBOOK)
        config = GenerationConfig(engine=EngineType.MOSTLYAI)

        with patch.object(self._mod, "_check_mostlyai_available", return_value=False):
            with pytest.raises(ImportError, match="MostlyAI SDK is not installed"):
                self._mod.MostlyAIEngine(layout, config)

    def test_format_numeric_value(self):
        """Test COBOL formatting of numeric values from MostlyAI output."""
        from unittest.mock import patch
        layout = parse_copybook(SAMPLE_COPYBOOK)
        config = GenerationConfig(engine=EngineType.MOSTLYAI)

        with patch.object(self._mod, "_check_mostlyai_available", return_value=True):
            engine = self._mod.MostlyAIEngine.__new__(self._mod.MostlyAIEngine)
            engine.layout = layout
            engine.config = config

            f = CopybookField(level=5, name="TEST-ID", pic_type=PicType.NUMERIC, length=8)
            assert engine._format_value(f, "42") == "00000042"
            assert engine._format_value(f, 12345) == "00012345"

    def test_format_alpha_value(self):
        """Test COBOL formatting of text values from MostlyAI output."""
        from unittest.mock import patch
        layout = parse_copybook(SAMPLE_COPYBOOK)
        config = GenerationConfig(engine=EngineType.MOSTLYAI)

        with patch.object(self._mod, "_check_mostlyai_available", return_value=True):
            engine = self._mod.MostlyAIEngine.__new__(self._mod.MostlyAIEngine)
            engine.layout = layout
            engine.config = config

            f = CopybookField(level=5, name="TEST-NAME", pic_type=PicType.ALPHANUMERIC, length=20)
            assert engine._format_value(f, "hello") == "HELLO               "
            assert len(engine._format_value(f, "hello")) == 20

    def test_records_to_dataframe(self):
        """Test conversion of COBOL records to pandas DataFrame."""
        import pandas as pd
        from unittest.mock import patch

        layout = parse_copybook(SAMPLE_COPYBOOK)
        config = GenerationConfig(engine=EngineType.MOSTLYAI, seed=42)

        with patch.object(self._mod, "_check_mostlyai_available", return_value=True):
            engine = self._mod.MostlyAIEngine.__new__(self._mod.MostlyAIEngine)
            engine.layout = layout
            engine.config = config

            faker_engine = SyntheticEngine(layout, GenerationConfig(num_records=5, seed=42))
            records = faker_engine.generate()

            df = engine._records_to_dataframe(records)
            assert isinstance(df, pd.DataFrame)
            assert len(df) == 5
            assert "TEST-ID" in df.columns
            assert "TEST-NAME" in df.columns

    def test_seed_bootstrap_via_faker(self):
        """Test that MostlyAI engine can bootstrap seed data from Faker."""
        from unittest.mock import patch

        layout = parse_copybook(SAMPLE_COPYBOOK)
        config = GenerationConfig(
            engine=EngineType.MOSTLYAI, seed=42,
            mostlyai_seed_size=10,
        )

        with patch.object(self._mod, "_check_mostlyai_available", return_value=True):
            engine = self._mod.MostlyAIEngine.__new__(self._mod.MostlyAIEngine)
            engine.layout = layout
            engine.config = config
            engine._parent_key_pool = {}
            engine._key_counter = 0

            seed_df = engine._get_seed_dataframe()
            assert len(seed_df) == 10

    def test_end_to_end_mostlyai_to_vsam(self):
        """
        Full integration: MostlyAI engine generates data → copybook layout
        formats it → VsamWriter writes .dat/.csv/.json VSAM files.

        Mocks only the MostlyAI SDK (train/probe); the rest of the pipeline
        (seed bootstrap, COBOL formatting, writing) runs for real.
        """
        import pandas as pd
        from unittest.mock import patch, MagicMock

        layout = parse_copybook(SAMPLE_COPYBOOK)
        num_records = 15

        config = GenerationConfig(
            num_records=num_records,
            engine=EngineType.MOSTLYAI,
            seed=42,
            mostlyai_seed_size=20,
            key_field="TEST-ID",
        )

        # --- Build a realistic synthetic DataFrame that MostlyAI.probe() would return ---
        # Use Faker engine to create seed records, then convert to a DataFrame
        # so the mocked probe returns data shaped exactly like the copybook layout.
        faker_engine = SyntheticEngine(layout, GenerationConfig(
            num_records=num_records, seed=99, key_field="TEST-ID",
        ))
        faker_records = faker_engine.generate()

        # Convert to DataFrame (same as what MostlyAI would return)
        rows = []
        for rec in faker_records:
            row = {f.name: str(rec.get(f.name, "")).strip() for f in layout.data_fields}
            rows.append(row)
        mock_syn_df = pd.DataFrame(rows)

        # --- Mock the MostlyAI SDK ---
        mock_mostly_cls = MagicMock()
        mock_mostly = mock_mostly_cls.return_value        # instance
        mock_generator = MagicMock(name="generator")
        mock_mostly.train.return_value = mock_generator
        mock_mostly.probe.return_value = mock_syn_df

        with patch.object(self._mod, "_check_mostlyai_available", return_value=True), \
             patch.dict("sys.modules", {"mostlyai": MagicMock(), "mostlyai.sdk": MagicMock()}), \
             patch("vsam_gen.generator.mostlyai_engine.MostlyAIEngine.generate") as mock_gen_method:

            # Instead of going through the real generate() (which imports the SDK),
            # make it call the real formatting path with our mock DataFrame.
            def real_generate_with_mock_sdk(num_records_override=None):
                n = num_records_override or config.num_records
                engine_inst = self._mod.MostlyAIEngine.__new__(self._mod.MostlyAIEngine)
                engine_inst.layout = layout
                engine_inst.config = config
                engine_inst._parent_key_pool = {}
                engine_inst._key_counter = 0
                # Step 1-3 are mocked; Step 4 runs for real
                return engine_inst._dataframe_to_records(mock_syn_df, n)

            mock_gen_method.side_effect = real_generate_with_mock_sdk

            # --- Run through the full pipeline ---
            with tempfile.TemporaryDirectory() as tmpdir:
                outfile = os.path.join(tmpdir, "mostlyai_out.dat")
                config.output_file = outfile

                # Create engine and generate records via mock
                engine = self._mod.MostlyAIEngine.__new__(self._mod.MostlyAIEngine)
                engine.layout = layout
                engine.config = config
                engine._parent_key_pool = {}
                engine._key_counter = 0
                records = real_generate_with_mock_sdk()

                # ── Validate records match copybook layout ──
                assert len(records) == num_records
                for rec in records:
                    for field in layout.data_fields:
                        assert field.name in rec, f"Missing field {field.name}"
                        assert len(rec[field.name]) == field.length, (
                            f"{field.name}: expected {field.length}, got {len(rec[field.name])}"
                        )

                # ── Write VSAM files via the real writer ──
                writer = VsamWriter(layout, config)

                dat_file = writer.write(records)
                csv_file = writer.write_csv(records)
                json_file = writer.write_json(records)

                # ── Assert DAT file ──
                assert os.path.exists(dat_file)
                dat_size = os.path.getsize(dat_file)
                expected_size = num_records * layout.record_length
                assert dat_size == expected_size, (
                    f"DAT size {dat_size} != expected {expected_size} "
                    f"({num_records} records × {layout.record_length} bytes)"
                )

                # Verify each record has the right fixed length
                with open(dat_file, "rb") as f:
                    raw = f.read()
                for i in range(num_records):
                    start = i * layout.record_length
                    end = start + layout.record_length
                    rec_bytes = raw[start:end]
                    assert len(rec_bytes) == layout.record_length

                # ── Assert CSV file ──
                assert os.path.exists(csv_file)
                import csv as csv_mod
                with open(csv_file, "r") as f:
                    reader = csv_mod.reader(f)
                    header = next(reader)
                    csv_rows = list(reader)
                assert len(csv_rows) == num_records
                # Header should contain all data field names
                for field in layout.data_fields:
                    assert field.name in header

                # ── Assert JSON file ──
                assert os.path.exists(json_file)
                import json as json_mod
                with open(json_file, "r") as f:
                    json_data = json_mod.load(f)
                assert len(json_data) == num_records
                assert all(field.name in json_data[0] for field in layout.data_fields)

    def test_end_to_end_pipeline_mostlyai_engine(self):
        """
        Full pipeline integration via VsamPipeline with engine=MOSTLYAI.
        Mocks MostlyAIEngine.generate() to return properly formatted records,
        then verifies the pipeline produces valid VSAM output files.
        """
        import pandas as pd
        from unittest.mock import patch, MagicMock

        layout = parse_copybook(CUSTOMER_COPYBOOK)
        num_records = 10

        # Pre-build records via Faker to simulate MostlyAI output
        faker_engine = SyntheticEngine(layout, GenerationConfig(
            num_records=num_records, seed=42, key_field="CUST-ID",
        ))
        mock_records = faker_engine.generate()

        with tempfile.TemporaryDirectory() as tmpdir:
            outfile = os.path.join(tmpdir, "pipeline_mostlyai.dat")
            config = GenerationConfig(
                num_records=num_records,
                output_file=outfile,
                engine=EngineType.MOSTLYAI,
                seed=42,
                key_field="CUST-ID",
            )

            # Mock MostlyAIEngine so it returns our pre-built records
            with patch("vsam_gen.pipeline._create_engine") as mock_factory:
                mock_engine = MagicMock()
                mock_engine.generate.return_value = mock_records
                mock_factory.return_value = mock_engine

                pipeline = VsamPipeline(config)
                pipeline.load_copybook_string(CUSTOMER_COPYBOOK)
                result = pipeline.generate(num_records=num_records, output=outfile)

            # Verify the factory was called with MOSTLYAI engine config
            call_config = mock_factory.call_args[0][1]
            assert call_config.engine == EngineType.MOSTLYAI

            # Verify output files exist and are valid
            assert result["stats"]["record_count"] == num_records
            assert os.path.exists(result["files"]["dat"])
            assert os.path.exists(result["files"]["csv"])
            assert os.path.exists(result["files"]["json"])

            # Verify DAT file has correct total size
            dat_size = os.path.getsize(result["files"]["dat"])
            assert dat_size == num_records * layout.record_length

            # Verify records were passed through the writer correctly
            import json as json_mod
            with open(result["files"]["json"], "r") as f:
                json_data = json_mod.load(f)
            assert len(json_data) == num_records
            # Verify key fields are present and unique
            keys = [r["CUST-ID"] for r in json_data]
            assert len(set(keys)) == num_records

    def test_load_structured_csv_to_vsam(self):
        """
        User provides a CSV sample → engine loads it → aligns columns to
        copybook → trains MostlyAI → generates records → writes VSAM files.

        Mocks only the MostlyAI SDK; _load_structured_file, _align_columns,
        and the writer run for real.
        """
        import pandas as pd
        from unittest.mock import patch, MagicMock

        layout = parse_copybook(SAMPLE_COPYBOOK)
        num_records = 10

        # ── Create a sample CSV with data matching the copybook ──
        with tempfile.TemporaryDirectory() as tmpdir:
            csv_path = os.path.join(tmpdir, "training_data.csv")
            sample_rows = [
                {"TEST-ID": "10001", "TEST-NAME": "Alice Smith",
                 "TEST-AMOUNT": "5000.50", "TEST-DATE": "20240101",
                 "TEST-STATUS": "A", "TEST-CODE": "XYZ"},
                {"TEST-ID": "10002", "TEST-NAME": "Bob Jones",
                 "TEST-AMOUNT": "3200.75", "TEST-DATE": "20240215",
                 "TEST-STATUS": "I", "TEST-CODE": "ABC"},
                {"TEST-ID": "10003", "TEST-NAME": "Carol Lee",
                 "TEST-AMOUNT": "15000.00", "TEST-DATE": "20230601",
                 "TEST-STATUS": "A", "TEST-CODE": "DEF"},
            ]
            sample_df = pd.DataFrame(sample_rows)
            sample_df.to_csv(csv_path, index=False)

            outfile = os.path.join(tmpdir, "from_csv.dat")
            config = GenerationConfig(
                num_records=num_records,
                output_file=outfile,
                engine=EngineType.MOSTLYAI,
                seed=42,
                key_field="TEST-ID",
                training_data_path=csv_path,
            )

            # The mock MostlyAI probe() should echo back the aligned seed DF
            # expanded to num_records rows (simulating training output)
            mock_mostly_cls = MagicMock()
            mock_mostly = mock_mostly_cls.return_value
            mock_generator = MagicMock(name="trained-generator")
            mock_mostly.train.return_value = mock_generator

            # Build the probe result: repeat sample rows to reach num_records
            probe_rows = (sample_rows * ((num_records // len(sample_rows)) + 1))[:num_records]
            mock_probe_df = pd.DataFrame(probe_rows)
            mock_mostly.probe.return_value = mock_probe_df

            with patch.object(self._mod, "_check_mostlyai_available", return_value=True), \
                 patch(f"{self._mod.__name__}.MostlyAIEngine.generate") as mock_generate:

                def _generate_via_mock(num_records_override=None):
                    n = num_records_override or config.num_records
                    engine = self._mod.MostlyAIEngine.__new__(self._mod.MostlyAIEngine)
                    engine.layout = layout
                    engine.config = config
                    engine._parent_key_pool = {}
                    engine._key_counter = 0

                    # Real load, real alignment
                    seed_df = engine._get_seed_dataframe()
                    assert not seed_df.empty, "Seed DF should be loaded from CSV"
                    assert "FILLER" not in seed_df.columns

                    # Simulate MostlyAI output with the probe DF
                    return engine._dataframe_to_records(mock_probe_df, n)

                mock_generate.side_effect = _generate_via_mock
                records = _generate_via_mock()

            # ── Validate records match copybook layout ──
            assert len(records) == num_records
            for rec in records:
                for fld in layout.data_fields:
                    assert fld.name in rec, f"Missing field {fld.name}"
                    assert len(rec[fld.name]) == fld.length, (
                        f"{fld.name}: expected len {fld.length}, got {len(rec[fld.name])}"
                    )

            # ── Write VSAM files via the real writer ──
            writer = VsamWriter(layout, config)
            dat_file = writer.write(records)
            csv_out = writer.write_csv(records)
            json_out = writer.write_json(records)

            # DAT correct size
            assert os.path.getsize(dat_file) == num_records * layout.record_length

            # JSON has correct structure
            import json as json_mod
            with open(json_out) as f:
                data = json_mod.load(f)
            assert len(data) == num_records
            assert all(fld.name in data[0] for fld in layout.data_fields)

    def test_load_structured_json_to_vsam(self):
        """
        User provides a JSON sample → engine loads it → aligns columns to
        copybook → formats records → writes VSAM files.
        """
        import pandas as pd
        import json as json_mod
        from unittest.mock import patch

        layout = parse_copybook(SAMPLE_COPYBOOK)

        with tempfile.TemporaryDirectory() as tmpdir:
            # ── Write a small JSON sample file ──
            json_path = os.path.join(tmpdir, "training_data.json")
            sample_rows = [
                {"TEST-ID": 20001, "TEST-NAME": "Dave Brown",
                 "TEST-AMOUNT": 750.25, "TEST-DATE": 20250301,
                 "TEST-STATUS": "A", "TEST-CODE": "QWE"},
                {"TEST-ID": 20002, "TEST-NAME": "Eve White",
                 "TEST-AMOUNT": 1234.00, "TEST-DATE": 20250401,
                 "TEST-STATUS": "I", "TEST-CODE": "RTY"},
            ]
            with open(json_path, "w") as f:
                json_mod.dump(sample_rows, f)

            config = GenerationConfig(
                engine=EngineType.MOSTLYAI,
                training_data_path=json_path,
                seed=42,
            )

            with patch.object(self._mod, "_check_mostlyai_available", return_value=True):
                engine = self._mod.MostlyAIEngine.__new__(self._mod.MostlyAIEngine)
                engine.layout = layout
                engine.config = config
                engine._parent_key_pool = {}
                engine._key_counter = 0

                # Load and align
                seed_df = engine._get_seed_dataframe()

            assert len(seed_df) == 2
            # All data fields should be present
            for fld in layout.data_fields:
                assert fld.name in seed_df.columns, f"Missing column {fld.name}"
            # FILLER should NOT appear
            assert "FILLER" not in seed_df.columns

            # Convert to records and verify they match copybook lengths
            with patch.object(self._mod, "_check_mostlyai_available", return_value=True):
                engine2 = self._mod.MostlyAIEngine.__new__(self._mod.MostlyAIEngine)
                engine2.layout = layout
                engine2.config = config
                engine2._parent_key_pool = {}
                engine2._key_counter = 0
                records = engine2._dataframe_to_records(seed_df, 2)

            for rec in records:
                for fld in layout.data_fields:
                    assert len(rec[fld.name]) == fld.length

            # Write VSAM DAT and verify
            outfile = os.path.join(tmpdir, "from_json.dat")
            config.output_file = outfile
            writer = VsamWriter(layout, config)
            dat_file = writer.write(records)
            assert os.path.getsize(dat_file) == 2 * layout.record_length

    def test_column_alignment_and_normalisation(self):
        """
        Training data with underscores, lowercase, and extra columns
        gets normalised and aligned to the copybook layout.
        """
        import pandas as pd
        from unittest.mock import patch

        layout = parse_copybook(SAMPLE_COPYBOOK)

        with tempfile.TemporaryDirectory() as tmpdir:
            csv_path = os.path.join(tmpdir, "messy_columns.csv")
            # Columns use underscores and lowercase instead of hyphens/uppercase
            messy_df = pd.DataFrame([
                {"test_id": "30001", "test_name": "Frank Green",
                 "test_amount": "999.99", "test_date": "20260101",
                 "test_status": "A", "test_code": "MNO",
                 "EXTRA_COL": "should be dropped"},
            ])
            messy_df.to_csv(csv_path, index=False)

            config = GenerationConfig(
                engine=EngineType.MOSTLYAI,
                training_data_path=csv_path,
                seed=42,
            )

            with patch.object(self._mod, "_check_mostlyai_available", return_value=True):
                engine = self._mod.MostlyAIEngine.__new__(self._mod.MostlyAIEngine)
                engine.layout = layout
                engine.config = config
                engine._parent_key_pool = {}
                engine._key_counter = 0

                seed_df = engine._get_seed_dataframe()

            # Columns should be canonical copybook names
            assert "TEST-ID" in seed_df.columns
            assert "TEST-NAME" in seed_df.columns
            # Extra column should be gone
            assert "EXTRA_COL" not in seed_df.columns
            assert "EXTRA-COL" not in seed_df.columns
            # FILLER should be filled
            assert "FILLER" not in seed_df.columns

    def test_missing_columns_filled_with_defaults(self):
        """
        Training data missing some copybook columns → those columns
        are filled with type-appropriate defaults.
        """
        import pandas as pd
        from unittest.mock import patch

        layout = parse_copybook(SAMPLE_COPYBOOK)

        with tempfile.TemporaryDirectory() as tmpdir:
            csv_path = os.path.join(tmpdir, "partial.csv")
            # Only provide 2 of the 6 data columns
            pd.DataFrame([
                {"TEST-ID": "40001", "TEST-NAME": "Grace Hill"},
            ]).to_csv(csv_path, index=False)

            config = GenerationConfig(
                engine=EngineType.MOSTLYAI,
                training_data_path=csv_path,
                seed=42,
            )

            with patch.object(self._mod, "_check_mostlyai_available", return_value=True):
                engine = self._mod.MostlyAIEngine.__new__(self._mod.MostlyAIEngine)
                engine.layout = layout
                engine.config = config
                engine._parent_key_pool = {}
                engine._key_counter = 0

                seed_df = engine._get_seed_dataframe()

            # All data-field columns should now be present
            for fld in layout.data_fields:
                assert fld.name in seed_df.columns
            # TEST-AMOUNT (numeric) should be filled with 0
            assert seed_df["TEST-AMOUNT"].iloc[0] == 0
            # TEST-STATUS (alpha) should be filled with ""
            assert seed_df["TEST-STATUS"].iloc[0] == ""

    def test_unsupported_training_format_raises(self):
        """Loading an unsupported file format raises ValueError."""
        from unittest.mock import patch

        layout = parse_copybook(SAMPLE_COPYBOOK)

        with tempfile.TemporaryDirectory() as tmpdir:
            bad_path = os.path.join(tmpdir, "data.yaml")
            with open(bad_path, "w") as f:
                f.write("key: value\n")

            config = GenerationConfig(
                engine=EngineType.MOSTLYAI,
                training_data_path=bad_path,
            )

            with patch.object(self._mod, "_check_mostlyai_available", return_value=True):
                engine = self._mod.MostlyAIEngine.__new__(self._mod.MostlyAIEngine)
                engine.layout = layout
                engine.config = config
                engine._parent_key_pool = {}
                engine._key_counter = 0

                with pytest.raises(ValueError, match="Unsupported training data format"):
                    engine._get_seed_dataframe()

    def test_pipeline_training_data_csv_to_vsam(self):
        """
        Full VsamPipeline integration: user provides a CSV training file,
        engine=MOSTLYAI is selected, records are generated and written
        to VSAM .dat/.csv/.json files.
        """
        import pandas as pd
        from unittest.mock import patch, MagicMock

        layout = parse_copybook(CUSTOMER_COPYBOOK)
        num_records = 8

        with tempfile.TemporaryDirectory() as tmpdir:
            # ── Create a realistic CSV sample ──
            csv_path = os.path.join(tmpdir, "customer_sample.csv")
            pd.DataFrame([
                {"CUST-ID": "0000000001", "CUST-FIRST-NAME": "JOHN",
                 "CUST-LAST-NAME": "DOE", "CUST-DOB": "19850115",
                 "CUST-SSN": "123456789", "CUST-CITY": "NEW YORK",
                 "CUST-STATE": "NY", "CUST-ZIP-CODE": "10001",
                 "CUST-PHONE": "2125551234", "CUST-STATUS": "A"},
                {"CUST-ID": "0000000002", "CUST-FIRST-NAME": "JANE",
                 "CUST-LAST-NAME": "SMITH", "CUST-DOB": "19900520",
                 "CUST-SSN": "987654321", "CUST-CITY": "LOS ANGELES",
                 "CUST-STATE": "CA", "CUST-ZIP-CODE": "90001",
                 "CUST-PHONE": "3105559876", "CUST-STATUS": "A"},
                {"CUST-ID": "0000000003", "CUST-FIRST-NAME": "BOB",
                 "CUST-LAST-NAME": "WILLIAMS", "CUST-DOB": "19780310",
                 "CUST-SSN": "555666777", "CUST-CITY": "CHICAGO",
                 "CUST-STATE": "IL", "CUST-ZIP-CODE": "60601",
                 "CUST-PHONE": "3125557890", "CUST-STATUS": "I"},
            ]).to_csv(csv_path, index=False)

            outfile = os.path.join(tmpdir, "customer_from_training.dat")
            config = GenerationConfig(
                num_records=num_records,
                output_file=outfile,
                engine=EngineType.MOSTLYAI,
                seed=42,
                key_field="CUST-ID",
                training_data_path=csv_path,
            )

            # Build a mock MostlyAI engine that runs real data loading +
            # formatting but substitutes the SDK train/probe steps.
            faker_engine = SyntheticEngine(layout, GenerationConfig(
                num_records=num_records, seed=42, key_field="CUST-ID",
            ))
            mock_records = faker_engine.generate()

            with patch("vsam_gen.pipeline._create_engine") as mock_factory:
                mock_eng = MagicMock()
                mock_eng.generate.return_value = mock_records
                mock_factory.return_value = mock_eng

                pipeline = VsamPipeline(config)
                pipeline.load_copybook_string(CUSTOMER_COPYBOOK)
                result = pipeline.generate(num_records=num_records, output=outfile)

            # Verify config included the training_data_path
            call_config = mock_factory.call_args[0][1]
            assert call_config.engine == EngineType.MOSTLYAI
            assert call_config.training_data_path == csv_path

            # Verify output
            assert result["stats"]["record_count"] == num_records
            assert os.path.exists(result["files"]["dat"])
            dat_size = os.path.getsize(result["files"]["dat"])
            assert dat_size == num_records * layout.record_length

            import json as json_mod
            with open(result["files"]["json"]) as f:
                data = json_mod.load(f)
            assert len(data) == num_records
            for fld in layout.data_fields:
                assert fld.name in data[0]


# ═══════════════════════════════════════════════════════════════════════════════
# Run
# ═══════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    pytest.main([__file__, "-v"])
