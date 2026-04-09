"""
MostlyAI Synthetic Data Engine
================================
Uses the MostlyAI SDK (LOCAL mode) to train a generative AI model on
seed data derived from the copybook layout, then generates statistically
consistent synthetic records at any scale.

Workflow:
  1. Bootstrap a small seed DataFrame using the Faker-based SyntheticEngine
     (or load user-provided training CSV).
  2. Train a MostlyAI tabular generator on that seed data.
  3. Use the trained generator to probe/generate the requested number of records.
  4. Format the MostlyAI DataFrame output back into COBOL-compatible fixed-length strings.

Requires: pip install 'mostlyai[local]'
"""

import logging
import os
import random
from typing import Any, Optional

from vsam_gen.models import CopybookField, CopybookLayout, GenerationConfig, PicType
from vsam_gen.generator.ai_inferrer import infer_all_fields

logger = logging.getLogger("vsam_gen")


def _check_mostlyai_available():
    """Check if the mostlyai package is installed."""
    try:
        from mostlyai.sdk import MostlyAI  # noqa: F401
        return True
    except ImportError:
        return False


class MostlyAIEngine:
    """
    Generates synthetic records using MostlyAI's tabular AI model.

    The engine first creates a seed DataFrame (via Faker or user CSV),
    trains a MostlyAI generator on it, then samples new records that
    preserve the statistical distributions of the seed data.

    Usage:
        engine = MostlyAIEngine(layout, config)
        records = engine.generate(1000)
    """

    def __init__(self, layout: CopybookLayout, config: Optional[GenerationConfig] = None):
        if not _check_mostlyai_available():
            raise ImportError(
                "MostlyAI SDK is not installed. Install it with:\n"
                "  pip install 'mostlyai[local]'\n"
                "See: https://github.com/mostly-ai/mostlyai"
            )

        self.layout = layout
        self.config = config or GenerationConfig()
        self._generator = None

        if self.config.seed is not None:
            random.seed(self.config.seed)

        # Run AI inference on fields
        if self.config.ai_infer_types:
            infer_all_fields(self.layout.fields)

        # Foreign key parent values (populated during multi-table gen)
        self._parent_key_pool: dict[str, list] = {}

        # Key tracking for KSDS uniqueness
        self._key_counter: int = 0

    def set_parent_key_pool(self, field_name: str, values: list):
        """Set available parent key values for foreign key relationships."""
        self._parent_key_pool[field_name.upper()] = values

    def generate(self, num_records: Optional[int] = None) -> list[dict[str, Any]]:
        """
        Generate synthetic records using MostlyAI.

        Steps:
          1. Build or load seed DataFrame
          2. Train MostlyAI generator (or load cached)
          3. Generate num_records via probe/generate
          4. Format back to COBOL-compatible strings

        Returns:
            List of dicts with field names as keys.
        """
        from mostlyai.sdk import MostlyAI

        n = num_records or self.config.num_records
        mostly = MostlyAI()

        # ── Step 1: Get seed data ────────────────────────────────────────
        seed_df = self._get_seed_dataframe()
        logger.info(f"  MostlyAI: seed data has {len(seed_df)} rows, "
                     f"{len(seed_df.columns)} columns")

        # ── Step 2: Train or load generator ──────────────────────────────
        generator = self._get_or_train_generator(mostly, seed_df)

        # ── Step 3: Generate synthetic data ──────────────────────────────
        logger.info(f"  MostlyAI: generating {n} synthetic records...")
        if n <= 10000:
            # Use probe for smaller batches (faster, no persistent dataset)
            syn_df = mostly.probe(generator, size=n)
        else:
            # Use generate for larger batches
            sd = mostly.generate(generator, size=n)
            syn_df = sd.data()

        logger.info(f"  MostlyAI: received {len(syn_df)} records from model")

        # ── Step 4: Format to COBOL-compatible records ───────────────────
        records = self._dataframe_to_records(syn_df, n)

        return records

    def _get_seed_dataframe(self):
        """
        Build the seed DataFrame for MostlyAI training.

        Priority:
          1. User-provided structured file (CSV, JSON, Excel).
          2. Bootstrap from Faker engine.

        When loading user data the columns are validated against the copybook
        layout.  Columns not present in the layout are dropped and missing
        columns are added with sensible filler values so that the downstream
        training always sees the same schema.
        """
        import pandas as pd

        # ── Option 1: User-provided structured data ──────────────────────
        path = self.config.training_data_path
        if path and os.path.exists(path):
            logger.info(f"  MostlyAI: loading training data from {path}")
            df = self._load_structured_file(path)
            df = self._align_columns(df)
            return df

        # Option 2: Bootstrap from Faker
        logger.info(f"  MostlyAI: bootstrapping {self.config.mostlyai_seed_size} "
                     f"seed records via Faker...")
        from vsam_gen.generator.synthetic_engine import SyntheticEngine

        # Create a Faker-based config for seed generation
        seed_config = GenerationConfig(
            num_records=self.config.mostlyai_seed_size,
            seed=self.config.seed,
            locale=self.config.locale,
            ai_infer_types=self.config.ai_infer_types,
            field_overrides=self.config.field_overrides,
            key_field=self.config.key_field,
        )
        faker_engine = SyntheticEngine(self.layout, seed_config)
        seed_records = faker_engine.generate()

        # Convert to DataFrame with proper column types
        return self._records_to_dataframe(seed_records)

    # ── Structured file loading helpers ──────────────────────────────────

    SUPPORTED_EXTENSIONS = {
        ".csv": "csv",
        ".tsv": "csv",         # tab-separated
        ".json": "json",
        ".jsonl": "jsonl",     # newline-delimited JSON
        ".xlsx": "excel",
        ".xls": "excel",
        ".parquet": "parquet",
    }

    def _load_structured_file(self, path: str):
        """
        Load a structured data file into a pandas DataFrame.

        Supported formats: CSV, TSV, JSON, JSON Lines, Excel, Parquet.
        The format is detected from the file extension.
        """
        import pandas as pd

        ext = os.path.splitext(path)[1].lower()
        fmt = self.SUPPORTED_EXTENSIONS.get(ext)

        if fmt is None:
            supported = ", ".join(sorted(self.SUPPORTED_EXTENSIONS.keys()))
            raise ValueError(
                f"Unsupported training data format '{ext}'. "
                f"Supported: {supported}"
            )

        logger.info(f"  MostlyAI: detected format '{fmt}' for {os.path.basename(path)}")

        if fmt == "csv":
            sep = "\t" if ext == ".tsv" else ","
            return pd.read_csv(path, sep=sep)
        elif fmt == "json":
            return pd.read_json(path)
        elif fmt == "jsonl":
            return pd.read_json(path, lines=True)
        elif fmt == "excel":
            return pd.read_excel(path)
        elif fmt == "parquet":
            return pd.read_parquet(path)

    def _align_columns(self, df):
        """
        Align a user-provided DataFrame to the copybook layout.

        - Normalises column names (strip, uppercase, replace spaces/underscores
          with hyphens) so that common naming mismatches are tolerated.
        - Drops columns not in the layout.
        - Fills missing layout columns with type-appropriate defaults.
        - Logs a warning for every missing column so the user knows.
        """
        import pandas as pd

        # Build a mapping: normalised_name → original column name
        def _normalise(name: str) -> str:
            return name.strip().upper().replace("_", "-").replace(" ", "-")

        col_map = {_normalise(c): c for c in df.columns}
        expected = {f.name for f in self.layout.data_fields}

        # Rename columns to their canonical copybook names where possible
        rename = {}
        for field in self.layout.data_fields:
            norm = _normalise(field.name)
            if norm in col_map and col_map[norm] != field.name:
                rename[col_map[norm]] = field.name
        if rename:
            df = df.rename(columns=rename)
            logger.info(f"  MostlyAI: renamed columns {rename}")

        # Drop extra columns not in layout
        extra = set(df.columns) - expected
        if extra:
            logger.info(f"  MostlyAI: dropping extra columns not in copybook: {sorted(extra)}")
            df = df.drop(columns=list(extra))

        # Fill missing columns with defaults
        missing = expected - set(df.columns)
        if missing:
            logger.warning(
                f"  MostlyAI: training data is missing copybook columns "
                f"(will be filled with defaults): {sorted(missing)}"
            )
            field_map = {f.name: f for f in self.layout.data_fields}
            for col in missing:
                f = field_map[col]
                if f.pic_type in (PicType.NUMERIC, PicType.SIGNED_NUMERIC,
                                  PicType.PACKED_DECIMAL, PicType.BINARY):
                    df[col] = 0
                else:
                    df[col] = ""

        # Reorder to match layout field order
        ordered = [f.name for f in self.layout.data_fields if f.name in df.columns]
        df = df[ordered]

        return df

    def _records_to_dataframe(self, records: list[dict[str, Any]]):
        """Convert COBOL record dicts to a pandas DataFrame with clean types."""
        import pandas as pd

        rows = []
        for record in records:
            row = {}
            for field in self.layout.data_fields:
                val = record.get(field.name, "")
                if isinstance(val, list):
                    # Flatten OCCURS arrays for training
                    row[field.name] = "|".join(str(v).strip() for v in val)
                else:
                    row[field.name] = str(val).strip()
            rows.append(row)

        df = pd.DataFrame(rows)

        # Set appropriate dtypes for numeric columns
        for field in self.layout.data_fields:
            if field.name in df.columns:
                if field.pic_type in (PicType.NUMERIC, PicType.SIGNED_NUMERIC,
                                      PicType.PACKED_DECIMAL, PicType.BINARY):
                    try:
                        if field.decimal_places > 0:
                            df[field.name] = pd.to_numeric(df[field.name], errors="coerce").fillna(0)
                        else:
                            df[field.name] = pd.to_numeric(df[field.name], errors="coerce").fillna(0).astype(int)
                    except (ValueError, TypeError):
                        pass  # keep as string if conversion fails

        return df

    def _get_or_train_generator(self, mostly, seed_df):
        """Train a new generator or load a cached one."""
        # Check for cached generator
        gen_path = self.config.mostlyai_generator_path
        if gen_path and os.path.exists(gen_path):
            logger.info(f"  MostlyAI: loading cached generator from {gen_path}")
            return mostly.generators.import_from_file(gen_path)

        # Train new generator
        logger.info(f"  MostlyAI: training tabular generator...")
        train_config = {
            "name": f"vsam-gen-{self.layout.name}",
            "tables": [
                {
                    "name": self.layout.name,
                    "data": seed_df,
                }
            ],
        }

        # Add max training time if configured
        if self.config.mostlyai_max_training_time:
            train_config["tables"][0]["tabular_model_configuration"] = {
                "max_training_time": self.config.mostlyai_max_training_time,
            }

        generator = mostly.train(config=train_config)
        logger.info(f"  MostlyAI: training complete")

        # Cache if path configured
        if gen_path:
            os.makedirs(os.path.dirname(gen_path) if os.path.dirname(gen_path) else ".", exist_ok=True)
            generator.export_to_file(gen_path)
            logger.info(f"  MostlyAI: generator saved to {gen_path}")

        return generator

    def _dataframe_to_records(self, df, target_count: int) -> list[dict[str, Any]]:
        """
        Convert a MostlyAI-generated DataFrame back into COBOL-formatted record dicts.
        Ensures field lengths match the copybook layout.
        """
        records = []
        data_fields = self.layout.data_fields

        for idx in range(min(len(df), target_count)):
            row = df.iloc[idx]
            record = {}

            for field in data_fields:
                col_name = field.name

                # Get value from DataFrame (column might be missing)
                if col_name in df.columns:
                    raw_val = row[col_name]
                else:
                    raw_val = self._fallback_value(field)

                # Handle the key field (unique sequential)
                if self.config.key_field and col_name.upper() == self.config.key_field.upper():
                    self._key_counter += 1
                    record[col_name] = self._format_value(field, str(self._key_counter))
                    continue

                # Handle foreign key fields
                if col_name.upper() in self._parent_key_pool:
                    pool = self._parent_key_pool[col_name.upper()]
                    raw_val = random.choice(pool) if pool else raw_val

                # Format to COBOL-compatible string
                record[col_name] = self._format_value(field, raw_val)

            records.append(record)

        return records

    def _format_value(self, field: CopybookField, value: Any) -> str:
        """Format a value to match COBOL field specs (length, padding)."""
        length = field.length
        str_val = str(value) if value is not None and str(value) != "nan" else ""

        if field.pic_type in (PicType.NUMERIC, PicType.SIGNED_NUMERIC,
                              PicType.PACKED_DECIMAL, PicType.BINARY):
            # Numeric: extract digits, zero-pad, right-justify
            clean = "".join(c for c in str_val if c.isdigit() or c == ".")
            if "." in clean and field.decimal_places > 0:
                # Convert decimal to implied-decimal integer form
                try:
                    num = float(clean)
                    int_val = int(round(num * (10 ** field.decimal_places)))
                    clean = str(abs(int_val))
                except (ValueError, OverflowError):
                    clean = "0"
            else:
                clean = "".join(c for c in clean if c.isdigit()) or "0"
            return clean.zfill(length)[:length]
        else:
            # Alphanumeric: left-justify, space-pad, uppercase
            return str_val.upper().ljust(length)[:length]

    def _fallback_value(self, field: CopybookField) -> str:
        """Generate a fallback value when MostlyAI doesn't produce the column."""
        if field.pic_type in (PicType.ALPHANUMERIC, PicType.ALPHABETIC):
            return " " * field.length
        return "0" * field.length

    def get_generated_key_values(self) -> list[str]:
        """Return all generated key values (for foreign key linking)."""
        return [str(i + 1).zfill(10) for i in range(self.config.num_records)]
