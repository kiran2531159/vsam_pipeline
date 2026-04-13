"""
VSAM Pipeline Orchestrator
============================
Main pipeline class that ties together parsing, generation, and writing.
Supports single and multi-table generation with referential integrity.
"""

import logging
import os
from typing import Any, Optional

from vsam_gen.models import CopybookLayout, GenerationConfig, VsamOrgType, EngineType
from vsam_gen.parser.copybook_parser import parse_copybook, parse_copybook_file
from vsam_gen.generator.synthetic_engine import SyntheticEngine
from vsam_gen.writer.vsam_writer import VsamWriter

logger = logging.getLogger("vsam_gen")


def _create_engine(layout: CopybookLayout, config: GenerationConfig):
    """Factory: create the appropriate generation engine based on config."""
    if config.engine == EngineType.MOSTLYAI:
        from vsam_gen.generator.mostlyai_engine import MostlyAIEngine
        return MostlyAIEngine(layout, config)
    return SyntheticEngine(layout, config)


class VsamPipeline:
    """
    End-to-end pipeline: Copybook → Parse → AI Inference → Generate → Write.

    Usage (simple):
        pipeline = VsamPipeline()
        pipeline.load_copybook("copybooks/customer.cpy")
        result = pipeline.generate(num_records=1000, output="output/customer.dat")

    Usage (multi-table):
        pipeline = VsamPipeline()
        pipeline.load_copybook("copybooks/customer.cpy", name="CUSTOMER")
        pipeline.load_copybook("copybooks/account.cpy",  name="ACCOUNT")
        pipeline.add_foreign_key("CUSTOMER", "CUST-ID", "ACCOUNT", "ACCT-CUST-ID")
        results = pipeline.generate_all(parent_records=500)

    Usage (from string):
        pipeline = VsamPipeline()
        pipeline.load_copybook_string(copybook_text, name="MY-RECORD")
        result = pipeline.generate(num_records=100)
    """

    def __init__(self, config: Optional[GenerationConfig] = None):
        self.config = config or GenerationConfig()
        self.layouts: dict[str, CopybookLayout] = {}
        self._foreign_keys: list[dict] = []
        self._generated_data: dict[str, list[dict]] = {}
        self._copybook_configs: dict[str, dict] = {}  # per-copybook YAML settings

        # Setup logging
        if not logger.handlers:
            handler = logging.StreamHandler()
            handler.setFormatter(logging.Formatter(
                "[%(levelname)s] %(message)s"
            ))
            logger.addHandler(handler)
            logger.setLevel(logging.INFO)

    def load_copybook(self, filepath: str, name: Optional[str] = None) -> CopybookLayout:
        """Load and parse a copybook from a file."""
        logger.info(f"Parsing copybook: {filepath}")
        layout = parse_copybook_file(filepath, name)
        self.layouts[layout.name] = layout
        logger.info(f"  Layout '{layout.name}': {len(layout.data_fields)} data fields, "
                     f"{layout.record_length} bytes/record")
        return layout

    def load_copybook_string(self, source: str, name: Optional[str] = None) -> CopybookLayout:
        """Load and parse a copybook from a string."""
        logger.info("Parsing copybook from string...")
        layout = parse_copybook(source, name)
        self.layouts[layout.name] = layout
        logger.info(f"  Layout '{layout.name}': {len(layout.data_fields)} data fields, "
                     f"{layout.record_length} bytes/record")
        return layout

    def add_foreign_key(self, parent_layout: str, parent_field: str,
                        child_layout: str, child_field: str):
        """
        Define a foreign key relationship between two layouts.
        When generating, child records will reference valid parent key values.
        """
        self._foreign_keys.append({
            "parent_layout": parent_layout,
            "parent_field": parent_field,
            "child_layout": child_layout,
            "child_field": child_field,
        })

    def generate(self, num_records: Optional[int] = None,
                 output: Optional[str] = None,
                 layout_name: Optional[str] = None,
                 formats: Optional[list[str]] = None) -> dict[str, Any]:
        """
        Generate synthetic VSAM data for a single layout.

        Args:
            num_records: Number of records to generate.
            output:      Output file path (e.g., "output/data.dat").
            layout_name: Which layout to use (default: first loaded).
            formats:     Output formats: ["dat", "csv", "json"] (default: all three).

        Returns:
            Dict with keys: records, files, layout, stats.
        """
        if not self.layouts:
            raise ValueError("No copybook loaded. Call load_copybook() first.")

        # Pick layout
        if layout_name:
            layout = self.layouts[layout_name]
        else:
            layout = next(iter(self.layouts.values()))

        # Override config
        config = GenerationConfig(
            num_records=num_records or self.config.num_records,
            output_file=output or self.config.output_file,
            encoding=self.config.encoding,
            vsam_type=self.config.vsam_type,
            key_field=self.config.key_field,
            seed=self.config.seed,
            locale=self.config.locale,
            engine=self.config.engine,
            ai_infer_types=self.config.ai_infer_types,
            field_overrides=self.config.field_overrides,
            training_data_path=self.config.training_data_path,
            mostlyai_seed_size=self.config.mostlyai_seed_size,
            mostlyai_max_training_time=self.config.mostlyai_max_training_time,
            mostlyai_generator_path=self.config.mostlyai_generator_path,
        )

        # Auto-detect key field for KSDS
        if config.vsam_type == VsamOrgType.KSDS and not config.key_field:
            for f in layout.data_fields:
                if any(kw in f.name.upper() for kw in ("KEY", "ID", "NUM", "NO")):
                    config.key_field = f.name
                    logger.info(f"  Auto-detected KSDS key: {f.name}")
                    break

        # Generate
        logger.info(f"Generating {config.num_records} records for '{layout.name}' "
                     f"[engine={config.engine.value}]...")
        engine = _create_engine(layout, config)
        records = engine.generate()

        # Store for multi-table linking
        self._generated_data[layout.name] = records

        # Write
        output_formats = formats or ["dat", "csv", "json"]
        files = {}
        writer = VsamWriter(layout, config)

        if "dat" in output_formats:
            dat_file = writer.write(records)
            files["dat"] = dat_file
            logger.info(f"  Written DAT: {dat_file} ({os.path.getsize(dat_file):,} bytes)")

        if "csv" in output_formats:
            csv_file = writer.write_csv(records)
            files["csv"] = csv_file
            logger.info(f"  Written CSV: {csv_file}")

        if "json" in output_formats:
            json_file = writer.write_json(records)
            files["json"] = json_file
            logger.info(f"  Written JSON: {json_file}")

        result = {
            "records": records,
            "files": files,
            "layout": layout,
            "stats": {
                "record_count": len(records),
                "record_length": layout.record_length,
                "total_bytes": layout.record_length * len(records),
                "field_count": len(layout.data_fields),
            },
        }

        logger.info(f"  Done: {len(records)} records × {layout.record_length} bytes = "
                     f"{layout.record_length * len(records):,} bytes total")
        return result

    def generate_all(self, parent_records: int = 500,
                     child_ratio: float = 3.0,
                     output_dir: str = "output",
                     formats: Optional[list[str]] = None) -> dict[str, Any]:
        """
        Generate data for all loaded layouts with foreign key relationships.

        Args:
            parent_records: Number of records for parent tables.
            child_ratio:    Average child records per parent record.
            output_dir:     Output directory.
            formats:        Output formats.

        Returns:
            Dict mapping layout_name -> generation result.
        """
        results = {}

        # Identify parent/child from foreign keys
        child_layouts = {fk["child_layout"] for fk in self._foreign_keys}
        parent_layouts = [name for name in self.layouts if name not in child_layouts]

        # If no FK defined, generate all independently
        if not self._foreign_keys:
            parent_layouts = list(self.layouts.keys())

        # Generate parents first
        for name in parent_layouts:
            output = os.path.join(output_dir, f"{name.lower()}.dat")
            result = self.generate(
                num_records=parent_records,
                output=output,
                layout_name=name,
                formats=formats,
            )
            results[name] = result

        # Generate children with FK linking
        for fk in self._foreign_keys:
            child_name = fk["child_layout"]
            parent_name = fk["parent_layout"]
            parent_field = fk["parent_field"]
            child_field = fk["child_field"]

            if child_name not in self.layouts:
                logger.warning(f"Child layout '{child_name}' not loaded, skipping.")
                continue

            # Extract parent key values
            parent_records_data = self._generated_data.get(parent_name, [])
            parent_keys = [
                r.get(parent_field, "")
                for r in parent_records_data
                if parent_field in r
            ]

            if not parent_keys:
                logger.warning(f"No parent key values found for {parent_name}.{parent_field}")
                parent_keys = [str(i + 1).zfill(10) for i in range(parent_records)]

            num_child = int(len(parent_keys) * child_ratio)
            output = os.path.join(output_dir, f"{child_name.lower()}.dat")

            # Temporarily update config for FK
            old_overrides = self.config.field_overrides.copy()
            self.config.field_overrides = old_overrides.copy()

            # Generate child records
            layout = self.layouts[child_name]
            config = GenerationConfig(
                num_records=num_child,
                output_file=output,
                encoding=self.config.encoding,
                vsam_type=self.config.vsam_type,
                seed=self.config.seed,
                locale=self.config.locale,
                engine=self.config.engine,
                ai_infer_types=self.config.ai_infer_types,
                field_overrides=self.config.field_overrides,
                training_data_path=self.config.training_data_path,
                mostlyai_seed_size=self.config.mostlyai_seed_size,
                mostlyai_max_training_time=self.config.mostlyai_max_training_time,
                mostlyai_generator_path=self.config.mostlyai_generator_path,
            )

            engine = _create_engine(layout, config)
            engine.set_parent_key_pool(child_field, parent_keys)

            logger.info(f"Generating {num_child} child records for '{child_name}' "
                        f"(linked to {parent_name}.{parent_field}) "
                        f"[engine={config.engine.value}]...")
            records = engine.generate()
            self._generated_data[child_name] = records

            writer = VsamWriter(layout, config)
            output_formats = formats or ["dat", "csv", "json"]
            files = {}

            if "dat" in output_formats:
                files["dat"] = writer.write(records)
            if "csv" in output_formats:
                files["csv"] = writer.write_csv(records)
            if "json" in output_formats:
                files["json"] = writer.write_json(records)

            results[child_name] = {
                "records": records,
                "files": files,
                "layout": layout,
                "stats": {
                    "record_count": len(records),
                    "record_length": layout.record_length,
                    "total_bytes": layout.record_length * len(records),
                    "field_count": len(layout.data_fields),
                },
            }

            self.config.field_overrides = old_overrides

        return results

    def set_copybook_configs(self, copybook_configs: dict[str, dict]):
        """
        Store per-copybook YAML configs for use during generation.
        Keys are layout names, values are the raw YAML dict for each copybook.
        """
        self._copybook_configs = copybook_configs

    def generate_all_mostlyai(
        self,
        copybook_configs: dict[str, dict],
        output_dir: str = "output",
        formats: Optional[list[str]] = None,
        parent_records: int = 500,
        child_ratio: float = 3.0,
    ) -> dict[str, Any]:
        """
        Multi-table MostlyAI generation.

        Trains a single MostlyAI generator on all linked tables
        (using their CSV training data), then generates synthetic data
        for all tables at once — preserving cross-table correlations
        and FK referential integrity through MostlyAI's context model.

        Args:
            copybook_configs: Dict of layout_name → YAML copybook config.
            output_dir:       Output directory.
            formats:          Output formats.
            parent_records:   Number of records for subject (root) tables.
            child_ratio:      Average child records per parent.

        Returns:
            Dict mapping layout_name -> generation result.
        """
        import pandas as pd
        from vsam_gen.generator.mostlyai_engine import MostlyAIEngine, _check_mostlyai_available

        if not _check_mostlyai_available():
            raise ImportError(
                "MostlyAI SDK is not installed. Install with:\n"
                "  uv pip install 'mostlyai[local]'"
            )
        from mostlyai.sdk import MostlyAI

        os.makedirs(output_dir, exist_ok=True)
        output_formats = formats or ["dat", "csv", "json"]

        # ── Identify subject (root) vs child tables ──────────────────────
        child_tables = set()
        for name, cb_cfg in copybook_configs.items():
            if cb_cfg.get("foreign_keys"):
                child_tables.add(name)
        subject_tables = [n for n in self.layouts if n not in child_tables]

        # ── Build MostlyAI training config with all tables ───────────────
        tables_config = []
        for name in self.layouts:
            layout = self.layouts[name]
            cb_cfg = copybook_configs.get(name, {})

            # Load training data for this table
            training_path = cb_cfg.get("training_data")
            engine = MostlyAIEngine.__new__(MostlyAIEngine)
            engine.layout = layout
            engine.config = GenerationConfig(
                training_data_path=training_path,
                mostlyai_seed_size=self.config.mostlyai_seed_size,
                seed=self.config.seed,
                locale=self.config.locale,
                ai_infer_types=self.config.ai_infer_types,
                key_field=cb_cfg.get("key_field"),
            )
            engine._parent_key_pool = {}
            engine._key_counter = 0

            seed_df = engine._get_seed_dataframe()
            logger.info(f"  MostlyAI multi-table: {name} has {len(seed_df)} rows, "
                         f"{len(seed_df.columns)} cols")

            table_cfg = {
                "name": name,
                "data": seed_df,
            }

            # Primary key
            pk = cb_cfg.get("primary_key") or cb_cfg.get("key_field")
            if pk:
                table_cfg["primary_key"] = pk

            # Foreign keys (MostlyAI format)
            fks = cb_cfg.get("foreign_keys", [])
            if fks:
                table_cfg["foreign_keys"] = [
                    {
                        "column": fk["column"],
                        "referenced_table": fk["referenced_table"],
                        "is_context": fk.get("is_context", False),
                    }
                    for fk in fks
                ]

            # Training config
            if self.config.mostlyai_max_training_time:
                table_cfg["tabular_model_configuration"] = {
                    "max_training_time": self.config.mostlyai_max_training_time,
                }

            tables_config.append(table_cfg)

        # ── Train the multi-table generator ──────────────────────────────
        logger.info("MostlyAI: training multi-table generator...")
        mostly = MostlyAI(local=True, quiet=True)
        generator = mostly.train(
            config={
                "name": "vsam-pipeline-multitable",
                "tables": tables_config,
            },
            start=True,
            wait=True,
        )
        logger.info("MostlyAI: training complete")

        # ── Cache generator if configured ────────────────────────────────
        gen_path = self.config.mostlyai_generator_path
        if gen_path:
            os.makedirs(os.path.dirname(gen_path) if os.path.dirname(gen_path) else ".", exist_ok=True)
            generator.export_to_file(gen_path)
            logger.info(f"MostlyAI: generator saved to {gen_path}")

        # ── Generate synthetic data ──────────────────────────────────────
        # Size dict: subject tables get parent_records, children are auto-scaled
        size_config = {}
        for name in subject_tables:
            cb_cfg = copybook_configs.get(name, {})
            explicit_records = cb_cfg.get("records", 0)
            size_config[name] = explicit_records if explicit_records > 0 else parent_records

        logger.info(f"MostlyAI: generating with sizes {size_config}...")
        syn_data = mostly.generate(generator, size=size_config)
        all_dfs = syn_data.data(return_type="dict")

        # ── Convert DataFrames to COBOL records and write VSAM files ─────
        results = {}
        for name in self.layouts:
            layout = self.layouts[name]
            cb_cfg = copybook_configs.get(name, {})

            if name in all_dfs:
                syn_df = all_dfs[name]
            else:
                logger.warning(f"MostlyAI: no synthetic data for {name}, skipping")
                continue

            logger.info(f"MostlyAI: formatting {len(syn_df)} records for {name}")

            # Convert DataFrame to COBOL-formatted records
            engine = MostlyAIEngine.__new__(MostlyAIEngine)
            engine.layout = layout
            engine.config = GenerationConfig(
                key_field=cb_cfg.get("key_field"),
                seed=self.config.seed,
            )
            engine._parent_key_pool = {}
            engine._key_counter = 0

            records = engine._dataframe_to_records(syn_df, len(syn_df))
            self._generated_data[name] = records

            # Write output files
            output_file = os.path.join(output_dir, f"{name.lower()}.dat")
            write_config = GenerationConfig(
                output_file=output_file,
                encoding=self.config.encoding,
            )
            writer = VsamWriter(layout, write_config)
            files = {}

            if "dat" in output_formats:
                files["dat"] = writer.write(records)
                logger.info(f"  Written DAT: {files['dat']}")
            if "csv" in output_formats:
                files["csv"] = writer.write_csv(records)
            if "json" in output_formats:
                files["json"] = writer.write_json(records)

            results[name] = {
                "records": records,
                "files": files,
                "layout": layout,
                "stats": {
                    "record_count": len(records),
                    "record_length": layout.record_length,
                    "total_bytes": layout.record_length * len(records),
                    "field_count": len(layout.data_fields),
                },
            }

        return results

    # ── Combined VSAM merge ─────────────────────────────────────────────

    def merge_to_combined_vsam(
        self,
        results: dict[str, Any],
        output_dir: str = "output",
        combined_copybook: Optional[str] = None,
        record_type_map: Optional[dict[str, str]] = None,
        formats: Optional[list[str]] = None,
    ) -> dict[str, Any]:
        """
        Merge multi-table generation results into a single combined VSAM file.

        Records are interleaved hierarchically following FK relationships:
            Parent → Child1 → GrandChild1a, GrandChild1b … → Child2 → …

        Each record is written at a fixed combined length with a 2-byte
        REC-TYPE prefix identifying the source table.

        Args:
            results:            Output from generate_all() / generate_all_mostlyai().
            output_dir:         Directory for combined output files.
            combined_copybook:  Path to a combined copybook (.cpy) that defines the
                                union layout.  If None, a virtual layout is built
                                automatically from the individual layouts.
            record_type_map:    Map of layout_name → 2-char type code.
                                e.g. {"CUSTOMER-RECORD": "CU", ...}
                                If None, auto-generated from first 2 chars.
            formats:            Output formats (dat/csv/json).  Default: all three.

        Returns:
            Dict with: combined_records, files, stats, record_type_map.
        """
        import json as _json
        import csv as _csv

        if not results:
            raise ValueError("No results to merge. Run generation first.")

        os.makedirs(output_dir, exist_ok=True)
        output_formats = formats or ["dat", "csv", "json"]

        # ── Build record type map ─────────────────────────────────────
        if record_type_map is None:
            record_type_map = self._auto_record_type_map(results)
        logger.info(f"Combined VSAM record type map: {record_type_map}")

        # ── Determine combined record length ──────────────────────────
        rec_type_len = 2  # REC-TYPE PIC X(02)
        max_data_len = max(r["layout"].record_length for r in results.values())
        combined_rec_len = rec_type_len + max_data_len

        # If a combined copybook was provided, load it and use its length
        combined_layout = None
        if combined_copybook:
            combined_layout = parse_copybook_file(combined_copybook, name="COMBINED-RECORD")
            combined_rec_len = combined_layout.record_length
            logger.info(f"Using combined copybook: {combined_copybook} "
                        f"(record_length={combined_rec_len})")

        # ── Build FK lookup: parent_layout → (parent_field, child_layout, child_field)
        children_of: dict[str, list[dict]] = {}
        for fk in self._foreign_keys:
            parent = fk["parent_layout"]
            children_of.setdefault(parent, []).append(fk)

        # ── Identify root tables (no FK pointing to them as child) ────
        child_set = {fk["child_layout"] for fk in self._foreign_keys}
        root_tables = [n for n in results if n not in child_set]

        # ── Interleave records hierarchically ─────────────────────────
        combined_records = []

        def _interleave(table_name: str, parent_key_value: Optional[str] = None,
                        parent_field: Optional[str] = None,
                        child_field: Optional[str] = None):
            """Recursively interleave parent → children."""
            if table_name not in results:
                return

            table_records = results[table_name]["records"]
            table_layout = results[table_name]["layout"]
            rec_type = record_type_map.get(table_name, "??")

            for rec in table_records:
                # If we're a child, only include records matching the parent key
                if parent_key_value is not None and child_field is not None:
                    rec_fk_val = rec.get(child_field, "").strip()
                    if rec_fk_val != parent_key_value.strip():
                        continue

                # Build combined record dict
                combined = {"REC-TYPE": rec_type.ljust(2)[:2]}
                # Copy all fields from the source record
                for field in table_layout.data_fields:
                    combined[field.name] = rec.get(field.name, "")

                combined_records.append({
                    "_type": rec_type,
                    "_table": table_name,
                    "_record": combined,
                })

                # Recurse into child tables
                for fk in children_of.get(table_name, []):
                    pk_field = fk["parent_field"]
                    pk_val = rec.get(pk_field, "")
                    _interleave(
                        fk["child_layout"],
                        parent_key_value=pk_val,
                        parent_field=pk_field,
                        child_field=fk["child_field"],
                    )

        for root in root_tables:
            _interleave(root)

        total_records = len(combined_records)
        logger.info(f"Combined VSAM: {total_records} interleaved records "
                     f"(record_length={combined_rec_len})")

        # ── Write combined DAT file ───────────────────────────────────
        files = {}

        if "dat" in output_formats:
            dat_path = os.path.join(output_dir, "combined.dat")
            encoding = "cp037" if self.config.encoding.lower() == "ebcdic" else "ascii"

            with open(dat_path, "wb") as f:
                for entry in combined_records:
                    rec = entry["_record"]
                    table_name = entry["_table"]
                    table_layout = results[table_name]["layout"]

                    # Build byte buffer: REC-TYPE + table data padded to max
                    buf = bytearray(combined_rec_len)

                    # Write REC-TYPE (2 bytes)
                    rec_type_bytes = rec["REC-TYPE"].encode(encoding)[:rec_type_len]
                    buf[:rec_type_len] = rec_type_bytes.ljust(rec_type_len, b" ")

                    # Write table-specific fields at their offsets
                    for field in table_layout.flat_fields:
                        if field.is_filler or field.redefines is not None:
                            continue
                        val = rec.get(field.name, "")
                        if val is None:
                            val = ""
                        val = str(val)

                        offset = rec_type_len + field.offset
                        if offset + field.length > combined_rec_len:
                            continue

                        # Encode based on PIC type
                        from vsam_gen.models import PicType
                        if field.pic_type in (PicType.NUMERIC, PicType.SIGNED_NUMERIC):
                            clean = val.replace("-", "").replace("+", "").replace(".", "")
                            formatted = clean.zfill(field.length)[:field.length]
                        else:
                            formatted = val.ljust(field.length)[:field.length]

                        try:
                            encoded = formatted.encode(encoding)
                        except (UnicodeEncodeError, UnicodeDecodeError):
                            encoded = formatted.encode(encoding, errors="replace")

                        buf[offset:offset + field.length] = encoded[:field.length]

                    f.write(bytes(buf))

            files["dat"] = dat_path
            logger.info(f"  Written combined DAT: {dat_path} "
                        f"({os.path.getsize(dat_path):,} bytes)")

        # ── Write combined CSV file ───────────────────────────────────
        if "csv" in output_formats:
            csv_path = os.path.join(output_dir, "combined.csv")

            # Collect all field names across all layouts
            all_fieldnames = ["REC-TYPE"]
            seen = set(all_fieldnames)
            for name in results:
                for field in results[name]["layout"].data_fields:
                    if field.name not in seen:
                        all_fieldnames.append(field.name)
                        seen.add(field.name)

            with open(csv_path, "w", newline="") as f:
                writer = _csv.DictWriter(f, fieldnames=all_fieldnames,
                                         extrasaction="ignore")
                writer.writeheader()
                for entry in combined_records:
                    row = {k: str(v).strip() for k, v in entry["_record"].items()}
                    writer.writerow(row)

            files["csv"] = csv_path
            logger.info(f"  Written combined CSV: {csv_path}")

        # ── Write combined JSON file ──────────────────────────────────
        if "json" in output_formats:
            json_path = os.path.join(output_dir, "combined.json")

            json_records = []
            for entry in combined_records:
                clean = {k: str(v).strip() for k, v in entry["_record"].items()}
                clean["_table"] = entry["_table"]
                json_records.append(clean)

            with open(json_path, "w") as f:
                _json.dump(json_records, f, indent=2)

            files["json"] = json_path
            logger.info(f"  Written combined JSON: {json_path}")

        # ── Per-type stats ────────────────────────────────────────────
        type_counts = {}
        for entry in combined_records:
            t = entry["_type"]
            type_counts[t] = type_counts.get(t, 0) + 1

        return {
            "combined_records": combined_records,
            "files": files,
            "stats": {
                "total_records": total_records,
                "combined_record_length": combined_rec_len,
                "total_bytes": total_records * combined_rec_len,
                "record_type_counts": type_counts,
            },
            "record_type_map": record_type_map,
        }

    def _auto_record_type_map(self, results: dict[str, Any]) -> dict[str, str]:
        """Generate 2-char record type codes from layout names."""
        type_map = {}
        used_codes = set()
        for name in results:
            # Try common prefixes: CUSTOMER→CU, ACCOUNT→AC, TRANSACTION→TX
            parts = name.replace("-RECORD", "").replace("_RECORD", "").split("-")
            code = parts[0][:2].upper()
            if code in used_codes:
                # Deduplicate by appending a digit
                for i in range(1, 10):
                    alt = code[0] + str(i)
                    if alt not in used_codes:
                        code = alt
                        break
            used_codes.add(code)
            type_map[name] = code
        return type_map

    def describe(self, layout_name: Optional[str] = None) -> str:
        """Print a human-readable description of a layout."""
        from vsam_gen.generator.ai_inferrer import infer_all_fields

        if layout_name:
            layouts = [self.layouts[layout_name]]
        else:
            layouts = list(self.layouts.values())

        lines = []
        for layout in layouts:
            if self.config.ai_infer_types:
                infer_all_fields(layout.fields)
            lines.append(f"{'=' * 70}")
            lines.append(f"Layout: {layout.name}")
            lines.append(f"Record Length: {layout.record_length} bytes")
            lines.append(f"Source: {layout.source_file or 'string'}")
            lines.append(f"{'=' * 70}")
            lines.append(f"{'Lvl':<4} {'Field Name':<30} {'PIC':<15} {'Len':<5} "
                         f"{'Off':<5} {'Semantic Type':<20}")
            lines.append(f"{'-' * 70}")

            for f in layout.fields:
                if f.is_group:
                    indent = "  " * (f.level // 5)
                    lines.append(f"{f.level:02d}   {indent}{f.name}")
                else:
                    pic = f.pic_clause or ""
                    sem = f.semantic_type or ""
                    lines.append(f"{f.level:02d}   {f.name:<30} {pic:<15} {f.length:<5} "
                                 f"{f.offset:<5} {sem}")
            lines.append("")

        return "\n".join(lines)
