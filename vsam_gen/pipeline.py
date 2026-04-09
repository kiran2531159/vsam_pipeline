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
