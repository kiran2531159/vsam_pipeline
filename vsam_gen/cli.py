"""
CLI Interface for VSAM Pipeline
=================================
Command-line tool for generating VSAM files from COBOL copybooks.

Usage:
    python -m vsam_gen.cli generate --copybook customer.cpy --records 1000 --output output/customer.dat
    python -m vsam_gen.cli describe --copybook customer.cpy
    python -m vsam_gen.cli multi --config pipeline_config.yaml
"""

import argparse
import sys
import yaml
import logging
from pathlib import Path

from vsam_gen.pipeline import VsamPipeline
from vsam_gen.models import GenerationConfig, VsamOrgType, EngineType


def main():
    parser = argparse.ArgumentParser(
        prog="vsam-gen",
        description="AI-powered VSAM file generator from COBOL copybooks",
    )
    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # ── generate command ──────────────────────────────────────────────
    gen_parser = subparsers.add_parser("generate", help="Generate VSAM data from a copybook")
    gen_parser.add_argument("-c", "--copybook", required=True, help="Path to COBOL copybook file")
    gen_parser.add_argument("-n", "--records", type=int, default=1000, help="Number of records")
    gen_parser.add_argument("-o", "--output", default="output/vsam_data.dat", help="Output file path")
    gen_parser.add_argument("-e", "--encoding", choices=["ascii", "ebcdic"], default="ascii")
    gen_parser.add_argument("-t", "--type", choices=["KSDS", "ESDS", "RRDS"], default="KSDS")
    gen_parser.add_argument("-k", "--key-field", help="Key field name for KSDS")
    gen_parser.add_argument("-s", "--seed", type=int, help="Random seed for reproducibility")
    gen_parser.add_argument("-f", "--formats", nargs="+", default=["dat", "csv", "json"],
                            choices=["dat", "csv", "json"], help="Output formats")
    gen_parser.add_argument("--locale", default="en_US", help="Faker locale")
    gen_parser.add_argument("--no-ai", action="store_true", help="Disable AI type inference")
    gen_parser.add_argument("--engine", choices=["faker", "mostlyai"], default="faker",
                            help="Data generation engine (default: faker)")
    gen_parser.add_argument("--training-data", help="CSV file with training data (MostlyAI engine)")
    gen_parser.add_argument("--seed-size", type=int, default=200,
                            help="Bootstrap seed size for MostlyAI (default: 200)")
    gen_parser.add_argument("--max-training-time", type=int,
                            help="Max training time in minutes (MostlyAI)")
    gen_parser.add_argument("--generator-path",
                            help="Path to save/load MostlyAI generator (.zip)")
    gen_parser.add_argument("-v", "--verbose", action="store_true", help="Verbose output")

    # ── describe command ──────────────────────────────────────────────
    desc_parser = subparsers.add_parser("describe", help="Describe a copybook layout")
    desc_parser.add_argument("-c", "--copybook", required=True, help="Path to COBOL copybook file")

    # ── multi command ─────────────────────────────────────────────────
    multi_parser = subparsers.add_parser("multi", help="Multi-table generation from YAML config")
    multi_parser.add_argument("--config", required=True, help="Path to YAML pipeline config")
    multi_parser.add_argument("-v", "--verbose", action="store_true")

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        sys.exit(1)

    if args.command == "generate":
        _cmd_generate(args)
    elif args.command == "describe":
        _cmd_describe(args)
    elif args.command == "multi":
        _cmd_multi(args)


def _cmd_generate(args):
    """Handle the generate command."""
    if args.verbose:
        logging.getLogger("vsam_gen").setLevel(logging.DEBUG)

    config = GenerationConfig(
        num_records=args.records,
        output_file=args.output,
        encoding=args.encoding,
        vsam_type=VsamOrgType[args.type],
        key_field=args.key_field,
        seed=args.seed,
        locale=args.locale,
        engine=EngineType(args.engine),
        ai_infer_types=not args.no_ai,
        training_data_path=args.training_data,
        mostlyai_seed_size=args.seed_size,
        mostlyai_max_training_time=args.max_training_time,
        mostlyai_generator_path=args.generator_path,
    )

    pipeline = VsamPipeline(config)
    pipeline.load_copybook(args.copybook)
    result = pipeline.generate(formats=args.formats)

    print(f"\nGeneration complete:")
    print(f"  Records:  {result['stats']['record_count']:,}")
    print(f"  RecLen:   {result['stats']['record_length']} bytes")
    print(f"  Total:    {result['stats']['total_bytes']:,} bytes")
    print(f"  Files:")
    for fmt, path in result["files"].items():
        print(f"    {fmt.upper()}: {path}")


def _cmd_describe(args):
    """Handle the describe command."""
    pipeline = VsamPipeline()
    pipeline.load_copybook(args.copybook)
    print(pipeline.describe())


def _cmd_multi(args):
    """Handle the multi-table config-driven generation."""
    config_path = Path(args.config)
    if not config_path.exists():
        print(f"Error: Config file not found: {config_path}")
        sys.exit(1)

    with open(config_path) as f:
        cfg = yaml.safe_load(f)

    pipeline_cfg = cfg.get("pipeline", {})
    engine_str = pipeline_cfg.get("engine", "faker")
    config = GenerationConfig(
        encoding=pipeline_cfg.get("encoding", "ascii"),
        seed=pipeline_cfg.get("seed"),
        locale=pipeline_cfg.get("locale", "en_US"),
        engine=EngineType(engine_str),
        ai_infer_types=pipeline_cfg.get("ai_infer_types", True),
        training_data_path=pipeline_cfg.get("training_data_path"),
        mostlyai_seed_size=pipeline_cfg.get("mostlyai_seed_size", 200),
        mostlyai_max_training_time=pipeline_cfg.get("mostlyai_max_training_time"),
        mostlyai_generator_path=pipeline_cfg.get("mostlyai_generator_path"),
    )

    pipeline = VsamPipeline(config)

    # Load copybooks with per-table settings from YAML
    copybook_configs = {}
    for cb in cfg.get("copybooks", []):
        layout = pipeline.load_copybook(cb["path"], name=cb.get("name"))
        copybook_configs[layout.name] = cb

    # Add foreign keys
    for fk in cfg.get("foreign_keys", []):
        pipeline.add_foreign_key(
            fk["parent_layout"], fk["parent_field"],
            fk["child_layout"], fk["child_field"],
        )

    # Choose generation strategy
    if engine_str == "mostlyai" and any(
        cb.get("foreign_keys") for cb in cfg.get("copybooks", [])
    ):
        # Multi-table MostlyAI mode: train all tables together
        results = pipeline.generate_all_mostlyai(
            copybook_configs=copybook_configs,
            output_dir=pipeline_cfg.get("output_dir", "output"),
            formats=pipeline_cfg.get("formats", ["dat", "csv", "json"]),
            parent_records=pipeline_cfg.get("parent_records", 500),
            child_ratio=pipeline_cfg.get("child_ratio", 3.0),
        )
    else:
        # Standard mode: per-table generation with FK propagation
        # Pass per-copybook training_data into pipeline
        pipeline.set_copybook_configs(copybook_configs)
        results = pipeline.generate_all(
            parent_records=pipeline_cfg.get("parent_records", 500),
            child_ratio=pipeline_cfg.get("child_ratio", 3.0),
            output_dir=pipeline_cfg.get("output_dir", "output"),
            formats=pipeline_cfg.get("formats", ["dat", "csv", "json"]),
        )

    print(f"\nMulti-table generation complete:")
    for name, result in results.items():
        print(f"\n  {name}:")
        print(f"    Records: {result['stats']['record_count']:,}")
        print(f"    Files:   {list(result['files'].values())}")

    # ── Combined VSAM merge ───────────────────────────────────────────
    if pipeline_cfg.get("combined_output"):
        combined = pipeline.merge_to_combined_vsam(
            results=results,
            output_dir=pipeline_cfg.get("output_dir", "output"),
            combined_copybook=pipeline_cfg.get("combined_copybook"),
            record_type_map=pipeline_cfg.get("record_type_map"),
            formats=pipeline_cfg.get("formats", ["dat", "csv", "json"]),
        )
        print(f"\n  COMBINED VSAM:")
        print(f"    Total records: {combined['stats']['total_records']:,}")
        print(f"    Record length: {combined['stats']['combined_record_length']} bytes")
        print(f"    Type counts:   {combined['stats']['record_type_counts']}")
        print(f"    Files:         {list(combined['files'].values())}")


if __name__ == "__main__":
    main()
