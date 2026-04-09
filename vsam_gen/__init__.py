"""
VSAM File Generation Pipeline
==============================
AI-powered synthetic data generator for COBOL VSAM files.
Parses copybook layouts and produces realistic fixed-length records.

Usage:
    from vsam_gen import VsamPipeline

    pipeline = VsamPipeline()
    pipeline.load_copybook("path/to/copybook.cpy")
    pipeline.generate(num_records=1000, output="output/data.dat")
"""

__version__ = "1.0.0"

from vsam_gen.pipeline import VsamPipeline
from vsam_gen.models import CopybookField, CopybookLayout, GenerationConfig, EngineType

__all__ = ["VsamPipeline", "CopybookField", "CopybookLayout", "GenerationConfig", "EngineType"]
