"""
Core data models for the VSAM generation pipeline.
"""

from dataclasses import dataclass, field
from enum import Enum
from typing import Optional


class PicType(Enum):
    """COBOL PIC clause data types."""
    ALPHANUMERIC = "X"
    NUMERIC = "9"
    SIGNED_NUMERIC = "S9"
    PACKED_DECIMAL = "COMP-3"
    BINARY = "COMP"
    ALPHABETIC = "A"


class VsamOrgType(Enum):
    """VSAM file organization types."""
    KSDS = "KSDS"   # Key-Sequenced Data Set
    ESDS = "ESDS"   # Entry-Sequenced Data Set
    RRDS = "RRDS"   # Relative Record Data Set


class EngineType(Enum):
    """Data generation engine types."""
    FAKER = "faker"
    MOSTLYAI = "mostlyai"


@dataclass
class CopybookField:
    """Represents a single field parsed from a COBOL copybook."""
    level: int
    name: str
    pic_clause: Optional[str] = None
    pic_type: Optional[PicType] = None
    length: int = 0
    decimal_places: int = 0
    occurs: int = 0
    redefines: Optional[str] = None
    is_filler: bool = False
    is_group: bool = False
    children: list = field(default_factory=list)
    offset: int = 0
    # AI-inferred semantic type (e.g., "name", "date", "phone", "amount")
    semantic_type: Optional[str] = None

    @property
    def total_length(self) -> int:
        """Total byte length including OCCURS repetitions."""
        if self.occurs > 0:
            return self.length * self.occurs
        return self.length

    def __repr__(self):
        pic = f" PIC {self.pic_clause}" if self.pic_clause else ""
        occ = f" OCCURS {self.occurs}" if self.occurs else ""
        return f"<Field L{self.level:02d} {self.name}{pic}{occ} [{self.length}B]>"


@dataclass
class CopybookLayout:
    """Complete parsed copybook layout."""
    name: str
    fields: list  # list of CopybookField
    record_length: int = 0
    source_file: Optional[str] = None

    @property
    def data_fields(self) -> list:
        """Return only elementary (non-group) fields that hold data."""
        return [f for f in self.fields if not f.is_group and not f.is_filler]

    @property
    def flat_fields(self) -> list:
        """Return all elementary fields in order, including fillers."""
        return [f for f in self.fields if not f.is_group]


@dataclass
class GenerationConfig:
    """Configuration for the data generation pipeline."""
    num_records: int = 1000
    output_file: str = "output/vsam_data.dat"
    encoding: str = "ascii"            # "ascii" | "ebcdic" (cp037)
    vsam_type: VsamOrgType = VsamOrgType.KSDS
    key_field: Optional[str] = None    # field name to use as KSDS key
    seed: Optional[int] = None         # reproducibility seed
    locale: str = "en_US"              # Faker locale
    # Engine selection: "faker" (fast, rule-based) or "mostlyai" (AI model-based)
    engine: EngineType = EngineType.FAKER
    # AI inference: auto-detect semantic types from field names
    ai_infer_types: bool = True
    # Custom field overrides: { "FIELD-NAME": {"type": "email", "pattern": "..."} }
    field_overrides: dict = field(default_factory=dict)
    # Multi-table: list of related copybook configs
    related_copybooks: list = field(default_factory=list)
    # Referential integrity links: [{"parent_field": "CUST-ID", "child_copybook": "...", "child_field": "..."}]
    foreign_keys: list = field(default_factory=list)
    # MostlyAI-specific: path to training data CSV (optional, bootstraps from Faker if absent)
    training_data_path: Optional[str] = None
    # MostlyAI-specific: seed sample size for bootstrapping when no training data provided
    mostlyai_seed_size: int = 200
    # MostlyAI-specific: max training time in minutes
    mostlyai_max_training_time: Optional[int] = None
    # MostlyAI-specific: path to save/load trained generator zip
    mostlyai_generator_path: Optional[str] = None


@dataclass
class RelatedCopybook:
    """A related copybook for multi-table generation."""
    copybook_path: str
    output_file: str
    num_records: int = 0   # 0 = auto (ratio-based)
    ratio: float = 3.0     # avg child records per parent
