# VSAM File Generation Pipeline

**AI-powered synthetic data generator for COBOL VSAM files from copybook layouts.**

Generate realistic, production-like test data for mainframe VSAM files — directly from COBOL copybook definitions. The pipeline uses AI-driven heuristic inference to understand field semantics (names, addresses, SSNs, dates, amounts, etc.) and produce contextually accurate synthetic data without manual configuration.

---

## Table of Contents

- [Problem Statement](#problem-statement)
- [Solution Architecture](#solution-architecture)
- [Features](#features)
- [Project Structure](#project-structure)
- [Installation](#installation)
- [Quick Start](#quick-start)
  - [Python API (3 Lines)](#python-api-3-lines)
  - [Command Line](#command-line)
  - [Multi-Table with Foreign Keys](#multi-table-with-foreign-keys)
- [Detailed Usage](#detailed-usage)
  - [1. Parsing Copybooks](#1-parsing-copybooks)
  - [2. AI Semantic Inference](#2-ai-semantic-inference)
  - [3. Synthetic Data Generation](#3-synthetic-data-generation)
  - [3b. MostlyAI Engine (Optional)](#3b-mostlyai-engine-optional)
  - [4. VSAM File Output](#4-vsam-file-output)
  - [5. Multi-Table Linked Generation](#5-multi-table-linked-generation)
  - [6. YAML Config-Driven Pipeline](#6-yaml-config-driven-pipeline)
- [Copybook Support](#copybook-support)
- [Semantic Type Reference](#semantic-type-reference)
- [Configuration Options](#configuration-options)
- [Integration Guide](#integration-guide)
  - [As a Python Library](#as-a-python-library)
  - [As a CLI Tool](#as-a-cli-tool)
  - [In CI/CD Pipelines](#in-cicd-pipelines)
  - [REST API Wrapper](#rest-api-wrapper)
- [Sample Copybooks](#sample-copybooks)
- [Testing](#testing)
- [How It Works — End to End](#how-it-works--end-to-end)
- [FAQ](#faq)

---

## Problem Statement

Mainframe modernization and testing projects constantly need **realistic VSAM test data**. Creating this data manually is:

- **Tedious**: Fixed-length records with precise byte offsets, packed decimals, EBCDIC encoding.
- **Error-prone**: One-off offset means corrupted records.
- **Slow**: Onboarding new copybook layouts requires re-implementing generators.
- **Incomplete**: Random data looks nothing like production — unrealistic names, invalid dates, nonsensical codes.

This pipeline **automates the entire process**: give it a copybook, get back production-realistic VSAM data files.

---

## Solution Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        USER INPUT                               │
│   COBOL Copybook (.cpy)  +  Config (optional YAML/dict)        │
└──────────────────────┬──────────────────────────────────────────┘
                       │
                       ▼
┌──────────────────────────────────────────────────────────────────┐
│  STAGE 1: COPYBOOK PARSER                                       │
│  ┌────────────────────────────────────────────────────────────┐  │
│  │ • Parse level numbers (01-49, 66, 77, 88)                 │  │
│  │ • Extract PIC clauses (X, 9, S9, A)                       │  │
│  │ • Handle COMP / COMP-3 usage                              │  │
│  │ • Process OCCURS, REDEFINES, FILLER                       │  │
│  │ • Compute byte offsets and record length                   │  │
│  └────────────────────────────────────────────────────────────┘  │
│  Output: CopybookLayout (structured field definitions)           │
└──────────────────────┬──────────────────────────────────────────┘
                       │
                       ▼
┌──────────────────────────────────────────────────────────────────┐
│  STAGE 2: AI SEMANTIC INFERRER                                   │
│  ┌────────────────────────────────────────────────────────────┐  │
│  │ • Pattern-match field names against 60+ semantic rules     │  │
│  │ • CUST-FIRST-NAME → "first_name"                          │  │
│  │ • ACCT-BALANCE    → "balance_amount"                       │  │
│  │ • TXN-DATE        → "date_recent"                          │  │
│  │ • Fallback: PIC-type-based heuristics                      │  │
│  └────────────────────────────────────────────────────────────┘  │
│  Output: Fields annotated with semantic_type                     │
└──────────────────────┬──────────────────────────────────────────┘
                       │
                       ▼
┌──────────────────────────────────────────────────────────────────┐
│  STAGE 3: SYNTHETIC DATA ENGINE                                  │
│  ┌────────────────────────────────────────────────────────────┐  │
│  │ • Faker-powered generators for each semantic type          │  │
│  │ • Realistic: names, addresses, SSNs, dates, amounts        │  │
│  │ • KSDS unique key generation                               │  │
│  │ • Foreign key linking (parent → child)                     │  │
│  │ • User overrides (constants, value lists, patterns)        │  │
│  │ • Reproducible via seed                                    │  │
│  └────────────────────────────────────────────────────────────┘  │
│  Output: List of record dicts                                    │
└──────────────────────┬──────────────────────────────────────────┘
                       │
                       ▼
┌──────────────────────────────────────────────────────────────────┐
│  STAGE 4: VSAM FILE WRITER                                       │
│  ┌────────────────────────────────────────────────────────────┐  │
│  │ • Fixed-length record formatting                           │  │
│  │ • ASCII or EBCDIC (cp037) encoding                         │  │
│  │ • COMP-3 packed decimal encoding                           │  │
│  │ • COMP binary encoding                                     │  │
│  │ • Multi-format output: .dat, .csv, .json                   │  │
│  └────────────────────────────────────────────────────────────┘  │
│  Output: VSAM .dat file + optional CSV/JSON                      │
└──────────────────────────────────────────────────────────────────┘
```

---

## Features

| Feature | Description |
|---|---|
| **Copybook Parsing** | Full support for COBOL PIC clauses, level hierarchies, OCCURS, REDEFINES, FILLER, COMP/COMP-3 |
| **AI Type Inference** | 60+ heuristic rules auto-detect field semantics from names (no manual mapping needed) |
| **Realistic Data** | Names, addresses, SSNs, phones, dates, amounts — all contextually appropriate |
| **Multi-Table** | Generate linked datasets with foreign key referential integrity |
| **VSAM Output** | Fixed-length binary files compatible with KSDS/ESDS/RRDS |
| **EBCDIC Support** | Native EBCDIC (cp037) encoding for mainframe-compatible output |
| **Packed Decimal** | COMP-3 packed decimal encoding for numeric fields |
| **Multi-Format** | Simultaneous .dat, .csv, .json output |
| **Reproducible** | Seed-based generation for deterministic test data |
| **Field Overrides** | Custom values, patterns, ranges, constants per field |
| **CLI + API** | Use from command line or embed in Python applications |
| **YAML Config** | Config-driven multi-table pipelines for complex scenarios |
| **Dual Engine** | Choose Faker (fast, rule-based) or MostlyAI (AI-trained, statistically consistent) |
| **Extensible** | Clean module architecture for adding new types/formats |

---

## Project Structure

```
vsam_pipeline/
├── vsam_gen/                          # Main package
│   ├── __init__.py                    # Public API exports
│   ├── __main__.py                    # python -m vsam_gen entry
│   ├── cli.py                         # Command-line interface
│   ├── models.py                      # Data models (CopybookField, CopybookLayout, etc.)
│   ├── pipeline.py                    # Pipeline orchestrator (VsamPipeline)
│   ├── parser/
│   │   ├── __init__.py
│   │   └── copybook_parser.py         # COBOL copybook parser
│   ├── generator/
│   │   ├── __init__.py
│   │   ├── ai_inferrer.py             # AI semantic type inference engine
│   │   ├── synthetic_engine.py        # Faker-based data generator
│   │   └── mostlyai_engine.py         # MostlyAI-based data generator (optional)
│   └── writer/
│       ├── __init__.py
│       └── vsam_writer.py             # VSAM fixed-length file writer
├── sample_copybooks/                  # Example COBOL copybooks
│   ├── customer.cpy                   # Customer master record
│   ├── account.cpy                    # Account record (FK → Customer)
│   ├── transaction.cpy                # Transaction record (FK → Account)
│   └── employee.cpy                   # Standalone employee record
├── config/
│   └── pipeline_config.yaml           # Multi-table YAML config example
├── tests/
│   └── test_pipeline.py               # Test suite (42 tests)
├── output/                            # Generated data files (gitignored)
├── requirements.txt
├── setup.py
└── README.md
```

---

## Installation

### Prerequisites
- Python 3.10+

### Setup

```bash
# Clone or navigate to the project
cd vsam_pipeline

# Create virtual environment
python3 -m venv .venv
source .venv/bin/activate

# Install dependencies
pip install -r requirements.txt

# (Optional) Install as editable package
pip install -e .

# (Optional) Install with MostlyAI engine support
pip install -e '.[mostlyai]'
# or directly:
pip install 'mostlyai[local]'
```

### Dependencies
- **faker** — Realistic synthetic data generation (default engine)
- **pyyaml** — YAML configuration parsing
- **pandas** — DataFrame processing (installed with MostlyAI)
- **mostlyai[local]** — *(optional)* MostlyAI tabular AI engine for statistically consistent data
- **pytest** — Test framework (dev only)

---

## Quick Start

### Python API (3 Lines)

```python
from vsam_gen import VsamPipeline

pipeline = VsamPipeline()
pipeline.load_copybook("sample_copybooks/customer.cpy")
result = pipeline.generate(num_records=1000, output="output/customer.dat")

# result["files"] → {"dat": "output/customer.dat", "csv": "output/customer.csv", "json": "output/customer.json"}
# result["stats"] → {"record_count": 1000, "record_length": 286, "total_bytes": 286000, ...}
```

### Command Line

```bash
# Generate 1000 customer records
python -m vsam_gen generate \
    --copybook sample_copybooks/customer.cpy \
    --records 1000 \
    --output output/customer.dat \
    --seed 42

# Describe a copybook layout
python -m vsam_gen describe --copybook sample_copybooks/customer.cpy

# Multi-table from YAML config
python -m vsam_gen multi --config config/pipeline_config.yaml
```

### Multi-Table with Foreign Keys

```python
from vsam_gen import VsamPipeline

pipeline = VsamPipeline()
pipeline.load_copybook("sample_copybooks/customer.cpy")
pipeline.load_copybook("sample_copybooks/account.cpy")
pipeline.load_copybook("sample_copybooks/transaction.cpy")

# Define relationships
pipeline.add_foreign_key("CUSTOMER-RECORD", "CUST-ID", "ACCOUNT-RECORD", "ACCT-CUST-ID")
pipeline.add_foreign_key("ACCOUNT-RECORD", "ACCT-NUMBER", "TRANSACTION-RECORD", "TXN-ACCT-NO")

# Generate: 500 customers → 1500 accounts → 4500 transactions
results = pipeline.generate_all(parent_records=500, child_ratio=3.0, output_dir="output")
```

---

## Detailed Usage

### 1. Parsing Copybooks

The parser handles standard COBOL copybook syntax:

```python
from vsam_gen.parser.copybook_parser import parse_copybook, parse_copybook_file

# From file
layout = parse_copybook_file("sample_copybooks/customer.cpy")

# From string
layout = parse_copybook("""
       01  MY-RECORD.
           05  MY-ID       PIC 9(08).
           05  MY-NAME     PIC X(30).
           05  MY-AMOUNT   PIC S9(07)V99.
""")

print(f"Record length: {layout.record_length} bytes")
print(f"Fields: {len(layout.data_fields)}")

for field in layout.data_fields:
    print(f"  {field.name}: PIC {field.pic_clause}, offset={field.offset}, len={field.length}")
```

**What gets parsed:**
- Level numbers (01, 05, 10, 15, etc.)
- PIC clauses: `X(n)`, `9(n)`, `S9(n)V9(m)`, `A(n)`
- USAGE: `COMP`, `COMP-3`, `BINARY`, `PACKED-DECIMAL`
- `OCCURS n TIMES`
- `REDEFINES field-name`
- `FILLER` fields
- Group-level (non-PIC) records
- Fixed-format source (columns 7-72)
- Continuation lines

### 2. AI Semantic Inference

The AI inferrer automatically determines what kind of data each field should contain:

```python
from vsam_gen.generator.ai_inferrer import infer_all_fields
from vsam_gen.parser.copybook_parser import parse_copybook

layout = parse_copybook(copybook_source)
infer_all_fields(layout.fields)

for field in layout.data_fields:
    print(f"  {field.name} → {field.semantic_type}")
    # CUST-FIRST-NAME → first_name
    # CUST-SSN        → ssn
    # ACCT-BALANCE    → balance_amount
    # TXN-DATE        → date_recent
```

**How it works:**
1. The field name (`CUST-FIRST-NAME`) is matched against 60+ regex patterns
2. First matching rule wins (rules are ordered by specificity)
3. If no name matches, the PIC type provides a fallback:
   - `PIC X(n)` with n≤30 → `alpha_text`
   - `PIC 9(n)` with V → `decimal_number`
   - `PIC 9(n)` → `integer`

**To disable AI inference:**
```python
from vsam_gen.models import GenerationConfig
config = GenerationConfig(ai_infer_types=False)
```

### 3. Synthetic Data Generation

The engine maps each semantic type to a Faker provider:

```python
from vsam_gen.generator.synthetic_engine import SyntheticEngine
from vsam_gen.models import GenerationConfig

config = GenerationConfig(
    num_records=100,
    seed=42,               # reproducible
    locale="en_US",        # Faker locale
    key_field="CUST-ID",   # unique keys for KSDS
    field_overrides={
        "CUST-STATUS": {"values": ["A", "I", "C"]},          # pick from list
        "CUST-ACCT-TYPE": {"values": ["SA", "CH", "MM"]},    # savings/checking/money market
        "CUST-COUNTRY": {"constant": "US"},                   # always "US"
    }
)

engine = SyntheticEngine(layout, config)
records = engine.generate()
# records[0] = {"CUST-ID": "0000000001", "CUST-FIRST-NAME": "DANIELLE     ...", ...}
```

**Field override types:**
| Override | Example | Behavior |
|---|---|---|
| `values` | `{"values": ["A","I","C"]}` | Random pick from list |
| `pattern` | `{"pattern": "???-####"}` | Faker bothify pattern |
| `range` | `{"range": [1, 100]}` | Random integer in range |
| `constant` | `{"constant": "US"}` | Fixed value |

### 3b. MostlyAI Engine (Optional)

The MostlyAI engine uses a **tabular AI model** to generate statistically consistent synthetic data.
It trains on seed data (bootstrapped from Faker or user-provided CSV) and produces records that
preserve the statistical distributions of the original data.

```bash
# Install MostlyAI support
pip install 'mostlyai[local]'
```

#### Python API

```python
from vsam_gen import VsamPipeline, EngineType
from vsam_gen.models import GenerationConfig

config = GenerationConfig(
    num_records=1000,
    engine=EngineType.MOSTLYAI,       # use MostlyAI instead of Faker
    seed=42,
    mostlyai_seed_size=200,           # bootstrap 200 Faker records as training data
    # training_data_path="my_data.csv",  # or provide your own training CSV
    # mostlyai_generator_path="gen.pkl", # cache the trained generator
)

pipeline = VsamPipeline(config)
pipeline.load_copybook("sample_copybooks/customer.cpy")
result = pipeline.generate(num_records=1000, output="output/customer.dat")
```

#### CLI

```bash
# Generate using MostlyAI engine
python -m vsam_gen generate \
    --copybook sample_copybooks/customer.cpy \
    --records 1000 \
    --output output/customer.dat \
    --engine mostlyai \
    --seed-size 200

# With user-provided training data
python -m vsam_gen generate \
    --copybook sample_copybooks/customer.cpy \
    --records 5000 \
    --output output/customer.dat \
    --engine mostlyai \
    --training-data real_sample.csv

# Cache the trained generator for reuse
python -m vsam_gen generate \
    --copybook sample_copybooks/customer.cpy \
    --records 1000 \
    --output output/customer.dat \
    --engine mostlyai \
    --generator-path cache/customer_gen.pkl
```

#### YAML Config

```yaml
copybooks:
  - name: CUSTOMER-RECORD
    path: sample_copybooks/customer.cpy
    records: 1000
    engine: mostlyai                 # per-table engine selection
    mostlyai_seed_size: 200
```

#### How It Works

```
Faker (200 seed records)  ──→  MostlyAI Training  ──→  Generate 1000 records
        OR                           │
User CSV (real data)      ──→  Train TabularARGN   ──→  Statistically consistent output
```

1. **Seed data**: Bootstrap a small DataFrame via Faker, or load a user-provided CSV
2. **Train**: MostlyAI trains a TabularARGN generative model on the seed data
3. **Generate**: The model produces any number of synthetic records preserving distributions
4. **Format**: Output is formatted back to COBOL-compatible fixed-length strings

**When to use MostlyAI vs Faker:**

| Aspect | Faker (default) | MostlyAI |
|---|---|---|
| Speed | Fast (~ms) | Slower (training step) |
| Setup | No extra install | `pip install 'mostlyai[local]'` |
| Statistical fidelity | Independent random values | Preserves correlations & distributions |
| Best for | Quick test data, CI/CD | Realistic production-like datasets |
| Training data | Not needed | Bootstrapped from Faker or user CSV |

### 4. VSAM File Output

The writer produces genuine fixed-length record files:

```python
from vsam_gen.writer.vsam_writer import VsamWriter
from vsam_gen.models import GenerationConfig

config = GenerationConfig(encoding="ebcdic")  # or "ascii"
writer = VsamWriter(layout, config)

# Binary fixed-length (.dat)
writer.write(records, "output/data.dat")

# CSV for debugging
writer.write_csv(records, "output/data.csv")

# JSON for API consumption
writer.write_json(records, "output/data.json")
```

**Encoding details:**
- **ASCII mode**: Standard ASCII encoding, space-padded alphanumeric, zero-padded numeric
- **EBCDIC mode**: IBM cp037 encoding, compatible with z/OS VSAM files
- **COMP-3**: Packed decimal encoding (digits + sign nibble)
- **COMP**: Big-endian binary encoding (halfword/fullword/doubleword)

### 5. Multi-Table Linked Generation

Generate parent-child datasets with referential integrity:

```python
pipeline = VsamPipeline()

# Load related layouts
pipeline.load_copybook("customer.cpy")
pipeline.load_copybook("account.cpy")

# Define relationship
pipeline.add_foreign_key(
    parent_layout="CUSTOMER-RECORD",
    parent_field="CUST-ID",
    child_layout="ACCOUNT-RECORD",
    child_field="ACCT-CUST-ID"
)

# Generate: parent records are created first,
# then child records reference valid parent keys
results = pipeline.generate_all(
    parent_records=500,
    child_ratio=3.0,        # avg 3 accounts per customer
    output_dir="output",
    formats=["dat", "csv"]
)

# Verify referential integrity
customer_ids = {r["CUST-ID"] for r in results["CUSTOMER-RECORD"]["records"]}
for acct in results["ACCOUNT-RECORD"]["records"]:
    assert acct["ACCT-CUST-ID"] in customer_ids  # ✓ every account links to a real customer
```

### 6. YAML Config-Driven Pipeline

For complex multi-table scenarios, use a YAML config:

```yaml
# pipeline_config.yaml
pipeline:
  encoding: ascii
  seed: 42
  locale: en_US
  ai_infer_types: true
  parent_records: 500
  child_ratio: 3.0
  output_dir: output
  formats: [dat, csv, json]

copybooks:
  - path: sample_copybooks/customer.cpy
    name: CUSTOMER-RECORD
  - path: sample_copybooks/account.cpy
    name: ACCOUNT-RECORD

foreign_keys:
  - parent_layout: CUSTOMER-RECORD
    parent_field: CUST-ID
    child_layout: ACCOUNT-RECORD
    child_field: ACCT-CUST-ID
```

```bash
python -m vsam_gen multi --config pipeline_config.yaml
```

---

## Copybook Support

| COBOL Feature | Supported | Notes |
|---|:---:|---|
| Level numbers 01-49 | ✅ | Full hierarchy support |
| Level 77 (standalone) | ✅ | Treated as elementary |
| Level 88 (condition) | ⚠️ | Parsed but not generated |
| PIC X(n) — alphanumeric | ✅ | Left-justified, space-padded |
| PIC 9(n) — numeric | ✅ | Right-justified, zero-padded |
| PIC S9(n) — signed numeric | ✅ | Sign handled |
| PIC A(n) — alphabetic | ✅ | |
| PIC 9(n)V9(m) — implied decimal | ✅ | Stored as integer (no V in data) |
| COMP / BINARY | ✅ | Big-endian 2/4/8 byte |
| COMP-3 / PACKED-DECIMAL | ✅ | BCD + sign nibble |
| OCCURS n TIMES | ✅ | Fixed-count arrays |
| REDEFINES | ✅ | No duplicate space allocation |
| FILLER | ✅ | Space/zero filled |
| Fixed-format (cols 7-72) | ✅ | Sequence numbers stripped |
| Continuation lines | ✅ | Column 7 hyphen |
| Comment lines (* in col 7) | ✅ | Skipped |

---

## Semantic Type Reference

The AI inferrer recognizes these field name patterns and maps them to data generators:

| Semantic Type | Field Name Patterns | Example Output |
|---|---|---|
| `first_name` | FIRST-NAME, FNAME | `DANIELLE` |
| `last_name` | LAST-NAME, SURNAME | `JOHNSON` |
| `full_name` | CUST-NAME, EMP-NAME | `JOHN SMITH` |
| `middle_initial` | MID-INIT, MIDDLE-NAME | `R` |
| `ssn` | SSN, SOC-SEC | `251292287` |
| `customer_id` | CUST-ID, CLIENT-ID | `0000000001` |
| `account_number` | ACCT-NUM, ACCOUNT-NO | `000000123456` |
| `street_address` | ADDR-LINE-1, STREET | `819 JOHNSON COURSE` |
| `city` | CITY, TOWN | `EAST WILLIAM` |
| `state_code` | STATE, ST | `AK` |
| `zip_code` | ZIP-CODE, POSTAL | `74064` |
| `phone_number` | PHONE, MOBILE | `3863794026` |
| `email` | EMAIL, E-MAIL | `joshua35@example.org` |
| `date_of_birth` | DOB, BIRTH-DATE | `19970513` |
| `date_past` | OPEN-DATE, CREATE-DATE | `20150103` |
| `date_recent` | TRANS-DATE, POST-DATE | `20240815` |
| `balance_amount` | BALANCE, BAL | `000148142` |
| `currency_amount` | AMOUNT, AMT, PAYMENT | `000045623` |
| `interest_rate` | RATE, INT-RATE, APR | `0050125` |
| `salary` | SALARY, WAGE | `0007850000` |
| `status_code` | STATUS, STAT | `A` |
| `type_code` | TYPE, TYP | `03` |
| `flag` | FLAG, IND, INDICATOR | `Y` |
| `gender` | GENDER, SEX | `M` |
| `company_name` | COMPANY, EMPLOYER | `SMITH AND SONS` |

*60+ rules total — see [ai_inferrer.py](vsam_gen/generator/ai_inferrer.py) for the complete list.*

---

## Configuration Options

### GenerationConfig

| Parameter | Type | Default | Description |
|---|---|---|---|
| `num_records` | int | 1000 | Records to generate |
| `output_file` | str | `output/vsam_data.dat` | Output file path |
| `encoding` | str | `ascii` | `ascii` or `ebcdic` (cp037) |
| `vsam_type` | VsamOrgType | `KSDS` | `KSDS`, `ESDS`, or `RRDS` |
| `key_field` | str | auto-detected | Field name for KSDS unique key |
| `seed` | int | None | Random seed for reproducibility |
| `locale` | str | `en_US` | Faker locale for regional data |
| `ai_infer_types` | bool | True | Enable AI semantic inference |
| `field_overrides` | dict | `{}` | Per-field custom generators |

### CLI Flags

```
generate:
  -c, --copybook     Path to COBOL copybook file (required)
  -n, --records      Number of records (default: 1000)
  -o, --output       Output file path
  -e, --encoding     ascii | ebcdic
  -t, --type         KSDS | ESDS | RRDS
  -k, --key-field    Key field name
  -s, --seed         Random seed
  -f, --formats      Output formats: dat csv json
  --locale           Faker locale
  --no-ai            Disable AI inference
  -v, --verbose      Verbose logging

describe:
  -c, --copybook     Path to COBOL copybook file (required)

multi:
  --config           Path to YAML config file (required)
```

---

## Integration Guide

### As a Python Library

The pipeline is designed as a clean importable module:

```python
# Minimal
from vsam_gen import VsamPipeline

pipeline = VsamPipeline()
pipeline.load_copybook("my_copybook.cpy")
data = pipeline.generate(num_records=100)
records = data["records"]  # list of dicts

# Advanced — with config
from vsam_gen import VsamPipeline, GenerationConfig

config = GenerationConfig(
    num_records=5000,
    encoding="ebcdic",
    seed=12345,
    key_field="POLICY-NUMBER",
    field_overrides={
        "STATUS-CODE": {"values": ["ACT", "SUS", "CAN"]},
    }
)
pipeline = VsamPipeline(config)
pipeline.load_copybook_string(my_copybook_text)
result = pipeline.generate(output="data/policies.dat", formats=["dat"])
```

### As a CLI Tool

```bash
# Install globally
pip install -e /path/to/vsam_pipeline

# Use anywhere
vsam-gen generate -c /path/to/copybook.cpy -n 10000 -o /output/data.dat
```

### In CI/CD Pipelines

```yaml
# GitHub Actions example
steps:
  - name: Generate test data
    run: |
      pip install -r vsam_pipeline/requirements.txt
      python -m vsam_gen generate \
        --copybook copybooks/claims.cpy \
        --records 10000 \
        --output test-data/claims.dat \
        --seed 42 \
        --formats dat csv
```

### REST API Wrapper

Wrap the pipeline in a lightweight Flask/FastAPI endpoint:

```python
from fastapi import FastAPI, UploadFile
from vsam_gen import VsamPipeline, GenerationConfig

app = FastAPI()

@app.post("/generate")
async def generate(copybook: UploadFile, num_records: int = 100):
    content = (await copybook.read()).decode()
    config = GenerationConfig(num_records=num_records)
    pipeline = VsamPipeline(config)
    pipeline.load_copybook_string(content)
    result = pipeline.generate(formats=["json"])
    return {
        "stats": result["stats"],
        "records": result["records"][:10],  # preview
        "files": result["files"],
    }
```

---

## Sample Copybooks

The project includes four sample copybooks demonstrating different patterns:

### Customer (`sample_copybooks/customer.cpy`)
- 19 data fields, 286 bytes/record
- Group-level hierarchy (personal info, address, contact, account)
- COMP-3 packed decimal (credit limit)
- FILLER padding

### Account (`sample_copybooks/account.cpy`)
- Foreign key to Customer (ACCT-CUST-ID → CUST-ID)
- Signed decimal amounts (balance, credit limit)
- Interest rate with 4 decimal places

### Transaction (`sample_copybooks/transaction.cpy`)
- Foreign key to Account (TXN-ACCT-NO → ACCT-NUMBER)
- Date + time fields
- Transaction description text
- Merchant information

### Employee (`sample_copybooks/employee.cpy`)
- Standalone record (no foreign keys)
- COMP-3 salary
- Department code

---

## Testing

```bash
# Activate virtual environment
source .venv/bin/activate

# Run all tests
python -m pytest tests/test_pipeline.py -v

# Run specific test class
python -m pytest tests/test_pipeline.py::TestCopybookParser -v
python -m pytest tests/test_pipeline.py::TestAIInferrer -v
python -m pytest tests/test_pipeline.py::TestSyntheticEngine -v
python -m pytest tests/test_pipeline.py::TestVsamWriter -v
python -m pytest tests/test_pipeline.py::TestPipeline -v

# Run MostlyAI engine tests (no SDK required — uses mocks)
python -m pytest tests/test_pipeline.py::TestMostlyAIEngine -v
```

### Test Coverage

| Test Suite | Tests | What's Covered |
|---|:---:|---|
| `TestCopybookParser` | 10 | PIC parsing, offsets, lengths, groups, fillers, files |
| `TestAIInferrer` | 10 | Name/SSN/date/phone/status/amount inference, fallbacks |
| `TestSyntheticEngine` | 6 | Record generation, field lengths, unique keys, seeds, overrides |
| `TestVsamWriter` | 4 | DAT/CSV/JSON output, fixed-length validation |
| `TestPipeline` | 7 | End-to-end string/file, describe, multi-table FK, engine selection |
| `TestMostlyAIEngine` | 5 | Import guard, value formatting, DataFrame conversion, seed bootstrap |
| **Total** | **42** | |

---

## How It Works — End to End

Let's trace what happens when you run:

```python
pipeline = VsamPipeline()
pipeline.load_copybook("sample_copybooks/customer.cpy")
result = pipeline.generate(num_records=3, output="output/demo.dat")
```

**Step 1 — Parse** (`copybook_parser.py`):
```
Input:  "05  CUST-FIRST-NAME  PIC X(25)."
Output: CopybookField(level=5, name="CUST-FIRST-NAME", pic_type=ALPHANUMERIC, length=25, offset=10)
```

**Step 2 — Infer** (`ai_inferrer.py`):
```
Input:  field.name = "CUST-FIRST-NAME"
Match:  r"(FIRST.*NAME|FNAME|GIVEN.*NAME)" → "first_name"
Output: field.semantic_type = "first_name"
```

**Step 3 — Generate** (`synthetic_engine.py`):
```
Input:  semantic_type = "first_name", length = 25
Action: faker.first_name() → "Danielle"
Output: "DANIELLE                 " (upper-cased, left-justified, space-padded to 25)
```

**Step 4 — Write** (`vsam_writer.py`):
```
Input:  record dict with all fields populated
Action: Place each value at its byte offset in a 286-byte buffer
Output: 286 bytes of fixed-length binary data per record
```

**Result:**
```
output/demo.dat  — 858 bytes (3 × 286)
output/demo.csv  — Human-readable CSV with headers
output/demo.json — JSON array of 3 records
```

---

## FAQ

**Q: Can I use my own copybook?**
A: Yes. Point `--copybook` or `load_copybook()` to any standard COBOL copybook file. The parser handles most common syntax automatically.

**Q: Does it produce real EBCDIC files?**
A: Yes. Set `encoding: ebcdic` and the output uses IBM code page 037 (cp037), compatible with z/OS VSAM.

**Q: How does it handle COMP-3 (packed decimal)?**
A: Fields with `USAGE COMP-3` are encoded as BCD with a trailing sign nibble (C=positive, D=negative), exactly matching mainframe packed decimal format.

**Q: Can I control what data a specific field gets?**
A: Yes, use `field_overrides` in the config:
```python
config = GenerationConfig(field_overrides={
    "STATUS": {"values": ["A", "I"]},
    "COUNTRY": {"constant": "US"},
    "REF-NUM": {"pattern": "REF-######"},
})
```

**Q: How do I ensure unique keys?**
A: Set `key_field="FIELD-NAME"` and keys auto-increment with zero-padding.

**Q: Can I generate data in non-English locales?**
A: Yes. Set `locale="de_DE"` (German), `"fr_FR"` (French), `"ja_JP"` (Japanese), etc. Faker supports 50+ locales.

**Q: How do I add a new semantic type?**
A: Add a pattern to `INFERENCE_RULES` in `ai_inferrer.py` and a corresponding generator lambda in `synthetic_engine.py`'s `generators` dict.

**Q: What if the AI inference gets a field wrong?**
A: Use `field_overrides` to explicitly set that field's generation strategy, or set `ai_infer_types=False` and rely on PIC-type fallbacks.

---

## License

MIT

---

*Built for mainframe modernization teams who need production-realistic test data without the production data.*
