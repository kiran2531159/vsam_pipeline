"""
COBOL Copybook Parser
======================
Parses standard COBOL copybook definitions into structured CopybookLayout objects.

Supports:
  - Level numbers (01-49, 66, 77, 88)
  - PIC X, PIC 9, PIC S9, PIC A clauses
  - PIC shorthand: PIC X(10), PIC 9(5)V9(2)
  - OCCURS clause (fixed count)
  - REDEFINES clause
  - FILLER fields
  - COMP / COMP-3 (packed decimal) usage
  - Group-level (non-PIC) records
  - Nested hierarchies (01 > 05 > 10 > ...)
"""

import re
from typing import Optional

from vsam_gen.models import CopybookField, CopybookLayout, PicType


# ── regex patterns ────────────────────────────────────────────────────────────

# Match a full COBOL data definition statement (may span multiple lines)
_RE_LEVEL = re.compile(
    r"^\s*(\d{2})\s+"            # level number
    r"([\w-]+|FILLER)",          # field name
    re.IGNORECASE
)

_RE_PIC = re.compile(
    r"PIC(?:TURE)?\s+IS\s+([\w\(\)V\.\+\-]+)"
    r"|PIC(?:TURE)?\s+([\w\(\)V\.\+\-]+)",
    re.IGNORECASE
)

_RE_OCCURS = re.compile(r"OCCURS\s+(\d+)\s+TIMES?", re.IGNORECASE)
_RE_REDEFINES = re.compile(r"REDEFINES\s+([\w-]+)", re.IGNORECASE)
_RE_USAGE = re.compile(
    r"(?:USAGE\s+IS\s+|USAGE\s+)?(COMP-3|COMP-1|COMP-2|COMP|COMPUTATIONAL-3|COMPUTATIONAL|BINARY|PACKED-DECIMAL|DISPLAY)",
    re.IGNORECASE
)

# ── PIC clause length computation ────────────────────────────────────────────

def _expand_pic(pic_raw: str) -> str:
    """Expand shorthand like X(10) -> XXXXXXXXXX, 9(5)V9(2) -> 99999V99"""
    def replacer(m):
        char = m.group(1)
        count = int(m.group(2))
        return char * count
    return re.sub(r"([XxAa9SsVvPp])\((\d+)\)", replacer, pic_raw)


def _compute_pic_length(pic_raw: str) -> tuple[int, int, PicType]:
    """
    Compute (byte_length, decimal_places, pic_type) from a PIC clause string.
    """
    expanded = _expand_pic(pic_raw.upper())

    # Determine type
    is_signed = expanded.startswith("S") or "S" in expanded
    has_alpha = "X" in expanded or "A" in expanded

    if has_alpha:
        pic_type = PicType.ALPHANUMERIC if "X" in expanded else PicType.ALPHABETIC
    elif is_signed:
        pic_type = PicType.SIGNED_NUMERIC
    else:
        pic_type = PicType.NUMERIC

    # Remove sign indicator for length calc
    clean = expanded.replace("S", "")

    # Count digits/chars, compute decimal places
    decimal_places = 0
    if "V" in clean:
        integer_part, decimal_part = clean.split("V", 1)
        decimal_places = len(decimal_part.replace(".", ""))
        total_chars = len(integer_part) + decimal_places
    else:
        total_chars = len(clean.replace(".", ""))

    byte_length = total_chars
    return byte_length, decimal_places, pic_type


def _compute_usage_length(byte_length: int, usage: str) -> int:
    """Adjust byte length for COMP/COMP-3 usage."""
    usage_upper = usage.upper().replace("COMPUTATIONAL", "COMP").replace("PACKED-DECIMAL", "COMP-3")

    if "COMP-3" in usage_upper:
        # Packed decimal: (digits + 1) / 2 rounded up
        return (byte_length + 2) // 2
    elif "COMP" in usage_upper or "BINARY" in usage_upper:
        # Binary: 1-4 digits=2 bytes, 5-9=4 bytes, 10-18=8 bytes
        if byte_length <= 4:
            return 2
        elif byte_length <= 9:
            return 4
        else:
            return 8
    return byte_length


# ── main parser ──────────────────────────────────────────────────────────────

def _normalize_source(text: str) -> list[str]:
    """
    Normalize copybook source: strip sequence numbers and comment indicators,
    join continuation lines, split on periods into individual statements.
    """
    lines = text.splitlines()
    clean_lines = []
    for line in lines:
        # Skip empty lines
        if not line.strip():
            continue
        # Handle fixed-format: cols 1-6 are sequence, col 7 is indicator
        if len(line) > 6 and line[6] in ("*", "/", "d", "D"):
            continue  # comment line
        # Strip sequence area (cols 1-6) if present in fixed format
        if len(line) > 72:
            content = line[6:72]
        elif len(line) > 6 and line[:6].strip().isdigit():
            content = line[6:]
        else:
            content = line
        # Handle continuation (col 7 = '-')
        if len(line) > 6 and line[6] == "-":
            # Continuation: append to previous line
            if clean_lines:
                clean_lines[-1] = clean_lines[-1].rstrip() + " " + content.strip()
                continue
        clean_lines.append(content)

    # Join all and split on period (COBOL statement terminator)
    full_text = " ".join(clean_lines)
    # Split on period but keep the period for parsing
    statements = [s.strip() + "." for s in full_text.split(".") if s.strip()]
    return statements


def parse_copybook(source: str, name: Optional[str] = None) -> CopybookLayout:
    """
    Parse a COBOL copybook source string into a CopybookLayout.

    Args:
        source: Raw copybook text.
        name:   Optional layout name (defaults to first 01-level name).

    Returns:
        CopybookLayout with all fields parsed and offsets computed.
    """
    statements = _normalize_source(source)
    fields: list[CopybookField] = []
    layout_name = name

    for stmt in statements:
        level_m = _RE_LEVEL.match(stmt)
        if not level_m:
            continue

        level = int(level_m.group(1))
        raw_name = level_m.group(2).strip()

        is_filler = raw_name.upper() == "FILLER"
        field_name = raw_name if not is_filler else f"FILLER-{len(fields)+1:03d}"

        # Set layout name from first 01-level
        if level == 1 and not layout_name:
            layout_name = raw_name

        # PIC clause
        pic_m = _RE_PIC.search(stmt)
        pic_clause = None
        pic_type = None
        byte_length = 0
        decimal_places = 0

        if pic_m:
            pic_clause = (pic_m.group(1) or pic_m.group(2)).strip()
            byte_length, decimal_places, pic_type = _compute_pic_length(pic_clause)

        # USAGE clause
        usage_m = _RE_USAGE.search(stmt)
        usage = None
        if usage_m:
            usage = usage_m.group(1)
            if pic_clause and usage:
                byte_length = _compute_usage_length(byte_length, usage)
                if "COMP-3" in usage.upper() or "PACKED" in usage.upper():
                    pic_type = PicType.PACKED_DECIMAL
                elif "COMP" in usage.upper() or "BINARY" in usage.upper():
                    pic_type = PicType.BINARY

        # OCCURS clause
        occurs_m = _RE_OCCURS.search(stmt)
        occurs = int(occurs_m.group(1)) if occurs_m else 0

        # REDEFINES clause
        redefines_m = _RE_REDEFINES.search(stmt)
        redefines = redefines_m.group(1) if redefines_m else None

        is_group = pic_clause is None and level < 88

        f = CopybookField(
            level=level,
            name=field_name,
            pic_clause=pic_clause,
            pic_type=pic_type,
            length=byte_length,
            decimal_places=decimal_places,
            occurs=occurs,
            redefines=redefines,
            is_filler=is_filler,
            is_group=is_group,
        )
        fields.append(f)

    # ── compute offsets and group lengths ──────────────────────────────
    _compute_offsets(fields)
    record_length = _compute_record_length(fields)

    return CopybookLayout(
        name=layout_name or "UNKNOWN",
        fields=fields,
        record_length=record_length,
    )


def parse_copybook_file(filepath: str, name: Optional[str] = None) -> CopybookLayout:
    """Parse a copybook from a file path."""
    with open(filepath, "r") as f:
        source = f.read()
    layout = parse_copybook(source, name)
    layout.source_file = filepath
    return layout


def _compute_offsets(fields: list[CopybookField]):
    """Compute byte offset for each field in the record."""
    offset = 0
    level_stack: list[tuple[int, int]] = []  # (level, start_offset)

    for f in fields:
        # If level is same or higher than previous, adjust stack
        while level_stack and level_stack[-1][0] >= f.level:
            level_stack.pop()

        if f.is_group:
            level_stack.append((f.level, offset))
            f.offset = offset
        else:
            f.offset = offset
            effective_len = f.total_length if f.occurs > 0 else f.length
            if f.redefines is None:
                offset += effective_len


def _compute_record_length(fields: list[CopybookField]) -> int:
    """Compute total record byte length from elementary fields."""
    total = 0
    for f in fields:
        if f.is_group or f.redefines is not None:
            continue
        total += f.total_length if f.occurs > 0 else f.length
    return total
