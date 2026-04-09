"""
VSAM File Writer
=================
Writes generated records to flat files in VSAM-compatible fixed-length format.

Supports:
  - ASCII and EBCDIC (cp037) encoding
  - Fixed-length records (no delimiters)
  - Optional RDW (Record Descriptor Word) headers
  - CSV/JSON export for debugging and downstream use
  - Packed decimal (COMP-3) encoding
"""

import json
import os
import struct
from typing import Any, Optional

from vsam_gen.models import CopybookField, CopybookLayout, GenerationConfig, PicType


class VsamWriter:
    """
    Writes records to VSAM-compatible fixed-length flat files.

    Usage:
        writer = VsamWriter(layout, config)
        writer.write(records)
    """

    def __init__(self, layout: CopybookLayout, config: Optional[GenerationConfig] = None):
        self.layout = layout
        self.config = config or GenerationConfig()
        self.encoding = "cp037" if self.config.encoding.lower() == "ebcdic" else "ascii"

    def write(self, records: list[dict[str, Any]], output_file: Optional[str] = None):
        """
        Write records to a fixed-length flat file.

        Args:
            records:     List of record dicts from the generator.
            output_file: Override output path (defaults to config.output_file).
        """
        filepath = output_file or self.config.output_file
        os.makedirs(os.path.dirname(filepath) if os.path.dirname(filepath) else ".", exist_ok=True)

        with open(filepath, "wb") as f:
            for record in records:
                raw_bytes = self._format_record(record)
                f.write(raw_bytes)

        return filepath

    def write_csv(self, records: list[dict[str, Any]], output_file: Optional[str] = None):
        """Write records as CSV for debugging / downstream consumption."""
        import csv

        filepath = output_file or self.config.output_file.replace(".dat", ".csv")
        os.makedirs(os.path.dirname(filepath) if os.path.dirname(filepath) else ".", exist_ok=True)

        data_fields = self.layout.data_fields
        fieldnames = [f.name for f in data_fields]

        with open(filepath, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
            writer.writeheader()
            for record in records:
                # Flatten OCCURS arrays
                flat = {}
                for key, val in record.items():
                    if isinstance(val, list):
                        flat[key] = "|".join(str(v).strip() for v in val)
                    else:
                        flat[key] = str(val).strip()
                writer.writerow(flat)

        return filepath

    def write_json(self, records: list[dict[str, Any]], output_file: Optional[str] = None):
        """Write records as JSON for API consumption."""
        filepath = output_file or self.config.output_file.replace(".dat", ".json")
        os.makedirs(os.path.dirname(filepath) if os.path.dirname(filepath) else ".", exist_ok=True)

        clean_records = []
        for record in records:
            clean = {}
            for key, val in record.items():
                if isinstance(val, list):
                    clean[key] = [str(v).strip() for v in val]
                else:
                    clean[key] = str(val).strip()
            clean_records.append(clean)

        with open(filepath, "w") as f:
            json.dump(clean_records, f, indent=2)

        return filepath

    def _format_record(self, record: dict[str, Any]) -> bytes:
        """Format a single record as fixed-length bytes."""
        buffer = bytearray(self.layout.record_length)

        for field in self.layout.flat_fields:
            if field.is_filler:
                # Fill with spaces (0x40 in EBCDIC, 0x20 in ASCII)
                fill = b"\x40" if self.encoding == "cp037" else b"\x20"
                effective_len = field.total_length if field.occurs > 0 else field.length
                buffer[field.offset:field.offset + effective_len] = fill * effective_len
                continue

            if field.redefines is not None:
                continue

            value = record.get(field.name)
            if value is None:
                continue

            if isinstance(value, list):
                # OCCURS: write each element consecutively
                for i, v in enumerate(value):
                    start = field.offset + (i * field.length)
                    encoded = self._encode_value(field, str(v))
                    buffer[start:start + field.length] = encoded
            else:
                encoded = self._encode_value(field, str(value))
                buffer[field.offset:field.offset + field.length] = encoded

        return bytes(buffer)

    def _encode_value(self, field: CopybookField, value: str) -> bytes:
        """Encode a single field value into bytes."""
        length = field.length

        if field.pic_type == PicType.PACKED_DECIMAL:
            return self._encode_packed_decimal(value, length)

        if field.pic_type == PicType.BINARY:
            return self._encode_binary(value, length)

        # Standard display encoding
        if field.pic_type in (PicType.NUMERIC, PicType.SIGNED_NUMERIC):
            # Right-justify, zero-fill numeric
            clean = value.replace("-", "").replace("+", "").replace(".", "")
            formatted = clean.zfill(length)[:length]
        else:
            # Left-justify, space-fill alphanumeric
            formatted = value.ljust(length)[:length]

        try:
            return formatted.encode(self.encoding)
        except (UnicodeEncodeError, UnicodeDecodeError):
            # Fallback: replace unencodable chars
            return formatted.encode(self.encoding, errors="replace")

    def _encode_packed_decimal(self, value: str, length: int) -> bytes:
        """
        Encode a value as COMP-3 (packed decimal).
        Each byte holds two digits, last nibble is the sign (C=positive, D=negative).
        """
        clean = value.replace("-", "").replace("+", "").replace(".", "")
        is_negative = "-" in value
        sign_nibble = 0x0D if is_negative else 0x0C

        # Ensure odd number of digits (for proper packing)
        digits = clean.zfill(length * 2 - 1)

        packed = bytearray()
        for i in range(0, len(digits) - 1, 2):
            high = int(digits[i]) if i < len(digits) else 0
            low = int(digits[i + 1]) if i + 1 < len(digits) else 0
            packed.append((high << 4) | low)

        # Last byte: last digit + sign
        last_digit = int(digits[-1]) if digits else 0
        packed.append((last_digit << 4) | sign_nibble)

        # Pad or truncate to exact length
        result = bytes(packed[-length:]).rjust(length, b"\x00")
        return result

    def _encode_binary(self, value: str, length: int) -> bytes:
        """Encode a value as COMP (binary)."""
        clean = value.replace("-", "").replace("+", "").replace(".", "")
        int_val = int(clean) if clean else 0

        if length == 2:
            return struct.pack(">h", int_val)
        elif length == 4:
            return struct.pack(">i", int_val)
        elif length == 8:
            return struct.pack(">q", int_val)
        else:
            return int_val.to_bytes(length, byteorder="big", signed=True)
