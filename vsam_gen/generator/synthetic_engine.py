"""
Synthetic Data Engine
======================
Generates realistic synthetic data for COBOL copybook fields using the
Faker library, guided by AI-inferred semantic types.

Each semantic type maps to a Faker provider or custom generator that
produces appropriately formatted and sized data.
"""

import random
import string
from datetime import datetime, timedelta
from typing import Any, Optional

from faker import Faker

from vsam_gen.models import CopybookField, CopybookLayout, GenerationConfig, PicType
from vsam_gen.generator.ai_inferrer import infer_all_fields


class SyntheticEngine:
    """
    Generates synthetic records based on a CopybookLayout.

    Usage:
        engine = SyntheticEngine(layout, config)
        records = engine.generate(1000)
    """

    def __init__(self, layout: CopybookLayout, config: Optional[GenerationConfig] = None):
        self.layout = layout
        self.config = config or GenerationConfig()
        self.fake = Faker(self.config.locale)

        if self.config.seed is not None:
            Faker.seed(self.config.seed)
            random.seed(self.config.seed)

        # Run AI inference on fields
        if self.config.ai_infer_types:
            infer_all_fields(self.layout.fields)

        # Key tracking for KSDS uniqueness
        self._generated_keys: set = set()
        self._key_counter: int = 0

        # Foreign key parent values (populated during multi-table gen)
        self._parent_key_pool: dict[str, list] = {}

    def set_parent_key_pool(self, field_name: str, values: list):
        """Set available parent key values for foreign key relationships."""
        self._parent_key_pool[field_name.upper()] = values

    def generate(self, num_records: Optional[int] = None) -> list[dict[str, Any]]:
        """
        Generate the specified number of synthetic records.

        Returns:
            List of dicts, where keys are field names and values are generated data.
        """
        n = num_records or self.config.num_records
        records = []
        for _ in range(n):
            record = self._generate_one_record()
            records.append(record)
        return records

    def _generate_one_record(self) -> dict[str, Any]:
        """Generate a single record with all fields populated."""
        record = {}
        for field in self.layout.data_fields:
            name = field.name

            # Check for user overrides first
            if name in self.config.field_overrides:
                value = self._apply_override(field, self.config.field_overrides[name])
            # Check if this is a foreign key field
            elif name.upper() in self._parent_key_pool:
                pool = self._parent_key_pool[name.upper()]
                value = random.choice(pool) if pool else self._generate_field_value(field)
            # Check if it's the key field (needs uniqueness)
            elif self.config.key_field and name.upper() == self.config.key_field.upper():
                value = self._generate_unique_key(field)
            else:
                value = self._generate_field_value(field)

            # Handle OCCURS (arrays)
            if field.occurs > 0:
                record[name] = [self._generate_field_value(field) for _ in range(field.occurs)]
            else:
                record[name] = value

        return record

    def _generate_unique_key(self, field: CopybookField) -> str:
        """Generate a unique key value for KSDS files."""
        self._key_counter += 1
        key_str = str(self._key_counter)
        if field.pic_type in (PicType.NUMERIC, PicType.SIGNED_NUMERIC,
                              PicType.PACKED_DECIMAL, PicType.BINARY):
            return key_str.zfill(field.length)
        else:
            return key_str.ljust(field.length)[:field.length]

    def _generate_field_value(self, field: CopybookField) -> str:
        """Generate a value for a field based on its semantic type."""
        sem = field.semantic_type
        length = field.length

        # Dispatch to semantic generators
        generators = {
            # ── IDs ──
            "ssn": lambda: self.fake.ssn().replace("-", "")[:length],
            "ein": lambda: self.fake.numerify("##-#######")[:length],
            "account_number": lambda: self.fake.numerify("#" * length),
            "customer_id": lambda: self.fake.numerify("#" * length),
            "employee_id": lambda: self.fake.numerify("#" * length),
            "transaction_id": lambda: self.fake.numerify("#" * length),
            "policy_number": lambda: self.fake.bothify("??#" * 3).upper()[:length],
            "claim_number": lambda: self.fake.numerify("#" * length),
            "order_number": lambda: self.fake.numerify("#" * length),
            "invoice_number": lambda: self.fake.numerify("#" * length),
            "routing_number": lambda: self.fake.numerify("#########")[:length],

            # ── Names ──
            "first_name": lambda: self.fake.first_name()[:length].upper().ljust(length),
            "last_name": lambda: self.fake.last_name()[:length].upper().ljust(length),
            "middle_initial": lambda: random.choice(string.ascii_uppercase)[:length].ljust(length),
            "full_name": lambda: self.fake.name()[:length].upper().ljust(length),
            "company_name": lambda: self.fake.company()[:length].upper().ljust(length),

            # ── Address ──
            "street_address": lambda: self.fake.street_address()[:length].upper().ljust(length),
            "address_line2": lambda: (self.fake.secondary_address() if random.random() > 0.4 else "")[:length].upper().ljust(length),
            "city": lambda: self.fake.city()[:length].upper().ljust(length),
            "state_code": lambda: self.fake.state_abbr()[:length].upper().ljust(length),
            "zip_code": lambda: self.fake.zipcode()[:length].ljust(length),
            "country_code": lambda: "US"[:length].ljust(length),

            # ── Contact ──
            "phone_number": lambda: self.fake.numerify("##########")[:length].ljust(length),
            "email": lambda: self.fake.email()[:length].lower().ljust(length),
            "fax_number": lambda: self.fake.numerify("##########")[:length].ljust(length),
            "url": lambda: self.fake.url()[:length].ljust(length),

            # ── Dates (COBOL format: YYYYMMDD or MMDDYYYY) ──
            "date_of_birth": lambda: self._gen_date(1940, 2005, length),
            "date_past": lambda: self._gen_date(2010, 2024, length),
            "date_future": lambda: self._gen_date(2025, 2035, length),
            "date_recent": lambda: self._gen_date(2023, 2026, length),
            "date_generic": lambda: self._gen_date(2015, 2026, length),

            # ── Time ──
            "time": lambda: self._gen_time(length),

            # ── Financial ──
            "balance_amount": lambda: self._gen_decimal(field, 0, 999999.99),
            "currency_amount": lambda: self._gen_decimal(field, 0, 99999.99),
            "interest_rate": lambda: self._gen_decimal(field, 0.01, 25.0),
            "salary": lambda: self._gen_decimal(field, 25000, 250000),
            "price": lambda: self._gen_decimal(field, 0.01, 9999.99),
            "credit_limit": lambda: self._gen_decimal(field, 1000, 100000),

            # ── Codes / Flags ──
            "status_code": lambda: random.choice(["A", "I", "C", "P", "D"])[:length].ljust(length),
            "type_code": lambda: random.choice(["01", "02", "03", "04", "05"])[:length].ljust(length),
            "generic_code": lambda: self.fake.bothify("?" * min(length, 4)).upper()[:length].ljust(length),
            "flag": lambda: random.choice(["Y", "N"])[:length].ljust(length),
            "gender": lambda: random.choice(["M", "F"])[:length].ljust(length),
            "name_prefix": lambda: random.choice(["MR", "MS", "DR", "MRS"])[:length].ljust(length),
            "name_suffix": lambda: random.choice(["JR", "SR", "II", "III", ""])[:length].ljust(length),

            # ── Numbers ──
            "count": lambda: self._gen_int(field, 0, 10 ** min(length, 6)),
            "percentage": lambda: self._gen_decimal(field, 0, 100),
            "score": lambda: self._gen_int(field, 0, 1000),
            "small_int": lambda: self._gen_int(field, 0, 99),
            "integer": lambda: self._gen_int(field, 0, 10 ** min(length, 8)),
            "decimal_number": lambda: self._gen_decimal(field, 0, 10 ** max(1, length - field.decimal_places - 1)),

            # ── Text ──
            "alpha_text": lambda: self.fake.lexify("?" * length).upper(),
            "text": lambda: self.fake.text(max_nb_chars=length)[:length].upper().ljust(length),
        }

        gen = generators.get(sem)
        if gen:
            return gen()

        # Fallback: fill based on PIC type
        return self._fallback_generate(field)

    def _fallback_generate(self, field: CopybookField) -> str:
        """Fallback generator when no semantic type is matched."""
        if field.pic_type in (PicType.ALPHANUMERIC, PicType.ALPHABETIC):
            return " " * field.length
        elif field.pic_type in (PicType.NUMERIC, PicType.SIGNED_NUMERIC,
                                PicType.PACKED_DECIMAL, PicType.BINARY):
            return "0" * field.length
        return " " * field.length

    def _gen_date(self, year_start: int, year_end: int, length: int) -> str:
        """Generate a date string in COBOL format."""
        start = datetime(year_start, 1, 1)
        end = datetime(year_end, 12, 31)
        delta = (end - start).days
        d = start + timedelta(days=random.randint(0, max(0, delta)))
        if length >= 8:
            return d.strftime("%Y%m%d")[:length]
        elif length >= 6:
            return d.strftime("%y%m%d")[:length]
        else:
            return d.strftime("%m%d")[:length]

    def _gen_time(self, length: int) -> str:
        """Generate a time string in HHMMSS format."""
        h = random.randint(0, 23)
        m = random.randint(0, 59)
        s = random.randint(0, 59)
        t = f"{h:02d}{m:02d}{s:02d}"
        return t[:length].ljust(length, "0")

    def _gen_decimal(self, field: CopybookField, low: float, high: float) -> str:
        """Generate a decimal number formatted for COBOL."""
        val = random.uniform(low, high)
        dec = field.decimal_places
        total = field.length

        if dec > 0:
            # Scale to integer representation (e.g., 123.45 with V99 = "12345")
            int_val = int(round(val * (10 ** dec)))
            return str(abs(int_val)).zfill(total)[:total]
        else:
            return str(int(abs(val))).zfill(total)[:total]

    def _gen_int(self, field: CopybookField, low: int, high: int) -> str:
        """Generate an integer formatted for COBOL."""
        val = random.randint(low, min(high, 10 ** field.length - 1))
        return str(val).zfill(field.length)[:field.length]

    def _apply_override(self, field: CopybookField, override: dict) -> str:
        """Apply a user-defined field override."""
        if "values" in override:
            val = random.choice(override["values"])
            return str(val)[:field.length].ljust(field.length)
        if "pattern" in override:
            return self.fake.bothify(override["pattern"])[:field.length].ljust(field.length)
        if "range" in override:
            low, high = override["range"]
            return self._gen_int(field, low, high)
        if "constant" in override:
            return str(override["constant"])[:field.length].ljust(field.length)
        return self._fallback_generate(field)

    def get_generated_key_values(self) -> list[str]:
        """Return all generated key values (for foreign key linking)."""
        return list(self._generated_keys) if self._generated_keys else [
            str(i + 1).zfill(10) for i in range(self.config.num_records)
        ]
