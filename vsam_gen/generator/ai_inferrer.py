"""
AI Field Type Inferrer
=======================
Infers semantic data types from COBOL field names using pattern matching
and heuristic AI rules. This allows the generator to produce realistic
synthetic data without manual field-by-field configuration.

The inferrer maps field names like "CUST-FIRST-NAME" -> "first_name",
"ACCT-BALANCE" -> "currency_amount", "CUST-SSN" -> "ssn", etc.
"""

import re
from typing import Optional

from vsam_gen.models import CopybookField, PicType


# ── semantic type rules ──────────────────────────────────────────────────────
# Each rule: (regex_pattern_for_field_name, semantic_type, optional_kwargs)
# Rules are evaluated top-down; first match wins.

INFERENCE_RULES: list[tuple[str, str]] = [
    # ── Names (before IDs to prevent CUST-ID matching CUST-MIDDLE-INIT) ──
    (r"(FIRST.*NAME|FNAME|GIVEN.*NAME)", "first_name"),
    (r"(LAST.*NAME|LNAME|SURNAME|FAMILY.*NAME)", "last_name"),
    (r"(MIDDLE.*NAME|MNAME|MID.*INIT|MIDDLE.*INIT)", "middle_initial"),
    (r"(COMPANY|CORP.*NAME|ORG.*NAME|BUSINESS.*NAME|EMPLOYER)", "company_name"),
    (r"(FULL.*NAME|CUST.*NAME|EMP.*NAME|CLIENT.*NAME|(?<![A-Z-])NAME)\b", "full_name"),

    # ── Identifiers / Keys ──
    (r"(SSN|SOC.*SEC|SOCIAL.*SEC)", "ssn"),
    (r"(EIN|TAX.*ID|TIN)\b", "ein"),
    (r"(ACCT.*NUM|ACCOUNT.*NO|ACCT.*NO|ACCT.*ID)", "account_number"),
    (r"(CUST.*ID|CUSTOMER.*ID|CLIENT.*ID|CLNT.*ID)\b", "customer_id"),
    (r"(EMP.*ID|EMPLOYEE.*ID|EMPL.*NO)\b", "employee_id"),
    (r"(TRANS.*ID|TXN.*ID|TRANSACTION.*ID)", "transaction_id"),
    (r"(POLICY.*NO|POLICY.*NUM|POL.*ID)", "policy_number"),
    (r"(CLAIM.*NO|CLAIM.*NUM|CLM.*ID)", "claim_number"),
    (r"(ORDER.*NO|ORDER.*NUM|ORD.*ID)", "order_number"),
    (r"(INVOICE.*NO|INV.*NUM)", "invoice_number"),
    (r"(ROUTING.*NUM|ROUT.*NO|ABA)", "routing_number"),

    # ── Address ──
    (r"(ADDR.*LINE.*1|ADDRESS.*1|STREET|ADDR1)", "street_address"),
    (r"(ADDR.*LINE.*2|ADDRESS.*2|APT|SUITE|ADDR2)", "address_line2"),
    (r"(CITY|TOWN)\b", "city"),
    (r"(STATE|(?<![A-Z])ST)\b(?!.*STATUS)", "state_code"),
    (r"(ZIP.*CODE|POSTAL|ZIP)\b", "zip_code"),
    (r"(COUNTRY|CNTRY)\b", "country_code"),

    # ── Contact ──
    (r"(PHONE|TELE|MOBILE|CELL)\b", "phone_number"),
    (r"(EMAIL|E.*MAIL)\b", "email"),
    (r"(FAX)\b", "fax_number"),
    (r"(WEB|URL|WEBSITE)\b", "url"),

    # ── Dates ──
    (r"(DATE.*BIRTH|DOB|BIRTH.*DATE)", "date_of_birth"),
    (r"(EFF.*DATE|EFFECTIVE.*DATE|START.*DATE|BEGIN.*DATE)", "date_past"),
    (r"(EXP.*DATE|EXPIR.*DATE|END.*DATE|TERM.*DATE)", "date_future"),
    (r"(OPEN.*DATE|CREATE.*DATE|ORIG.*DATE)", "date_past"),
    (r"(TRANS.*DATE|TXN.*DATE|POST.*DATE|PROC.*DATE)", "date_recent"),
    (r"(DATE|DT)\b", "date_generic"),

    # ── Time ──
    (r"(TIME|TM|TIMESTAMP)\b", "time"),

    # ── Financial ──
    (r"(BALANCE|BAL)\b", "balance_amount"),
    (r"(AMOUNT|AMT|PAYMENT|PMT)\b", "currency_amount"),
    (r"(RATE|INT.*RATE|APR)\b", "interest_rate"),
    (r"(SALARY|WAGE|PAY.*RATE|COMPENSATION)", "salary"),
    (r"(PRICE|COST|FEE|CHARGE)\b", "price"),
    (r"(CREDIT.*LIMIT|CR.*LIMIT)", "credit_limit"),

    # ── Status / Codes ──
    (r"(STATUS|STAT)\b", "status_code"),
    (r"(TYPE|TYP)\b", "type_code"),
    (r"(CODE|CD)\b", "generic_code"),
    (r"(FLAG|FLG|IND|INDICATOR)\b", "flag"),
    (r"(GENDER|SEX)\b", "gender"),
    (r"(TITLE|PREFIX)\b", "name_prefix"),
    (r"(SUFFIX)\b", "name_suffix"),

    # ── Counts / Quantities ──
    (r"(COUNT|CNT|QTY|QUANTITY|NUM|NBR)\b", "count"),
    (r"(PERCENT|PCT)\b", "percentage"),
    (r"(SCORE|RATING)\b", "score"),

    # ── Description / Text ──
    (r"(DESC|DESCRIPTION|NARR|NARRATIVE|COMMENT|REMARK|NOTE)", "text"),
]

# Compile patterns once
_COMPILED_RULES = [(re.compile(pat, re.IGNORECASE), stype) for pat, stype in INFERENCE_RULES]


def infer_semantic_type(field: CopybookField) -> Optional[str]:
    """
    Infer the semantic type of a copybook field from its name and PIC type.

    Returns:
        A semantic type string (e.g., "first_name", "currency_amount")
        or None if no match is found.
    """
    name = field.name.upper().replace("_", "-")

    for pattern, semantic_type in _COMPILED_RULES:
        if pattern.search(name):
            return semantic_type

    # ── fallback heuristics based on PIC type alone ──
    if field.pic_type in (PicType.ALPHANUMERIC, PicType.ALPHABETIC):
        if field.length <= 2:
            return "generic_code"
        elif field.length <= 30:
            return "alpha_text"
        else:
            return "text"

    if field.pic_type in (PicType.NUMERIC, PicType.SIGNED_NUMERIC,
                          PicType.PACKED_DECIMAL, PicType.BINARY):
        if field.decimal_places > 0:
            return "decimal_number"
        elif field.length <= 2:
            return "small_int"
        else:
            return "integer"

    return None


def infer_all_fields(fields: list[CopybookField]) -> list[CopybookField]:
    """
    Run AI inference on all fields in a copybook, setting semantic_type.

    Args:
        fields: List of parsed CopybookField objects.

    Returns:
        The same list with semantic_type populated.
    """
    for f in fields:
        if not f.is_group:
            f.semantic_type = infer_semantic_type(f)
    return fields
