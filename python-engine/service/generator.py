import json
import random
import re
from datetime import datetime, timezone
from typing import Any

from snowflake import next_id


MAX_RECORDS_PER_REQUEST = 10_000
NAMES = ("Alex", "Morgan", "Taylor", "Jordan", "Casey", "Riley")
PHONE_PREFIXES = ("130", "131", "132", "155", "156", "186", "188")
ENUM_RULE = re.compile(r"^enum\(([^()]*)\)$")


def _value(field: dict[str, Any], context: dict[str, Any]) -> Any:
    field_type = field.get("type", "string")
    rule = field.get("rule", "").strip()
    if not rule:
        if field_type == "snowflake":
            return next_id()
        if field_type == "int":
            return 0
        if field_type == "datetime":
            return datetime.now(timezone.utc).isoformat()
        return ""
    if rule == "name()":
        return random.choice(NAMES)
    if rule == "phone()":
        return random.choice(PHONE_PREFIXES) + "".join(str(random.randrange(10)) for _ in range(8))
    if rule == "now()":
        return datetime.now(timezone.utc).isoformat()
    if rule == "snowflake()":
        return next_id()
    match = ENUM_RULE.fullmatch(rule)
    if match:
        options = [option.strip() for option in match.group(1).split(",") if option.strip()]
        if not options:
            raise ValueError(f"enum rule for field {field.get('name', '')!r} has no values")
        value = random.choice(options)
        if field_type == "int":
            try:
                return int(value)
            except ValueError as error:
                raise ValueError(f"enum value {value!r} is not an integer") from error
        return value
    if rule.startswith("context."):
        key = rule.removeprefix("context.")
        if not key or key not in context:
            raise ValueError(f"context value {key!r} is not provided")
        return context[key]
    raise ValueError(f"unsupported rule {rule!r} for field {field.get('name', '')!r}")


def validate_template(template: dict[str, Any]) -> list[str]:
    fields = template.get("fields")
    if not isinstance(fields, list) or not fields:
        return ["template fields must be a non-empty array"]

    errors = []
    field_names: set[str] = set()
    for field in fields:
        if not isinstance(field, dict) or not isinstance(field.get("name"), str) or not field["name"]:
            errors.append("each template field must have a non-empty name")
            continue
        if not isinstance(field.get("type", "string"), str) or not isinstance(field.get("rule", ""), str):
            errors.append(f"field {field['name']!r} type and rule must be strings")
            continue
        if not isinstance(field.get("nullable", False), bool):
            errors.append(f"field {field['name']!r} nullable must be a boolean")
            continue
        if field["name"] in field_names:
            errors.append(f"duplicate field name {field['name']!r}")
            continue
        field_names.add(field["name"])

        rule = field.get("rule", "").strip()
        if not rule or rule in {"name()", "phone()", "now()", "snowflake()"}:
            continue
        match = ENUM_RULE.fullmatch(rule)
        if match:
            options = [option.strip() for option in match.group(1).split(",") if option.strip()]
            if not options:
                errors.append(f"enum rule for field {field['name']!r} has no values")
            elif field.get("type", "string") == "int":
                for option in options:
                    try:
                        int(option)
                    except ValueError:
                        errors.append(f"enum value {option!r} for field {field['name']!r} is not an integer")
            continue
        if rule.startswith("context.") and rule.removeprefix("context.").strip():
            continue
        errors.append(f"unsupported rule {rule!r} for field {field['name']!r}")
    return errors


def generate(template: dict[str, Any], context: dict[str, Any], count: int) -> list[dict[str, Any]]:
    if count < 0 or count > MAX_RECORDS_PER_REQUEST:
        raise ValueError(f"count must be between 0 and {MAX_RECORDS_PER_REQUEST}")
    errors = validate_template(template)
    if errors:
        raise ValueError("; ".join(errors))

    fields = template["fields"]
    rows = []
    for _ in range(count):
        row = {}
        for field in fields:
            value = _value(field, context)
            if value is None and not field.get("nullable", False):
                raise ValueError(f"field {field['name']!r} cannot be null")
            row[field["name"]] = value
        rows.append(row)
    return rows


def encode_rows(rows: list[dict[str, Any]]) -> str:
    return json.dumps(rows, ensure_ascii=False, separators=(",", ":"))
