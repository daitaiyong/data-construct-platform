"""Small, deliberately constrained template parser for the starter service."""

import string
from typing import Mapping


def render(template: str, variables: Mapping[str, str]) -> str:
    formatter = string.Formatter()
    parts = list(formatter.parse(template))
    for _, name, format_spec, conversion in parts:
        if name is not None and (not name.isidentifier() or format_spec or conversion):
            raise ValueError("only simple variable placeholders are supported")
    fields = {name for _, name, _, _ in parts if name}
    missing = fields.difference(variables)
    if missing:
        raise ValueError(f"missing template variables: {', '.join(sorted(missing))}")

    rendered = []
    for literal, name, format_spec, conversion in parts:
        rendered.append(literal)
        if name is None:
            continue
        rendered.append(variables[name])
    return "".join(rendered)
