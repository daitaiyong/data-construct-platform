"""Expression runtime boundary.

Expressions are intentionally not evaluated with Python eval(). Add a
whitelisted expression grammar here before enabling user-authored expressions.
"""


def evaluate(expression: str, context: dict[str, object]) -> object:
    del expression, context
    raise NotImplementedError("expression evaluation is not configured")
