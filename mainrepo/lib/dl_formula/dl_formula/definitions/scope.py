from enum import (
    IntEnum,
    unique,
)


_BIT = 1


def _auto_bit() -> int:
    global _BIT
    old = _BIT
    _BIT <<= 1
    return old


@unique
class Scope(IntEnum):
    STABLE = _auto_bit()
    UNSTABLE = _auto_bit()

    # Functionality-bound scopes
    EXPLICIT_USAGE = _auto_bit()  # Can be used in formulas explicitly
    SUGGESTED = _auto_bit()  # Function is listed in suggestions
    DOCUMENTED = _auto_bit()  # Documentation is generated for function
