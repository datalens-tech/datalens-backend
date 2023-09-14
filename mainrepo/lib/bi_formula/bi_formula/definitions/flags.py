from __future__ import annotations

from enum import (
    IntEnum,
    unique,
)

ContextFlags = int  # bitwise combination of ContextFlag values


@unique
class ContextFlag(IntEnum):
    # for correct handling of boolean expressions in MS SQL Server
    REQ_CONDITION = 0x0001  # the expression should be translated into a boolean condition
    IS_CONDITION = 0x0002  # the expression is a boolean condition
    DEPRECATED = 0x0004  # function implementation is deprecated
