from __future__ import annotations

from .base import (
    BIMSSQLDialect,
)


__all__ = (
    "BIMSSQLDialect",
)


__version__ = "0.3"
VERSION = tuple(int(part) for part in __version__.split("."))
