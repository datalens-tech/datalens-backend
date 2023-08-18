from __future__ import annotations

from .base import GSheetsDialect, dialect, register_dialect
from .version import __version__


__all__ = (
    "GSheetsDialect",
    "dialect",
    "register_dialect",
    "__version__",
)
