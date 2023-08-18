from __future__ import annotations

from .version import __version__  # noqa
from . import base


base.dialect = dialect = base.MetrikaApiDialect


__all__ = (
    "dialect",
)
