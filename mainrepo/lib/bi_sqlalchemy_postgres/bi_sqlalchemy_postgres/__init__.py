from __future__ import annotations

from .base import BIPGDialect
from .asyncpg import AsyncBIPGDialect


__all__ = (
    'BIPGDialect',
    'AsyncBIPGDialect',
)


__version__ = '0.1'
VERSION = tuple(int(part) for part in __version__.split('.'))
