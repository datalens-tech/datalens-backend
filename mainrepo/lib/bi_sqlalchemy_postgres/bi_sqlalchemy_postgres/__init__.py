from __future__ import annotations

from .base import BIPGDialect
from .asyncpg import AsyncBIPGDialect

__all__ = (
    'BIPGDialect',
    'AsyncBIPGDialect',
)
