"""
Compatibility imports.

It is recommended to import the necessary modules directly,
i.e. `from bi_api_lib.utils.something import ...` instead of `from bi_api_lib.utils import ...`.
"""

from __future__ import annotations

from .base import (
    # ...
    query_execution_context, profile_stats,
    need_permission, need_permission_on_entry,
    chunks,
)


__all__ = (
    'query_execution_context', 'profile_stats',
    'need_permission', 'need_permission_on_entry',
    'chunks',
)
