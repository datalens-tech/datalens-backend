"""
Compatibility imports.

It is recommended to import the necessary modules directly,
i.e. `from dl_api_lib.utils.something import ...` instead of `from dl_api_lib.utils import ...`.
"""

from __future__ import annotations

from .base import (  # ...
    chunks,
    need_permission,
    need_permission_on_entry,
    profile_stats,
    query_execution_context,
)

__all__ = (
    "query_execution_context",
    "profile_stats",
    "need_permission",
    "need_permission_on_entry",
    "chunks",
)
