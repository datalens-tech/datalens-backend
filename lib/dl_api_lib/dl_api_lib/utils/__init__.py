"""
Compatibility imports.

It is recommended to import the necessary modules directly,
i.e. `from dl_api_lib.utils.something import ...` instead of `from dl_api_lib.utils import ...`.
"""

from __future__ import annotations

from .base import (  # ...
    check_permission_on_entry,
    enrich_resp_dict_with_data_export_info,
    get_data_export_base_result,
    need_permission_on_entry,
    profile_stats,
    query_execution_context,
)


__all__ = (
    "check_permission_on_entry",
    "enrich_resp_dict_with_data_export_info",
    "get_data_export_base_result",
    "need_permission_on_entry",
    "profile_stats",
    "query_execution_context",
)
