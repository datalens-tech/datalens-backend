from __future__ import annotations

from typing import (
    TYPE_CHECKING,
    List,
)

import attr

if TYPE_CHECKING:
    from bi_constants.enums import DataSourceRole
    from bi_core.us_connection_base import ClassicConnectionSQL
    from bi_query_processing.translation.primitives import TranslatedMultiQueryBase


@attr.s
class QueryExecutionInfo:
    role: DataSourceRole = attr.ib(kw_only=True)
    translated_multi_query: TranslatedMultiQueryBase = attr.ib(kw_only=True)
    target_connections: List[ClassicConnectionSQL] = attr.ib(kw_only=True)
