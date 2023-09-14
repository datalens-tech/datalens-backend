from __future__ import annotations

from typing import (
    Any,
    Optional,
    TypeVar,
)

import attr

from bi_core.constants import DataAPILimits
from bi_query_processing.enums import (
    EmptyQueryMode,
    QueryType,
)

_QUERY_META_TV = TypeVar("_QUERY_META_TV", bound="QueryMetaInfo")


@attr.s
class QueryMetaInfo:
    query_type: QueryType = attr.ib(kw_only=True, default=QueryType.result)
    phantom_select_ids: list[str] = attr.ib(kw_only=True, factory=list)
    field_order: Optional[list[tuple[int, str]]] = attr.ib(kw_only=True, default=None)
    row_count_hard_limit: int = attr.ib(kw_only=True, default=DataAPILimits.DEFAULT_SOURCE_DB_LIMIT)
    from_subquery: bool = attr.ib(kw_only=True, default=False)
    subquery_limit: int = attr.ib(kw_only=True, default=None)
    empty_query_mode: EmptyQueryMode = attr.ib(kw_only=True, default=EmptyQueryMode.error)

    @property
    def result_field_ids(self) -> list[str]:
        assert self.field_order is not None
        return [field_id for idx, field_id in self.field_order]

    def clone(self: _QUERY_META_TV, **updates: Any) -> _QUERY_META_TV:
        return attr.evolve(self, **updates)
