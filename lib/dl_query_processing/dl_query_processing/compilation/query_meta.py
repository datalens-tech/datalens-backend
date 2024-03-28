from __future__ import annotations

from typing import (
    Any,
    Hashable,
    NamedTuple,
    Optional,
)

import attr
from typing_extensions import Self

from dl_core.constants import DataAPILimits
from dl_query_processing.enums import (
    EmptyQueryMode,
    QueryType,
)


class QueryElementExtract(NamedTuple):
    values: tuple[Optional[Hashable], ...]


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
    def extract(self) -> QueryElementExtract:
        return QueryElementExtract(
            values=(
                self.query_type.name,
                tuple(self.phantom_select_ids),
                tuple(self.field_order) if self.field_order is not None else None,
                self.row_count_hard_limit,
                self.from_subquery,
                self.subquery_limit,
                self.empty_query_mode.name,
            ),
        )

    @property
    def result_field_ids(self) -> list[str]:
        assert self.field_order is not None
        return [field_id for idx, field_id in self.field_order]

    def clone(self, **updates: Any) -> Self:
        return attr.evolve(self, **updates)
