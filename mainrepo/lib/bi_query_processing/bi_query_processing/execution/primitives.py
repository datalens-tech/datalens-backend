from __future__ import annotations

from typing import (
    Any,
    List,
    Optional,
    Type,
    TypeVar,
)

import attr

from bi_constants.types import TBIDataRow
from bi_core.utils import attrs_evolve_to_subclass
from bi_query_processing.translation.primitives import TranslatedQueryMetaInfo

_META_TV = TypeVar("_META_TV", bound="ExecutedQueryMetaInfo")


@attr.s
class ExecutedQueryMetaInfo(TranslatedQueryMetaInfo):
    debug_query: Optional[str] = attr.ib(kw_only=True, default=None)
    target_connection_ids: set[str] = attr.ib(kw_only=True, factory=set)

    @classmethod
    def from_trans_meta(
        cls: Type[_META_TV],
        trans_meta: TranslatedQueryMetaInfo,
        debug_query: Optional[str] = None,
        target_connection_ids: Optional[set[str]] = None,
    ) -> _META_TV:
        return attrs_evolve_to_subclass(
            cls=cls,
            inst=trans_meta,
            debug_query=debug_query,
            target_connection_ids=target_connection_ids or set(),
        )


@attr.s
class ExecutedQuery:
    rows: List[TBIDataRow] = attr.ib(kw_only=True)
    meta: ExecutedQueryMetaInfo = attr.ib(kw_only=True, factory=ExecutedQueryMetaInfo)

    def clone(self, **updates: Any) -> ExecutedQuery:
        return attr.evolve(self, **updates)
