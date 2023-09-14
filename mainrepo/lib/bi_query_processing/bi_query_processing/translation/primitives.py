from __future__ import annotations

import abc
from typing import (
    AbstractSet,
    Any,
    Iterable,
    List,
    NamedTuple,
    Optional,
    Sequence,
    Type,
    TypeVar,
)

import attr

from bi_constants.enums import BIType
from bi_core.components.ids import (
    AvatarId,
    FieldId,
)
from bi_core.db.elements import SchemaColumn
from bi_core.query.expression import (
    ExpressionCtx,
    JoinOnExpressionCtx,
    OrderByExpressionCtx,
)
from bi_core.utils import attrs_evolve_to_subclass
from bi_formula.core.datatype import (
    DataType,
    DataTypeParams,
)
from bi_query_processing.compilation.primitives import FromObject
from bi_query_processing.compilation.query_meta import QueryMetaInfo
from bi_query_processing.enums import ExecutionLevel


class DetailedType(NamedTuple):
    field_id: str
    data_type: BIType
    # TODO: native_type: Optional[GenericNativeType] = None
    formula_data_type: Optional[DataType] = None
    formula_data_type_params: Optional[DataTypeParams] = None


_META_TV = TypeVar("_META_TV", bound="TranslatedQueryMetaInfo")


@attr.s
class TranslatedQueryMetaInfo(QueryMetaInfo):
    detailed_types: Optional[list[Optional[DetailedType]]] = attr.ib(kw_only=True, factory=list)  # type: ignore

    @classmethod
    def from_comp_meta(
        cls: Type[_META_TV],
        comp_meta: QueryMetaInfo,
        detailed_types: Optional[list[Optional[DetailedType]]] = None,
    ) -> _META_TV:
        detailed_types = detailed_types or []
        assert detailed_types is not None
        return attrs_evolve_to_subclass(
            cls=cls,
            inst=comp_meta,
            detailed_types=detailed_types,
        )


@attr.s(auto_attribs=True, frozen=True)
class ExpressionCtxExt(ExpressionCtx):
    """
    A variant of ExpressionCtx that knows about bi_formula.
    Should be used for the SELECT part of the query,
    as it contains info for postprocessing the data,
    which is not needed in other clauses.
    """

    formula_data_type: Optional[DataType] = None
    formula_data_type_params: Optional[DataTypeParams] = None
    original_field_id: Optional[FieldId] = None


@attr.s(frozen=True)
class TranslatedJoinedFromObject:
    root_from_id: Optional[str] = attr.ib(kw_only=True, default=None)
    froms: Sequence[FromObject] = attr.ib(kw_only=True, default=())  # FIXME: switch to translated froms

    def iter_ids(self) -> Iterable[str]:
        return (from_obj.id for from_obj in self.froms)


@attr.s(frozen=True)
class TranslatedFlatQuery:
    id: AvatarId = attr.ib(kw_only=True)
    alias: str = attr.ib(kw_only=True)
    level_type: ExecutionLevel = attr.ib(kw_only=True)

    select: List[ExpressionCtxExt] = attr.ib(kw_only=True)
    where: List[ExpressionCtx] = attr.ib(kw_only=True)
    group_by: List[ExpressionCtx] = attr.ib(kw_only=True)
    having: List[ExpressionCtx] = attr.ib(kw_only=True)
    order_by: List[OrderByExpressionCtx] = attr.ib(kw_only=True)
    join_on: List[JoinOnExpressionCtx] = attr.ib(kw_only=True)
    joined_from: TranslatedJoinedFromObject = attr.ib(kw_only=True)
    limit: Optional[int] = attr.ib(kw_only=True)
    offset: Optional[int] = attr.ib(kw_only=True)
    column_list: list[SchemaColumn] = attr.ib(kw_only=True)
    meta: TranslatedQueryMetaInfo = attr.ib(kw_only=True, factory=TranslatedQueryMetaInfo)

    def is_empty(self) -> bool:
        return not self.select


_TRANS_MULTI_QUERY_TV = TypeVar("_TRANS_MULTI_QUERY_TV", bound="TranslatedMultiQueryBase")


@attr.s(frozen=True)
class TranslatedMultiQueryBase(abc.ABC):
    @abc.abstractmethod
    def iter_queries(self) -> Iterable[TranslatedFlatQuery]:
        raise NotImplementedError

    @abc.abstractmethod
    def get_top_queries(self) -> list[TranslatedFlatQuery]:
        raise NotImplementedError

    def get_single_top_query(self) -> TranslatedFlatQuery:
        queries = self.get_top_queries()
        assert len(queries) == 1
        return queries[0]

    @abc.abstractmethod
    def get_query_by_id(self, id: str) -> TranslatedFlatQuery:
        raise NotImplementedError

    @abc.abstractmethod
    def for_level_type(self: _TRANS_MULTI_QUERY_TV, level_type: ExecutionLevel) -> _TRANS_MULTI_QUERY_TV:
        raise NotImplementedError

    @abc.abstractmethod
    def get_base_froms(self) -> dict[str, FromObject]:
        raise NotImplementedError

    @abc.abstractmethod
    def is_empty(self) -> bool:
        raise NotImplementedError

    @abc.abstractmethod
    def get_base_root_from_ids(self) -> AbstractSet[str]:
        raise NotImplementedError

    def clone(self: _TRANS_MULTI_QUERY_TV, **updates: Any) -> _TRANS_MULTI_QUERY_TV:
        return attr.evolve(self, **updates)


@attr.s(frozen=True)
class TranslatedLevel:
    level_type: ExecutionLevel = attr.ib(kw_only=True)
    queries: List[TranslatedFlatQuery] = attr.ib(factory=list)


@attr.s(frozen=True)
class TranslatedMultiLevelQuery(TranslatedMultiQueryBase):  # TODO: Remove this implementation
    levels: List[TranslatedLevel] = attr.ib(kw_only=True)

    def level_count(self) -> int:
        return len(self.levels)

    @property
    def top_level(self) -> TranslatedLevel:
        return self.levels[-1]

    def get_required_avatar_ids(self) -> set[str]:
        if not self.levels:
            return set()
        return {from_id for query in self.levels[0].queries for from_id in query.joined_from.iter_ids()}

    def iter_queries(self) -> Iterable[TranslatedFlatQuery]:
        for level in self.levels:
            for query in level.queries:
                yield query

    def get_top_queries(self) -> list[TranslatedFlatQuery]:
        return self.top_level.queries

    def get_query_by_id(self, id: str) -> TranslatedFlatQuery:
        for level in self.levels:
            for query in level.queries:
                if query.id == id:
                    return query
        raise KeyError(id)

    def for_level_type(self, level_type: ExecutionLevel) -> TranslatedMultiLevelQuery:
        levels = [level for level in self.levels if level.level_type == level_type]
        return self.clone(levels=levels)

    def is_empty(self) -> bool:
        return not self.levels or not self.levels[0].queries

    def get_base_root_from_ids(self) -> AbstractSet[str]:
        if not self.levels or not self.levels[0].queries:
            return frozenset()
        return {
            query.joined_from.root_from_id
            for query in self.levels[0].queries
            if query.joined_from.root_from_id is not None
        }

    def get_base_froms(self) -> dict[str, FromObject]:
        if not self.levels:
            return {}

        result: dict[str, FromObject] = {}
        for query in self.levels[0].queries:
            for from_obj in query.joined_from.froms:
                if from_obj.id not in result:
                    result[from_obj.id] = from_obj
        return result


@attr.s(frozen=True)
class TranslatedMultiQuery(TranslatedMultiQueryBase):
    queries: list[TranslatedFlatQuery] = attr.ib(kw_only=True)

    # internal caches
    _queries_by_id: dict[str, TranslatedFlatQuery] = attr.ib(init=False)
    _unreferenced_ids: list[str] = attr.ib(init=False)

    @_queries_by_id.default
    def _make_queries_by_id(self) -> dict[str, TranslatedFlatQuery]:
        return {query.id: query for query in self.queries}

    @_unreferenced_ids.default
    def _make_unreferenced_ids(self) -> list[str]:
        referenced_ids = {from_obj.id for query in self.queries for from_obj in query.joined_from.froms}
        query_ids = {query.id for query in self.queries}
        return sorted(query_ids - referenced_ids)

    def iter_queries(self) -> Iterable[TranslatedFlatQuery]:
        return self.queries

    def get_top_queries(self) -> list[TranslatedFlatQuery]:
        return [self._queries_by_id[query_id] for query_id in self._unreferenced_ids]

    def get_query_by_id(self, id: str) -> TranslatedFlatQuery:
        return self._queries_by_id[id]

    def for_level_type(self, level_type: ExecutionLevel) -> TranslatedMultiQuery:
        queries = [query for query in self.queries if query.level_type == level_type]
        return self.clone(queries=queries)

    def get_base_froms(self) -> dict[str, FromObject]:
        result: dict[str, FromObject] = {}
        for query in self.queries:
            for from_obj in query.joined_from.froms:
                if from_obj.id not in self._queries_by_id and from_obj.id not in result:
                    result[from_obj.id] = from_obj
        return result

    def is_empty(self) -> bool:
        return not self.queries

    def get_base_root_from_ids(self) -> AbstractSet[str]:
        result: set[str] = set()
        for query in self.queries:
            root_from_id = query.joined_from.root_from_id
            if root_from_id is not None and root_from_id not in self._queries_by_id:
                result.add(root_from_id)
        return result
