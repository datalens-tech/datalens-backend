from __future__ import annotations

import abc
from typing import (
    AbstractSet,
    Any,
    ClassVar,
    Generator,
    Iterable,
    Optional,
    Sequence,
)

import attr
from typing_extensions import Self

from dl_constants.enums import (
    JoinType,
    OrderDirection,
)
import dl_formula.core.nodes as formula_nodes
from dl_query_processing.compilation.query_meta import (
    QueryElementExtract,
    QueryMetaInfo,
)
from dl_query_processing.enums import (
    ExecutionLevel,
    QueryPart,
)


@attr.s(slots=True, frozen=True)
class CompiledFormulaInfo:
    show_names: ClassVar[tuple[str, ...]] = (
        "formula_obj",
        "alias",
        "avatar_ids",
        "original_field_id",
    )

    formula_obj: formula_nodes.Formula = attr.ib()
    alias: Optional[str] = attr.ib()
    avatar_ids: set[str] = attr.ib(factory=set)
    original_field_id: Optional[str] = attr.ib(default=None)

    @property
    def extract(self) -> QueryElementExtract:
        return QueryElementExtract(
            values=(
                self.formula_obj.extract,
                self.alias,
                frozenset(self.avatar_ids),
                self.original_field_id,
            ),
        )

    @property
    def not_none_alias(self) -> str:
        assert self.alias is not None
        return self.alias

    @property
    def complexity(self) -> int:
        assert self.formula_obj.complexity is not None
        return self.formula_obj.complexity

    def pretty(
        self,
        indent: str = "  ",
        initial_indent: str = "",
        do_leading_indent: bool = False,
    ) -> str:
        content_item_strs: list[str] = []
        item_indent = f"{initial_indent}{indent}"
        for item_name in self.show_names:
            item = getattr(self, item_name)
            if isinstance(item, formula_nodes.Formula):
                item_str = item.pretty(
                    indent=indent,
                    initial_indent=item_indent,
                    do_leading_indent=False,
                )
            else:
                item_str = str(item)

            content_item_strs.append(f"{item_name}={item_str}")

        content_str = "".join([f"{item_indent}{item_str},\n" for item_str in content_item_strs])
        text = "{lead_idt}{cls}(\n{cont}\n{ini_idt})".format(
            lead_idt=initial_indent if do_leading_indent else "",
            cls=self.__class__.__name__,
            cont=content_str,
            ini_idt=initial_indent,
        )
        return text

    def clone(self, **updates: Any) -> Self:
        return attr.evolve(self, **updates)


@attr.s(slots=True, frozen=True)
class CompiledOrderByFormulaInfo(CompiledFormulaInfo):  # noqa
    show_names = CompiledFormulaInfo.show_names + ("direction",)

    direction: OrderDirection = attr.ib(kw_only=True)

    @property
    def extract(self) -> QueryElementExtract:
        return QueryElementExtract(
            values=(
                self.formula_obj.extract,
                self.alias,
                frozenset(self.avatar_ids),
                self.original_field_id,
                self.direction,
            ),
        )


@attr.s(slots=True, frozen=True)
class CompiledJoinOnFormulaInfo(CompiledFormulaInfo):  # noqa
    show_names = CompiledFormulaInfo.show_names + (
        "left_id",
        "right_id",
        "join_type",
    )

    left_id: str = attr.ib(kw_only=True)  # is root for feature-managed relations
    right_id: str = attr.ib(kw_only=True)
    join_type: JoinType = attr.ib(kw_only=True)

    @property
    def extract(self) -> QueryElementExtract:
        return QueryElementExtract(
            values=(
                self.formula_obj.extract,
                self.alias,
                frozenset(self.avatar_ids),
                self.original_field_id,
                self.left_id,
                self.right_id,
                self.join_type.name,
            ),
        )


@attr.s(frozen=True)
class FromColumn:
    id: str = attr.ib(kw_only=True)
    name: str = attr.ib(kw_only=True)

    def clone(self, **updates: Any) -> FromColumn:
        return attr.evolve(self, **updates)

    @property
    def extract(self) -> QueryElementExtract:
        return QueryElementExtract(
            values=(
                self.id,
                self.name,
            ),
        )


@attr.s(frozen=True)
class FromObject:
    id: str = attr.ib(kw_only=True)
    alias: str = attr.ib(kw_only=True)
    columns: tuple[FromColumn, ...] = attr.ib(kw_only=True)

    @property
    def extract(self) -> QueryElementExtract:
        return QueryElementExtract(
            values=(
                self.id,
                self.alias,
                tuple(col.extract for col in self.columns),
            ),
        )

    def clone(self, **updates: Any) -> Self:
        return attr.evolve(self, **updates)


@attr.s(frozen=True)
class JoinedFromObject:
    root_from_id: Optional[str] = attr.ib(kw_only=True, default=None)
    froms: Sequence[FromObject] = attr.ib(kw_only=True, default=())

    @property
    def extract(self) -> QueryElementExtract:
        return QueryElementExtract(
            values=(
                self.root_from_id,
                tuple(from_obj.extract for from_obj in self.froms),
            ),
        )

    def iter_ids(self) -> Iterable[str]:
        return (from_obj.id for from_obj in self.froms)

    def clone(self, **updates: Any) -> JoinedFromObject:
        return attr.evolve(self, **updates)


# Use double letter as the base query ID to avoid collisions
# with column aliases that use single letters
BASE_QUERY_ID = "qq"


@attr.s(slots=True, frozen=True)
class CompiledQuery:
    show_names = (
        "id",
        "select",
        "group_by",
        "filters",
        "order_by",
        "join_on",
        "limit",
        "offset",
    )

    id: str = attr.ib(kw_only=True)
    level_type: ExecutionLevel = attr.ib(kw_only=True)

    select: list[CompiledFormulaInfo] = attr.ib(kw_only=True, factory=list)
    group_by: list[CompiledFormulaInfo] = attr.ib(kw_only=True, factory=list)
    filters: list[CompiledFormulaInfo] = attr.ib(kw_only=True, factory=list)
    order_by: list[CompiledOrderByFormulaInfo] = attr.ib(kw_only=True, factory=list)
    join_on: list[CompiledJoinOnFormulaInfo] = attr.ib(kw_only=True, factory=list)
    joined_from: JoinedFromObject = attr.ib(kw_only=True, factory=JoinedFromObject)
    limit: Optional[int] = attr.ib(kw_only=True, default=None)
    offset: Optional[int] = attr.ib(kw_only=True, default=None)
    meta: QueryMetaInfo = attr.ib(kw_only=True, factory=QueryMetaInfo)

    @property
    def extract(self) -> QueryElementExtract:
        return QueryElementExtract(
            values=(
                self.id,
                self.level_type.name,
                tuple(formula.extract for formula in self.select),
                tuple(formula.extract for formula in self.group_by),
                tuple(formula.extract for formula in self.filters),
                tuple(formula.extract for formula in self.order_by),
                tuple(formula.extract for formula in self.join_on),
                self.joined_from.extract,
                self.limit,
                self.offset,
                self.meta.extract,
            ),
        )

    def get_complexity(self) -> int:
        return sum(formula.complexity for formula in self.all_formulas)

    def is_empty(self) -> bool:
        return not self.select

    def pretty(
        self,
        indent: str = "  ",
        initial_indent: str = "",
        do_leading_indent: bool = False,
    ) -> str:
        content_item_strs: list[str] = []
        item_indent = f"{initial_indent}{indent}"
        subitem_indent = f"{item_indent}{indent}"
        for item_name in self.show_names:
            item = getattr(self, item_name)
            if isinstance(item, list):
                if len(item) == 0:
                    item_str = "[]"
                else:
                    item_str = "[\n"
                    for subitem in item:
                        assert isinstance(subitem, CompiledFormulaInfo)
                        subitem_str = subitem.pretty(
                            indent=indent,
                            initial_indent=subitem_indent,
                            do_leading_indent=True,
                        )
                        item_str += f"{subitem_str},\n"

                    item_str += f"{item_indent}]"
            else:
                item_str = str(item)

            content_item_strs.append(f"{item_name}={item_str}")

        content_str = "".join([f"{item_indent}{item_str},\n" for item_str in content_item_strs])
        text = "{lead_idt}{cls}(\n{cont}\n{ini_idt})".format(
            lead_idt=initial_indent if do_leading_indent else "",
            cls=self.__class__.__name__,
            cont=content_str,
            ini_idt=initial_indent,
        )
        return text

    @property
    def all_formulas(self) -> Generator[CompiledFormulaInfo, None, None]:
        yield from self.select
        yield from self.group_by
        yield from self.filters
        yield from self.order_by
        yield from self.join_on

    def get_formula_list(self, query_part: QueryPart) -> Sequence[CompiledFormulaInfo]:
        if query_part == QueryPart.select:
            return self.select
        if query_part == QueryPart.group_by:
            return self.group_by
        if query_part == QueryPart.filters:
            return self.filters
        if query_part == QueryPart.order_by:
            return self.order_by
        if query_part == QueryPart.join_on:
            return self.join_on
        raise ValueError(query_part)

    def clone(self, **updates: Any) -> CompiledQuery:
        return attr.evolve(self, **updates)


@attr.s(frozen=True)
class AvatarFromObject(FromObject):
    avatar_id: str = attr.ib(kw_only=True)
    source_id: str = attr.ib(kw_only=True)


@attr.s(frozen=True)
class SubqueryFromObject(FromObject):
    query_id: str = attr.ib(kw_only=True)


@attr.s(frozen=True)
class CompiledMultiQueryBase(abc.ABC):
    @property
    def extract(self) -> QueryElementExtract:
        return QueryElementExtract(
            values=(tuple(query.extract for query in sorted(self.iter_queries(), key=lambda query: query.id)),),
        )

    @abc.abstractmethod
    def iter_queries(self) -> Iterable[CompiledQuery]:
        raise NotImplementedError

    @abc.abstractmethod
    def get_top_queries(self) -> list[CompiledQuery]:
        raise NotImplementedError

    @abc.abstractmethod
    def get_bottom_queries(self) -> list[CompiledQuery]:
        raise NotImplementedError

    def get_single_top_query(self) -> CompiledQuery:
        queries = self.get_top_queries()
        assert len(queries) == 1
        return queries[0]

    @abc.abstractmethod
    def get_query_by_id(self, id: str) -> CompiledQuery:
        raise NotImplementedError

    @abc.abstractmethod
    def for_level_type(self, level_type: ExecutionLevel) -> Self:
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

    def clone(self, **updates: Any) -> Self:
        return attr.evolve(self, **updates)

    def get_complexity(self) -> int:
        return sum(query.get_complexity() for query in self.iter_queries())

    def query_count(self) -> int:
        return sum(1 for _ in self.iter_queries())


@attr.s(frozen=True)
class CompiledMultiQuery(CompiledMultiQueryBase):
    queries: list[CompiledQuery] = attr.ib(kw_only=True)

    # internal caches
    _queries_by_id: dict[str, CompiledQuery] = attr.ib(init=False)
    _unreferenced_ids: list[str] = attr.ib(init=False)

    @_queries_by_id.default
    def _make_queries_by_id(self) -> dict[str, CompiledQuery]:
        return {query.id: query for query in self.queries}

    @_unreferenced_ids.default
    def _make_unreferenced_ids(self) -> list[str]:
        referenced_ids = {from_obj.id for query in self.queries for from_obj in query.joined_from.froms}
        query_ids = {query.id for query in self.queries}
        return sorted(query_ids - referenced_ids)

    def iter_queries(self) -> Iterable[CompiledQuery]:
        return self.queries

    def get_top_queries(self) -> list[CompiledQuery]:
        return [self._queries_by_id[query_id] for query_id in self._unreferenced_ids]

    def get_bottom_queries(self) -> list[CompiledQuery]:
        raise NotImplementedError

    def get_query_by_id(self, id: str) -> CompiledQuery:
        return self._queries_by_id[id]

    def for_level_type(self, level_type: ExecutionLevel) -> CompiledMultiQuery:
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
