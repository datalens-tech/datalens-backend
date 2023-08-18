from __future__ import annotations

from typing import AbstractSet, Any, Generator, Iterable, List, NamedTuple, Tuple

import attr

from bi_core.components.ids import AvatarId

from bi_query_processing.enums import ExecutionLevel
from bi_query_processing.compilation.primitives import CompiledQuery, CompiledMultiQueryBase, FromObject


class MultiQueryIndex(NamedTuple):
    level_idx: int
    query_idx: int


@attr.s(slots=True, frozen=True)
class CompiledLevel:
    level_type: ExecutionLevel = attr.ib(kw_only=True)
    queries: List[CompiledQuery] = attr.ib(factory=list)

    def get_complexity(self) -> int:
        return sum(query.get_complexity() for query in self.queries)

    def query_count(self) -> int:
        return len(self.queries)

    def clone(self, **updates: Any) -> CompiledLevel:
        return attr.evolve(self, **updates)

    def get_from_objects(self) -> dict[str, FromObject]:
        result: dict[str, FromObject] = {}
        for query in self.queries:
            for from_obj in query.joined_from.froms:
                result[from_obj.id] = from_obj
        return result


@attr.s(slots=True, frozen=True)
class CompiledMultiLevelQuery(CompiledMultiQueryBase):
    """
    Rearranged slice query:
    - from <query_part, level>
    - to <level, query, query_part>
    """

    levels: List[CompiledLevel] = attr.ib(kw_only=True)

    def get_complexity(self) -> int:
        return sum(level.get_complexity() for level in self.levels)

    def level_count(self) -> int:
        return len(self.levels)

    def query_count(self) -> int:
        return sum(level.query_count() for level in self.levels)

    @property
    def top_level(self) -> CompiledLevel:
        return self.levels[-1]

    def __getitem__(self, item: MultiQueryIndex) -> CompiledQuery:
        return self.levels[item.level_idx].queries[item.query_idx]

    def get_query_index_by_id(self, id: AvatarId) -> MultiQueryIndex:
        for level_idx, compiled_level in enumerate(self.levels):
            for query_idx, compiled_flat_query in enumerate(compiled_level.queries):
                if compiled_flat_query.id == id:
                    return MultiQueryIndex(level_idx=level_idx, query_idx=query_idx)

        raise KeyError(f'No query with ID {id}')

    def clone(self, **updates: Any) -> CompiledMultiLevelQuery:
        return attr.evolve(self, **updates)

    def enumerate(self) -> Generator[Tuple[MultiQueryIndex, CompiledQuery], None, None]:
        for level_idx, compiled_level in enumerate(self.levels):
            for query_idx, compiled_flat_query in enumerate(compiled_level.queries):
                yield MultiQueryIndex(level_idx=level_idx, query_idx=query_idx), compiled_flat_query

    def iter_queries(self) -> Iterable[CompiledQuery]:
        for level in self.levels:
            yield from level.queries

    def get_top_queries(self) -> list[CompiledQuery]:
        return self.top_level.queries

    def get_bottom_queries(self) -> list[CompiledQuery]:
        return self.levels[0].queries

    def get_query_by_id(self, id: str) -> CompiledQuery:
        query_idx = self.get_query_index_by_id(id=id)
        return self[query_idx]

    def for_level_type(self: CompiledMultiLevelQuery, level_type: ExecutionLevel) -> CompiledMultiLevelQuery:
        levels = [level for level in self.levels if level.level_type == level_type]
        return self.clone(levels=levels)

    def get_base_froms(self) -> dict[str, FromObject]:
        if not self.levels:
            return {}

        result: dict[str, FromObject] = {}
        for query in self.levels[0].queries:
            for from_obj in query.joined_from.froms:
                if from_obj.id not in result:
                    result[from_obj.id] = from_obj
        return result

    def is_empty(self) -> bool:
        return self.query_count() != 0

    def get_base_root_from_ids(self) -> AbstractSet[str]:
        if not self.levels or not self.levels[0].queries:
            return frozenset()
        return {
            query.joined_from.root_from_id for query in self.levels[0].queries
            if query.joined_from.root_from_id is not None
        }
