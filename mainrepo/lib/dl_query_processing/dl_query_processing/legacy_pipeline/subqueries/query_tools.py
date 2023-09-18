from __future__ import annotations

from itertools import count as it_count
from typing import (
    AbstractSet,
    Dict,
    Iterable,
    Iterator,
    List,
    Optional,
    Sequence,
    Tuple,
    TypeVar,
)

import attr

from dl_core.components.ids import AvatarId
import dl_formula.core.nodes as formula_nodes
from dl_formula.inspect.expression import used_fields
from dl_query_processing.compilation.primitives import (
    CompiledFormulaInfo,
    CompiledJoinOnFormulaInfo,
    CompiledQuery,
    FromColumn,
    FromObject,
    JoinedFromObject,
    SubqueryFromObject,
)
from dl_query_processing.legacy_pipeline.separation.primitives import (
    CompiledLevel,
    CompiledMultiLevelQuery,
    MultiQueryIndex,
)
from dl_query_processing.multi_query.tools import remap_formula_obj_fields


def get_formula_list_used_avatar_ids(formulas: Iterable[CompiledFormulaInfo]) -> AbstractSet[AvatarId]:
    return {avatar_id for formula in formulas for avatar_id in formula.avatar_ids}


def get_query_level_used_fields(compiled_level: CompiledLevel) -> AbstractSet[str]:
    return {
        field.name
        for compiled_query in compiled_level.queries
        for formula in compiled_query.all_formulas
        for field in used_fields(formula.formula_obj)
    }


@attr.s
class AliasRemapper:
    base: str = attr.ib()
    mapping: Dict[str, str] = attr.ib(init=False, factory=dict)
    _alias_cnt: Iterator[int] = attr.ib(init=False, factory=it_count)

    def remap(self, old_alias: Optional[str]) -> str:
        assert old_alias is not None
        if old_alias not in self.mapping:
            self.mapping[old_alias] = f"{self.base}_{next(self._alias_cnt)}"
        return self.mapping[old_alias]

    def __call__(self, old_alias: Optional[str]) -> str:
        return self.remap(old_alias)


_COMPILED_FORMULA_TV = TypeVar("_COMPILED_FORMULA_TV", bound=CompiledFormulaInfo)


def remap_compiled_formula_fields(
    compiled_formula: _COMPILED_FORMULA_TV,
    alias: Optional[str],
    field_name_map: Dict[str, str],
    avatar_map: Dict[AvatarId, AvatarId],
) -> _COMPILED_FORMULA_TV:
    formula_obj = compiled_formula.formula_obj
    if field_name_map:
        formula_obj = remap_formula_obj_fields(formula_obj, field_name_map)
    avatar_ids = {avatar_map.get(av_id, av_id) for av_id in compiled_formula.avatar_ids}

    update_kwargs = dict(formula_obj=formula_obj, alias=alias, avatar_ids=avatar_ids)
    if isinstance(compiled_formula, CompiledJoinOnFormulaInfo):
        update_kwargs["left_id"] = avatar_map.get(compiled_formula.left_id, compiled_formula.left_id)
        update_kwargs["right_id"] = avatar_map.get(compiled_formula.right_id, compiled_formula.right_id)

    compiled_formula = compiled_formula.clone(**update_kwargs)

    return compiled_formula


def remap_compiled_formula_list(
    compiled_formula_list: List[_COMPILED_FORMULA_TV],
    alias_remapper: AliasRemapper,
    field_name_map: Dict[str, str],
    avatar_map: Dict[AvatarId, AvatarId],
) -> List[_COMPILED_FORMULA_TV]:
    return [
        remap_compiled_formula_fields(
            formula,
            alias=(
                alias_remapper(formula.alias)
                if not isinstance(formula, CompiledJoinOnFormulaInfo)
                else None  # because join_on formulas are the only ones that don't have aliases
            ),
            field_name_map=field_name_map,
            avatar_map=avatar_map,
        )
        for formula in compiled_formula_list
    ]


def copy_and_remap_query(
    compiled_query: CompiledQuery,
    id: AvatarId,
    field_name_map: Dict[str, str],
    avatar_map: Dict[AvatarId, AvatarId],
) -> Tuple[CompiledQuery, Dict[str, str]]:
    alias_remapper = AliasRemapper(base=id)
    select = remap_compiled_formula_list(
        compiled_query.select,
        alias_remapper=alias_remapper,
        field_name_map=field_name_map,
        avatar_map=avatar_map,
    )
    group_by = remap_compiled_formula_list(
        compiled_query.group_by,
        alias_remapper=alias_remapper,
        field_name_map=field_name_map,
        avatar_map=avatar_map,
    )
    order_by = remap_compiled_formula_list(
        compiled_query.order_by,
        alias_remapper=alias_remapper,
        field_name_map=field_name_map,
        avatar_map=avatar_map,
    )
    filters = remap_compiled_formula_list(
        compiled_query.filters,
        alias_remapper=alias_remapper,
        field_name_map=field_name_map,
        avatar_map=avatar_map,
    )
    join_on = remap_compiled_formula_list(
        compiled_query.join_on,
        alias_remapper=alias_remapper,
        field_name_map=field_name_map,
        avatar_map=avatar_map,
    )
    used_avatar_ids = {avatar_map.get(from_id, from_id) for from_id in compiled_query.joined_from.iter_ids()}
    original_root_from_id = compiled_query.joined_from.root_from_id
    new_root_from_id: Optional[str] = None
    if original_root_from_id is not None:
        new_root_from_id = avatar_map.get(original_root_from_id, original_root_from_id)

    reverse_avatar_map = {new_from_id: old_from_id for old_from_id, new_from_id in avatar_map.items()}

    original_from_objects = {from_obj.id: from_obj for from_obj in compiled_query.joined_from.froms}
    froms: list[FromObject] = []
    for new_from_id in sorted(used_avatar_ids):
        old_from_id = reverse_avatar_map.get(new_from_id, new_from_id)
        old_from_obj = original_from_objects[old_from_id]
        new_from_obj: FromObject
        if new_from_id == old_from_id:
            new_from_obj = old_from_obj
        else:
            assert isinstance(old_from_obj, SubqueryFromObject), "Avatar froms should not be re-mapped"

            remapped_columns: list[FromColumn] = []
            for col in old_from_obj.columns:
                assert col.id == col.name
                new_col_id = field_name_map[col.id]
                remapped_columns.append(FromColumn(id=new_col_id, name=new_col_id))

            new_from_obj = old_from_obj.clone(
                id=new_from_id,
                alias=new_from_id,
                query_id=new_from_id,
                columns=tuple(remapped_columns),
            )

        assert new_from_obj is not None
        froms.append(new_from_obj)

    joined_from = JoinedFromObject(root_from_id=new_root_from_id, froms=froms)
    query_copy = compiled_query.clone(
        id=id,
        select=select,
        group_by=group_by,
        order_by=order_by,
        filters=filters,
        join_on=join_on,
        joined_from=joined_from,
    )
    return query_copy, alias_remapper.mapping


def get_query_dimension_list(compiled_query: CompiledQuery) -> List[CompiledFormulaInfo]:
    if compiled_query.group_by:
        # GROUP BY is explicitly specified, so just return the list
        return compiled_query.group_by

    # No explicit GROUP BY.
    # So, fall back to the SELECT expressions
    return compiled_query.select


def get_query_dimension_aliases(compiled_query: CompiledQuery) -> List[str]:
    dimensions = get_query_dimension_list(compiled_query=compiled_query)
    result: List[str] = []
    for formula in dimensions:
        alias = formula.alias
        assert alias is not None
        result.append(alias)

    return result


@attr.s
class CompiledMultiLevelQueryIncrementalPatch:
    level_patches: List[List[CompiledQuery]] = attr.ib(kw_only=True, factory=list)

    def add_query_for_level(self, level_idx: int, compiled_query: CompiledQuery) -> None:
        self.level_patches[level_idx].append(compiled_query)

    @property
    def top_level_query(self) -> CompiledQuery:
        assert len(self.level_patches[-1]) == 1, "There must be exactly 1 top-level query in the patch"
        return self.level_patches[-1][0]

    @property
    def level_count(self) -> int:
        return len(self.level_patches)

    @property
    def empty(self) -> bool:
        return not any(bool(level) for level in self.level_patches)

    @classmethod
    def generate(cls, level_cnt: int) -> CompiledMultiLevelQueryIncrementalPatch:
        return cls(level_patches=[[] for _ in range(level_cnt)])


def apply_multi_query_incremental_patches(
    compiled_multi_query: CompiledMultiLevelQuery,
    patches: Sequence[CompiledMultiLevelQueryIncrementalPatch],
) -> CompiledMultiLevelQuery:
    """
    Apply incremental patches, i.e. add new sub-queries to multi-query.
    """

    if all(patch.empty for patch in patches):
        return compiled_multi_query

    assert len({patch.level_count for patch in patches}) == 1, "All patches must have the same number of levels"
    patch_level_count = patches[0].level_count

    bottom_levels = compiled_multi_query.levels[:patch_level_count]
    unchanged_top_levels = compiled_multi_query.levels[patch_level_count:]

    # For each level add all queries for every patch to the original level
    for level_idx, original_level in enumerate(bottom_levels):
        combined_level_patch = [query for patch in patches for query in patch.level_patches[level_idx]]
        ids_from_patch = {q.id for q in combined_level_patch}
        ids_from_original = {q.id for q in original_level.queries}
        if ids_from_patch & ids_from_original:
            raise RuntimeError("Patch contains queries with ids already used in multi-query")
        bottom_levels[level_idx] = original_level.clone(queries=original_level.queries + combined_level_patch)

    patched_multi_query = compiled_multi_query.clone(levels=bottom_levels + unchanged_top_levels)
    return patched_multi_query


@attr.s
class CompiledMultiLevelQueryReplacementPatch:
    subquery_idx: MultiQueryIndex = attr.ib(kw_only=True)
    new_subquery: CompiledQuery = attr.ib(kw_only=True)


def apply_multi_query_replacement_patches(
    compiled_multi_query: CompiledMultiLevelQuery,
    patches: Sequence[CompiledMultiLevelQueryReplacementPatch],
) -> CompiledMultiLevelQuery:
    """
    Apply patches replacing certain sub-queries at given indices
    whithin a multi-query.
    """

    # Remove patches that don't change anything
    patches = [patch for patch in patches if compiled_multi_query[patch.subquery_idx] is not patch.new_subquery]
    if not patches:  # Nothing to do -> lazily return original multi-query
        return compiled_multi_query

    # Find out which of the levels we need to update
    level_indices = {patch.subquery_idx.level_idx for patch in patches}
    levels: List[CompiledLevel] = []
    for level_idx, level in enumerate(compiled_multi_query.levels):
        if level_idx in level_indices:
            # We have some replacements to be made for this level
            new_subquery_by_idx = {
                patch.subquery_idx.query_idx: patch.new_subquery
                for patch in patches
                if patch.subquery_idx.level_idx == level_idx
            }
            # Patch level queries
            level = level.clone(
                queries=[
                    # For each query return its replacement,
                    # if it is available; otherwise use the original.
                    new_subquery_by_idx.get(query_idx, query)
                    for query_idx, query in enumerate(level.queries)
                ]
            )

        # Put either the original or replacement level back into the structure
        levels.append(level)

    return compiled_multi_query.clone(levels=levels)


def add_dummy_select_column(compiled_flat_query: CompiledQuery, alias: str) -> CompiledQuery:
    """
    Add a dummy SELECT column containing an integer constant to a query.
    This is needed to join 0-dimensional LODs.
    """

    dummy_formula = CompiledFormulaInfo(
        formula_obj=formula_nodes.Formula.make(formula_nodes.LiteralInteger.make(1)),
        alias=alias,
        avatar_ids=set(),
        original_field_id=None,
    )
    return compiled_flat_query.clone(
        select=[*compiled_flat_query.select, dummy_formula],
        group_by=[*compiled_flat_query.group_by, dummy_formula],
    )
