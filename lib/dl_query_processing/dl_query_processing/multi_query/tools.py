from typing import (
    Any,
    TypeVar,
)

import attr

import dl_formula.core.nodes as formula_nodes
from dl_query_processing.compilation.primitives import (
    CompiledMultiQuery,
    CompiledMultiQueryBase,
    CompiledQuery,
)


def build_requirement_subtree(multi_query: CompiledMultiQueryBase, top_query_id: str) -> CompiledMultiQueryBase:
    def _recursive_build(top_query: CompiledQuery) -> dict[str, CompiledQuery]:
        base_from_ids = set(multi_query.get_base_froms().keys())
        result: dict[str, CompiledQuery] = {}
        for from_obj in top_query.joined_from.froms:
            if from_obj.id not in base_from_ids:
                subquery = multi_query.get_query_by_id(from_obj.id)
                result[from_obj.id] = subquery
                result.update(_recursive_build(subquery))
        return result

    query_dict = _recursive_build(multi_query.get_query_by_id(top_query_id))
    queries = sorted(query_dict.values(), key=lambda item: item.id)
    result_multi_query = CompiledMultiQuery(queries=queries)  # FIXME: Replace with a lazy version
    return result_multi_query


@attr.s(frozen=True)
class CompiledMultiQueryPatch:
    patch_multi_query: CompiledMultiQueryBase = attr.ib(kw_only=True)


def apply_query_patch(multi_query: CompiledMultiQueryBase, patch: CompiledMultiQueryPatch) -> CompiledMultiQueryBase:
    if patch.patch_multi_query.is_empty():
        # Patch is empty
        return multi_query

    original_query_indices: dict[str, int] = {}
    queries: list[CompiledQuery] = []
    for idx, query in enumerate(multi_query.iter_queries()):
        original_query_indices[query.id] = idx
        queries.append(query)

    for query in patch.patch_multi_query.iter_queries():
        patch_idx = original_query_indices.get(query.id)
        if patch_idx is not None:
            queries[patch_idx] = query
        else:
            queries.append(query)

    return CompiledMultiQuery(queries=queries)


_FORMULA_NODE_TV = TypeVar("_FORMULA_NODE_TV", bound=formula_nodes.FormulaItem)


def remap_formula_obj_fields(
    node: _FORMULA_NODE_TV,
    field_name_map: dict[str, str],
) -> _FORMULA_NODE_TV:
    def remap_field(_node: formula_nodes.Field, *args: Any, **kwargs: Any) -> formula_nodes.Field:
        return formula_nodes.Field.make(name=field_name_map[_node.name])

    # In case the node is itself a Field
    if isinstance(node, formula_nodes.Field):
        return remap_field(node)  # type: ignore

    # For all other cases when it contains Fields
    updated_node = node.replace_nodes(
        match_func=lambda _node, parent_stack: isinstance(_node, formula_nodes.Field),
        replace_func=remap_field,  # type: ignore
    )
    return updated_node
