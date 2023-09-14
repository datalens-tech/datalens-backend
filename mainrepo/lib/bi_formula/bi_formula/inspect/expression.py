from __future__ import annotations

from typing import (
    AbstractSet,
    Callable,
    Dict,
    Generator,
    Iterable,
    List,
    Optional,
    Set,
    Tuple,
    Type,
)

from bi_formula.collections import NodeSet
from bi_formula.core.datatype import DataType
import bi_formula.core.exc as exc
import bi_formula.core.fork_nodes as fork_nodes
from bi_formula.core.index import NodeHierarchyIndex
import bi_formula.core.nodes as nodes
from bi_formula.core.tag import LevelTag
import bi_formula.definitions.type_strategy
import bi_formula.inspect.env
import bi_formula.inspect.function
import bi_formula.inspect.literal
import bi_formula.inspect.node
import bi_formula.translation.ext_nodes as ext_nodes


def used_fields(item: nodes.FormulaItem) -> List[nodes.Field]:
    return item.list_node_type(nodes.Field)


def used_func_calls(item: nodes.FormulaItem) -> List[nodes.FuncCall]:
    return item.list_node_type(nodes.FuncCall)


def validate_not_compiled_expressions(node: nodes.FormulaItem) -> None:
    # TODO: Remove
    if isinstance(node, ext_nodes.CompiledExpression):
        raise exc.InspectionError("CompiledExpression nodes are not supported in inspection")


def is_constant_expression(
    node: nodes.FormulaItem,
    env: Optional[bi_formula.inspect.env.InspectionEnvironment] = None,
) -> bool:
    """
    Check whether the given formula node (subtree) defines a constant expression.
    Return ``True`` is it does, otherwise ``False``.
    """
    result: Optional[bool] = None
    if env is not None:
        result = env.cache_is_const_expr.get(node)
    if result is None:
        validate_not_compiled_expressions(node)
        if isinstance(node, nodes.BaseLiteral) or isinstance(node, nodes.Null):
            result = True
        elif isinstance(node, nodes.ParenthesizedExpr):
            result = is_constant_expression(node.expr, env=env)
        else:
            result = False

        if env is not None:
            env.cache_is_unbound_expr.add(node, value=result)

    assert result is not None
    return result


def is_bound_only_to(node: nodes.FormulaItem, allow_nodes: NodeSet) -> bool:
    """
    Check whether the given formula node (subtree) defines an unbound (field-independent) expression.
    Return ``True`` is it does, otherwise ``False``.
    """

    if node in allow_nodes:
        return True

    if isinstance(node, nodes.Field):
        return False

    for child in autonomous_children(node):
        if not is_bound_only_to(node=child, allow_nodes=allow_nodes):
            return False

    return True


def is_aggregate_expression(
    node: nodes.FormulaItem,
    env: "bi_formula.inspect.env.InspectionEnvironment",
) -> bool:
    """
    Check whether the given formula node (subtree) defines an aggregate expression.
    Return ``True`` is it does, otherwise ``False``.
    """

    result = env.cache_is_aggregate_expr.get(node)
    if result is None:
        validate_not_compiled_expressions(node)
        if bi_formula.inspect.node.is_aggregate_function(node):
            result = True
        else:
            result = False
            for child in autonomous_children(node):
                if is_aggregate_expression(node=child, env=env):
                    result = True
                    break
        env.cache_is_aggregate_expr.add(node, value=result)

    return result


def is_aggregated_above_sub_node(
    node: nodes.FormulaItem,
    index: Optional[NodeHierarchyIndex] = None,
    sub_node: Optional[nodes.FormulaItem] = None,
) -> bool:
    """
    Check whether the given formula node (subtree) wraps
    the given ``sub_node`` into an aggregation.
    """

    if index is not None and sub_node is not None:
        raise ValueError("Cannot specify both index and sub_node")

    if index is None:
        assert sub_node is not None
        index = node.resolve_index(sub_node, pointer_eq=True)

    # Check whether any of the nodes wrapping sub_node are aggregations
    assert index is not None
    for wrapping_node in node.iter_index(index, exclude_last=True):  # exclude the sub_node
        # ... because we only care about its wrappers
        validate_not_compiled_expressions(wrapping_node)
        if bi_formula.inspect.node.is_aggregate_function(wrapping_node):
            return True

    return False


def is_window_expression(
    node: nodes.FormulaItem,
    env: "bi_formula.inspect.env.InspectionEnvironment",
) -> bool:
    """
    Check whether the given formula node (subtree) defines a window function expression.
    Return ``True`` is it does, otherwise ``False``.
    """

    result = env.cache_is_window_expr.get(node)
    if result is None:
        validate_not_compiled_expressions(node)
        if isinstance(node, nodes.WindowFuncCall):
            result = True
        else:
            result = False
            for child in autonomous_children(node):
                if is_window_expression(node=child, env=env):
                    result = True
                    break
        env.cache_is_window_expr.add(node, value=result)

    return result


def is_query_fork_expression(
    node: nodes.FormulaItem,
    env: "bi_formula.inspect.env.InspectionEnvironment",
) -> bool:
    """
    Check whether the given formula node (subtree) contains any ``QueryFork`` nodes
    either directly as one of its children, or indirectly nested in the tree of its children.
    Return ``True`` is it does, otherwise ``False``.
    """

    result = env.cache_is_query_fork_expr.get(node)
    if result is None:
        validate_not_compiled_expressions(node)
        if isinstance(node, fork_nodes.QueryFork):
            result = True
        else:
            result = False
            for child in autonomous_children(node):
                if is_query_fork_expression(node=child, env=env):
                    result = True
                    break
        env.cache_is_query_fork_expr.add(node, value=result)

    return result


def get_query_fork_nesting_level(
    node: nodes.FormulaItem,
    env: "bi_formula.inspect.env.InspectionEnvironment",
) -> int:
    """
    TODO
    """

    result = env.cache_query_fork_nesting.get(node)
    if result is None:
        validate_not_compiled_expressions(node)

        self_qf_level = 0
        if isinstance(node, fork_nodes.QueryFork):
            self_qf_level = 1

        max_child_qf_level = 0
        for child in autonomous_children(node):
            max_child_qf_level = max(
                max_child_qf_level,
                get_query_fork_nesting_level(node=child, env=env),
            )

        result = self_qf_level + max_child_qf_level
        env.cache_query_fork_nesting.add(node, value=result)

    return result


def infer_data_type(
    node: nodes.FormulaItem,
    field_types: Dict[str, DataType],
    env: "bi_formula.inspect.env.InspectionEnvironment",
) -> DataType:
    """Calculate the data type that should be returned by the given expression"""

    def _infer_data_type() -> DataType:
        if isinstance(node, nodes.Field):
            result = field_types.get(node.name)
            if result is None:
                raise exc.TranslationUnknownFieldError(
                    f"Data type inference error: unknown field: {node.name}",
                )

        elif isinstance(node, (nodes.BaseLiteral, nodes.Null)):
            result = bi_formula.inspect.literal.get_data_type(node)

        elif isinstance(node, nodes.ExpressionList):
            child_types = [infer_data_type(child, field_types=field_types, env=env) for child in node.children]
            result = bi_formula.definitions.type_strategy.FromArgs().get_from_args(arg_types=child_types)

        elif isinstance(node, (nodes.OperationCall, nodes.IfBlock, nodes.CaseBlock)):
            if isinstance(node, nodes.IfBlock):
                func_name = "if"
                children = autonomous_children(node)
            elif isinstance(node, nodes.CaseBlock):
                func_name = "case"
                children = autonomous_children(node)
            else:
                func_name = node.name
                children = node.args

            child_types = [infer_data_type(child, field_types=field_types, env=env) for child in children]
            result = bi_formula.inspect.function.get_return_type(
                name=func_name, arg_types=child_types, is_window=isinstance(node, nodes.WindowFuncCall)
            )

        elif isinstance(node, (nodes.Formula, nodes.ParenthesizedExpr)):
            result = infer_data_type(node.expr, field_types=field_types, env=env)

        elif isinstance(node, fork_nodes.QueryFork):
            result = infer_data_type(node.result_expr, field_types=field_types, env=env)

        else:
            raise TypeError(type(node))

        assert result is not None
        return result

    data_type = env.cache_data_type.get(node)
    if data_type is None:
        data_type = _infer_data_type()
        env.cache_data_type.add(node, value=data_type)

    return data_type


def enumerate_fields(
    node: nodes.FormulaItem,
    prefix: NodeHierarchyIndex = NodeHierarchyIndex(),
) -> Generator[Tuple[NodeHierarchyIndex, nodes.Field], None, None]:
    """Just like ``enumerate``, but yields only field nodes."""
    for index, sub_node in node.enumerate(prefix=prefix):
        if isinstance(sub_node, nodes.Field):
            yield index, sub_node


def enumerate_autonomous_children(
    node: nodes.FormulaItem,
    prefix: NodeHierarchyIndex = NodeHierarchyIndex(),
    exclude_node_types: Tuple[Type[nodes.FormulaItem], ...] = (),
    parent_stack: Tuple[nodes.FormulaItem, ...] = (),
) -> Iterable[Tuple[NodeHierarchyIndex, nodes.FormulaItem, Tuple[nodes.FormulaItem, ...]]]:
    """
    Return relative hierarchy indexes and nodes for all logical children -
    nodes that can be interpreted as standalone expressions.
    (e.g. a ``WhenPart`` or ``WindowGrouping`` can't - they're just parts
    of larger constructs - ``CaseBlock`` and ``WindowFunction`` respectively)
    """

    stack = parent_stack + (node,)
    for index, sub_node in node.enumerate(max_depth=1, prefix=prefix):
        if sub_node is node:
            # Don't need self - only children
            continue

        if isinstance(sub_node, exclude_node_types):
            continue

        if not sub_node.autonomous:
            yield from enumerate_autonomous_children(sub_node, prefix=index, parent_stack=stack)
            continue

        yield index, sub_node, stack


def autonomous_children_w_stack(
    node: nodes.FormulaItem,
    exclude_node_types: Tuple[Type[nodes.FormulaItem], ...] = (),
    parent_stack: Tuple[nodes.FormulaItem, ...] = (),
) -> Iterable[Tuple[nodes.FormulaItem, Tuple[nodes.FormulaItem, ...]]]:
    for _, child, stack in enumerate_autonomous_children(
        node,
        exclude_node_types=exclude_node_types,
        parent_stack=parent_stack,
    ):
        yield child, stack


def autonomous_children(
    node: nodes.FormulaItem,
    exclude_node_types: Tuple[Type[nodes.FormulaItem], ...] = (),
) -> Iterable[nodes.FormulaItem]:
    for _, child, _ in enumerate_autonomous_children(node, exclude_node_types=exclude_node_types):
        yield child


def collect_tags(node: nodes.FormulaItem) -> AbstractSet[LevelTag]:
    result: Set[LevelTag] = set()
    for _, child in node.enumerate():
        level_tag = child.level_tag
        if level_tag is not None:
            result.add(level_tag)
    return result


def get_wrapping_level(
    node: nodes.FormulaItem,
    qualify_node_cb: Callable[[nodes.FormulaItem], bool],
    stop_at_node_cb: Optional[Callable[[nodes.FormulaItem], bool]] = None,
    stop_at_level: Optional[int] = None,
) -> int:
    """
    Calculate wrapping level for node.

    This is the maximum number of times ``qualify_node_cb`` returns ``True``
    in a sub-tree of the given node.
    Recursion stops whenever ``stop_at_node_cb`` (if specified) returns ``True``.
    """

    def _get_level_recursively(cur_node: nodes.FormulaItem, level: int) -> int:
        if stop_at_node_cb is not None and stop_at_node_cb(cur_node):
            return level
        if qualify_node_cb(cur_node):
            level += 1

        if stop_at_level is not None and level >= stop_at_level:
            return level

        child_max_level = max((_get_level_recursively(child, level) for child in cur_node.children), default=level)
        return max(level, child_max_level)

    return _get_level_recursively(node, 0)


def _match_bfb_names(node: nodes.FormulaItem, bfb_names: AbstractSet[str]) -> bool:
    if isinstance(node, (nodes.FuncCall, fork_nodes.QueryFork)):
        return node.before_filter_by.field_names == bfb_names
    # Assume True if BFB is not supported
    return True


def get_window_function_wrapping_level(node: nodes.FormulaItem, bfb_names: AbstractSet[str]) -> int:
    return get_wrapping_level(
        node,
        qualify_node_cb=lambda _node: isinstance(_node, nodes.WindowFuncCall),
        stop_at_node_cb=lambda _node: (not _match_bfb_names(_node, bfb_names)),
    )


def is_double_aggregated_expression(node: nodes.FormulaItem) -> bool:
    level = get_wrapping_level(
        node,
        qualify_node_cb=lambda _node: bi_formula.inspect.node.is_aggregate_function(_node),
        stop_at_level=2,
    )
    return level == 2


def get_qfork_wrapping_level_until_winfunc(node: nodes.FormulaItem, bfb_names: AbstractSet[str]) -> int:
    return get_wrapping_level(
        node,
        qualify_node_cb=lambda _node: isinstance(_node, fork_nodes.QueryFork),
        stop_at_node_cb=lambda _node: (
            isinstance(_node, nodes.WindowFuncCall) or not _match_bfb_names(_node, bfb_names)
        ),
    )


def contains_non_default_lod_dimensions(node: nodes.FormulaItem) -> bool:
    if bi_formula.inspect.node.has_non_default_lod_dimensions(node):
        return True
    for child in autonomous_children(node):
        if contains_non_default_lod_dimensions(child):
            return True

    return False


def contains_extended_aggregations(node: nodes.FormulaItem, include_double_agg: bool = False) -> bool:
    if bi_formula.inspect.node.is_extended_aggregation(node):
        return True
    for child in autonomous_children(node):
        if contains_extended_aggregations(child, include_double_agg=False):
            return True

    if include_double_agg:
        return is_double_aggregated_expression(node)

    return False


def contains_lookup_functions(node: nodes.FormulaItem) -> bool:
    if bi_formula.inspect.node.is_lookup_function(node):
        return True
    for child in autonomous_children(node):
        if contains_lookup_functions(child):
            return True

    return False


def resolve_dimensions(
    node_stack: Iterable[nodes.FormulaItem],
    dimensions: List[nodes.FormulaItem],  # TODO: rename to global_dimensions
    env: "bi_formula.inspect.env.InspectionEnvironment",
    exclude_constants: bool = True,
) -> Tuple[List[nodes.FormulaItem], NodeSet, NodeSet]:
    """
    Return dimension list for current node and set of dimension extracts of the parent node
    ("the surrounding environment")
    """

    dimension_set = parent_dimension_set = NodeSet(dimensions)

    # Generate dimension list recursively
    for node in node_stack:  # from top level to the node in question
        if not (
            isinstance(node, nodes.FuncCall)
            and bi_formula.inspect.node.is_aggregate_function(node)
            or isinstance(node, fork_nodes.QueryFork)
        ):
            continue

        lod = node.lod
        if isinstance(lod, nodes.InheritedLodSpecifier):
            continue

        parent_dimensions = dimensions
        parent_dimension_set = dimension_set

        if isinstance(lod, nodes.FixedLodSpecifier):
            dimensions = list(lod.dim_list)
        elif isinstance(lod, nodes.IncludeLodSpecifier):
            dimensions = parent_dimensions + [dim for dim in lod.dim_list if dim not in parent_dimension_set]
        elif isinstance(lod, nodes.ExcludeLodSpecifier):
            exclude_set = NodeSet(lod.dim_list)
            dimensions = [dim for dim in parent_dimensions if dim not in exclude_set]
        else:
            raise TypeError(f"Invalid type for lod: {type(lod)}")

        if exclude_constants:
            dimensions = [dim for dim in dimensions if not is_constant_expression(dim, env=env)]

        dimension_set = NodeSet(dimensions)

    return dimensions, dimension_set, parent_dimension_set


def iter_aggregation_functions(node: nodes.FormulaItem) -> Generator[nodes.FuncCall, None, None]:
    """
    Iterate over all (indirect) children that are aggregate functions.
    Do not go inside aggregate functions.
    """

    for _, child in node.enumerate(max_depth=1):
        if child is node:
            # Don't need self - only children
            continue

        if bi_formula.inspect.node.is_aggregate_function(child):
            assert isinstance(child, nodes.FuncCall)
            yield child

        else:
            yield from iter_aggregation_functions(child)


def iter_operation_calls(node: nodes.FormulaItem) -> Generator[nodes.OperationCall, None, None]:
    """
    Iterate over all (indirect) children that are operation calls (functions and operators).
    """

    for _, child in node.enumerate():
        if isinstance(child, nodes.OperationCall):
            yield child


def get_toplevel_dimension_set(node: nodes.FormulaItem) -> NodeSet:
    toplevel_dimensions: NodeSet = NodeSet()
    for agg_func in iter_aggregation_functions(node):
        lod = agg_func.lod
        assert isinstance(lod, nodes.FixedLodSpecifier)  # Dimensions must already be resolved
        for dim in lod.dim_list:
            if dim not in toplevel_dimensions:
                toplevel_dimensions.add(dim)

    return toplevel_dimensions
