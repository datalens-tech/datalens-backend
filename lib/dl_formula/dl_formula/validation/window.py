from __future__ import annotations

from typing import Collection

import attr

import dl_formula.core.exc as exc
import dl_formula.core.nodes as nodes
from dl_formula.inspect.env import InspectionEnvironment
import dl_formula.inspect.expression
import dl_formula.inspect.function
import dl_formula.inspect.node
from dl_formula.validation.validator import (
    Checker,
    ValidatorProxy,
)


@attr.s
class WindowFunctionChecker(Checker):
    _inspect_env: InspectionEnvironment = attr.ib()
    _allow_nested: bool = attr.ib(default=False)
    _filter_ids: Collection[str] = attr.ib(factory=frozenset)
    _unselected_dimension_ids: Collection[str] = attr.ib(factory=frozenset)

    def perform_node_check(
        self,
        validator: ValidatorProxy,
        node: nodes.FormulaItem,
        parent_stack: tuple[nodes.FormulaItem, ...],
    ) -> None:
        if not dl_formula.inspect.expression.is_window_expression(node, env=self._inspect_env):
            # node has no window functions in it,
            return

        children_w_stacks = dl_formula.inspect.expression.autonomous_children_w_stack(
            node,
            parent_stack=parent_stack,
        )
        for child, stack in children_w_stacks:
            with validator.handle_error(node=node):
                self.check_node(validator=validator, node=child, parent_stack=stack)

        if isinstance(node, nodes.WindowFuncCall):
            # inspect args
            has_agg_children = False
            for child in node.args:
                if not self._allow_nested:
                    if dl_formula.inspect.expression.is_window_expression(child, env=self._inspect_env):
                        with validator.handle_error(node=node):
                            raise exc.NestedWindowFunctionError(
                                "Nested window functions are not allowed",
                                token=dl_formula.inspect.node.get_token(child),
                                position=child.position,
                            )

                if dl_formula.inspect.expression.is_aggregate_expression(child, env=self._inspect_env):
                    has_agg_children = True

            if not has_agg_children:
                with validator.handle_error(node=node):
                    raise exc.WindowFunctionWOAggregationError(
                        f"Window function {node.name.upper()} has no aggregated expressions among its arguments",
                        token=dl_formula.inspect.node.get_token(node),
                        position=node.position,
                    )

            bfb = node.before_filter_by
            for bfb_name in sorted(bfb.field_names):
                if bfb_name in self._filter_ids and bfb_name in self._unselected_dimension_ids:
                    with validator.handle_error(node=bfb):
                        raise exc.WindowFunctionUnselectedDimensionError(
                            "Window function's BEFORE FILTER BY clause refers to a field "
                            "that is neither an aggregation nor a dimension in the query.",
                            token=dl_formula.inspect.node.get_token(bfb),
                            position=bfb.position,
                        )
