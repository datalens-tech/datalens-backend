from __future__ import annotations

from itertools import chain
from typing import (
    ClassVar,
    List,
    Sequence,
    Tuple,
    Type,
)

import attr

from dl_formula.collections import NodeSet
import dl_formula.core.exc as exc
import dl_formula.core.fork_nodes as fork_nodes
import dl_formula.core.nodes as nodes
from dl_formula.inspect.env import InspectionEnvironment
import dl_formula.inspect.expression
import dl_formula.inspect.function
import dl_formula.inspect.node
from dl_formula.validation.env import ValidationEnvironment
from dl_formula.validation.validator import (
    Checker,
    ValidatorProxy,
)


@attr.s
class AggregationChecker(Checker):
    _inspect_env: InspectionEnvironment = attr.ib()
    _valid_env: ValidationEnvironment = attr.ib()
    _allow_nested_agg: bool = attr.ib(default=False)
    _allow_inconsistent_inside_agg: bool = attr.ib(default=False)
    _global_dimensions: List[nodes.FormulaItem] = attr.ib(kw_only=True)

    _exclude_node_types: ClassVar[Tuple[Type[nodes.FormulaItem], ...]] = (
        fork_nodes.QueryForkJoiningBase,
        nodes.LodSpecifier,
    )

    """
    Check whether the usage of aggregations is consistent throughout the expression.
    The following are considered an error:
    - multiple aggregations in nested nodes
    - nodes with different aggregation levels are present at a single level as children of one node
    """

    def _check_is_dimension_bound(self, node: nodes.FormulaItem, dimension_set: NodeSet) -> bool:
        """
        Return ``True`` if the node tree is dependent only on dimensions.
        Cache the results in ``self._valid_env``.
        """

        dim_bound = dl_formula.inspect.expression.is_bound_only_to(node, allow_nodes=dimension_set)
        return dim_bound

    def perform_node_check(
        self,
        validator: ValidatorProxy,
        node: nodes.FormulaItem,
        parent_stack: Tuple[nodes.FormulaItem, ...],
    ) -> None:
        if not dl_formula.inspect.expression.is_aggregate_expression(node, env=self._inspect_env):
            # node has no aggregations in it,
            # so by definition there's nothing wrong with its aggregation
            return

        def _aggregation_in_stack(node_stack: Sequence[nodes.FormulaItem]) -> bool:
            for wrapping_node in node_stack:
                if dl_formula.inspect.node.is_aggregate_function(wrapping_node):
                    return True
            return False

        not_agg_children = False
        agg_children = False
        children_w_stacks = dl_formula.inspect.expression.autonomous_children_w_stack(
            node,  # TODO: Find a solution more elegant than excludes:
            parent_stack=parent_stack,
            exclude_node_types=self._exclude_node_types,
        )

        for child, stack in children_w_stacks:
            with validator.handle_error(node=node):
                self.check_node(validator=validator, node=child, parent_stack=stack)

            if dl_formula.inspect.expression.is_aggregate_expression(child, env=self._inspect_env):
                agg_children = True
            else:
                _, dimension_set, _ = dl_formula.inspect.expression.resolve_dimensions(
                    node_stack=chain(stack, (child,)),
                    dimensions=self._global_dimensions,
                    env=self._inspect_env,
                )
                dim_bound = self._check_is_dimension_bound(child, dimension_set=dimension_set)
                if not dim_bound:
                    not_agg_children = True
            if agg_children and not_agg_children:
                with validator.handle_error(node=node):
                    do_raise = True
                    if self._allow_inconsistent_inside_agg:
                        # Allow agg and non-agg arguments inside another aggregation
                        # (find aggregation in parent_stack)
                        do_raise = not _aggregation_in_stack(parent_stack)

                    if do_raise:
                        raise exc.InconsistentAggregationError(
                            "Inconsistent aggregation among operands",
                            token=dl_formula.inspect.node.get_token(child),
                            position=child.position,
                        )

        if not self._allow_nested_agg and dl_formula.inspect.node.is_aggregate_function(node):
            # function is an aggregation and some of its arguments are already aggregated -> double aggregation
            if agg_children:
                with validator.handle_error(node=node):
                    raise exc.DoubleAggregationError(
                        "Double aggregation is forbidden",
                        token=dl_formula.inspect.node.get_token(node),
                        position=node.position,
                    )
