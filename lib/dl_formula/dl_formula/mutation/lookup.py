from __future__ import annotations

import abc
from typing import (
    ClassVar,
    Dict,
    List,
    Optional,
    Sequence,
    Tuple,
    Type,
)

import attr

from dl_formula.collections import NodeSet
import dl_formula.core.aux_nodes as aux_nodes
import dl_formula.core.exc as exc
import dl_formula.core.fork_nodes as fork_nodes
import dl_formula.core.nodes as nodes
from dl_formula.inspect.expression import (
    is_aggregate_expression,
    used_fields,
)
from dl_formula.mutation.dim_resolution import DimensionResolvingMutationBase
from dl_formula.mutation.mutation import FormulaMutation


class LookupFunctionMutatorBase(abc.ABC):
    name: ClassVar[str]
    supported_arg_counts: Tuple[int, ...]

    @classmethod
    @abc.abstractmethod
    def get_result_expression(
        cls,
        func_args: Sequence[nodes.FormulaItem],
    ) -> nodes.FormulaItem:
        raise NotImplementedError

    @classmethod
    @abc.abstractmethod
    def get_lookup_dimension(
        cls,
        func_args: Sequence[nodes.FormulaItem],
    ) -> nodes.FormulaItem:
        raise NotImplementedError

    @classmethod
    @abc.abstractmethod
    def get_lookup_conditions(
        cls,
        func_args: Sequence[nodes.FormulaItem],
    ) -> List[fork_nodes.JoinConditionBase]:
        raise NotImplementedError

    @classmethod
    @abc.abstractmethod
    def get_bfb_filter_mutations(
        cls,
        lookup_dimension: nodes.FormulaItem,
        func_args: Sequence[nodes.FormulaItem],
    ) -> List[fork_nodes.BfbFilterMutationSpec]:
        raise NotImplementedError


LOOKUP_MUTATOR_REGISTRY: Dict[str, Type[LookupFunctionMutatorBase]] = {}


def register_lookup_mutator(mutator_cls: Type[LookupFunctionMutatorBase]) -> Type[LookupFunctionMutatorBase]:
    LOOKUP_MUTATOR_REGISTRY[mutator_cls.name] = mutator_cls
    return mutator_cls


class DateLookupFunctionMutatorBase(LookupFunctionMutatorBase):
    @classmethod
    def get_result_expression(
        cls,
        func_args: Sequence[nodes.FormulaItem],
    ) -> nodes.FormulaItem:
        return func_args[0]

    @classmethod
    def get_lookup_dimension(
        cls,
        func_args: Sequence[nodes.FormulaItem],
    ) -> nodes.FormulaItem:
        return func_args[1]


_MONTH_BASED_UNITS = frozenset(("month", "year", "quarter"))


@register_lookup_mutator
class AgoLookupFunctionMutator(DateLookupFunctionMutatorBase):
    name = "ago"
    supported_arg_counts = (2, 3, 4)

    @classmethod
    def get_lookup_conditions(
        cls,
        func_args: Sequence[nodes.FormulaItem],
    ) -> List[fork_nodes.JoinConditionBase]:
        lookup_dimension = cls.get_lookup_dimension(func_args)
        fork_join_expr = nodes.FuncCall.make(
            name="dateadd",
            args=[lookup_dimension, *func_args[2:]],
        )
        main_lookup_condition = fork_nodes.BinaryJoinCondition.make(expr=lookup_dimension, fork_expr=fork_join_expr)

        conditions: List[fork_nodes.JoinConditionBase] = [main_lookup_condition]

        # An additional condition is needed for cases when the unit is month-based
        # to avoid date duplication for months with different lengths
        if len(func_args) > 2:  # default unit is "day", so we're not interested
            unit_name = "day"
            try:
                string_arg: nodes.LiteralString = [
                    arg for arg in func_args[2:] if isinstance(arg, nodes.LiteralString)
                ][0]
                unit_name = string_arg.value.lower()
            except IndexError:
                pass

            if unit_name in _MONTH_BASED_UNITS:
                day_eq_condition = fork_nodes.BinaryJoinCondition.make(
                    expr=nodes.FuncCall.make("day", args=[lookup_dimension]),
                    fork_expr=nodes.FuncCall.make("day", args=[lookup_dimension]),
                )
                conditions.append(day_eq_condition)

        return conditions

    @classmethod
    def get_bfb_filter_mutations(
        cls,
        lookup_dimension: nodes.FormulaItem,
        func_args: Sequence[nodes.FormulaItem],
    ) -> List[fork_nodes.BfbFilterMutationSpec]:
        return [
            fork_nodes.BfbFilterMutationSpec.make(
                original=lookup_dimension,
                replacement=nodes.FuncCall.make(
                    name="dateadd",
                    args=[lookup_dimension, *func_args[2:]],
                ),
            )
        ]


@register_lookup_mutator
class AtDateLookupFunctionMutator(DateLookupFunctionMutatorBase):
    name = "at_date"
    supported_arg_counts = (3,)

    @classmethod
    def get_lookup_conditions(
        cls,
        func_args: Sequence[nodes.FormulaItem],
    ) -> List[fork_nodes.JoinConditionBase]:
        lookup_dimension = cls.get_lookup_dimension(func_args)
        lookup_condition = fork_nodes.BinaryJoinCondition.make(expr=func_args[2], fork_expr=lookup_dimension)
        return [lookup_condition]

    @classmethod
    def get_bfb_filter_mutations(
        cls,
        lookup_dimension: nodes.FormulaItem,
        func_args: Sequence[nodes.FormulaItem],
    ) -> List[fork_nodes.BfbFilterMutationSpec]:
        return []


def _get_arg_error_node(node: nodes.FuncCall) -> Optional[aux_nodes.ErrorNode]:
    func_name = node.name
    mutator = LOOKUP_MUTATOR_REGISTRY[func_name]

    if len(node.args) not in mutator.supported_arg_counts:
        return aux_nodes.ErrorNode.make(
            err_code=exc.LookupFunctionArgNumberError.default_code,
            message=(
                f"Invalid number of arguments for function {func_name.upper()}: "
                f"{len(node.args)}. Either 2, 3 or 4 are needed."
            ),
            meta=node.meta,
        )

    return None


@attr.s
class LookupFunctionToQueryForkMutation(DimensionResolvingMutationBase):
    """
    A mutation that converts all lookup (`AGO` and `AT_DATE`) function calls to ``QueryFork`` nodes
    using a list of explicit joining conditions.
    """

    _allow_empty_dimensions: bool = attr.ib(kw_only=True)

    def match_node(self, node: nodes.FormulaItem, parent_stack: Tuple[nodes.FormulaItem, ...]) -> bool:
        return isinstance(node, nodes.FuncCall) and node.name in LOOKUP_MUTATOR_REGISTRY

    def make_replacement(
        self,
        old: nodes.FormulaItem,
        parent_stack: Tuple[nodes.FormulaItem, ...],
    ) -> nodes.FormulaItem:
        assert isinstance(old, nodes.FuncCall)
        func_name = old.name
        assert func_name in LOOKUP_MUTATOR_REGISTRY

        err_node = _get_arg_error_node(old)
        if err_node is not None:
            return err_node

        mutator = LOOKUP_MUTATOR_REGISTRY[func_name]

        # Prepare result_expr
        result_expr = mutator.get_result_expression(old.args)
        if not is_aggregate_expression(node=result_expr, env=self._inspect_env):
            return aux_nodes.ErrorNode.make(
                err_code=exc.LookupFunctionWOAggregationError.default_code,
                message=f"Result expression of function {func_name.upper()} is not aggregated.",
                meta=old.meta,
            )

        # Prepare joining
        lookup_dimension = mutator.get_lookup_dimension(old.args)
        # Args will be validated as part of the condition expression
        lookup_conditions = mutator.get_lookup_conditions(old.args)
        ignore_dimensions_set = NodeSet(nodes=old.ignore_dimensions.children)

        if is_aggregate_expression(node=lookup_dimension, env=self._inspect_env):
            return aux_nodes.ErrorNode.make(
                err_code=exc.LookupFunctionAggregatedDimensionError.default_code,
                message=f"The lookup dimension of function {func_name.upper()} is an aggregation.",
                meta=old.meta,
            )
        if lookup_dimension in ignore_dimensions_set:
            return aux_nodes.ErrorNode.make(
                err_code=exc.LookupFunctionIgnoredLookupDimensionError.default_code,
                message=f"Cannot ignore lookup dimension of function {func_name.upper()}",
                meta=old.meta,
            )

        lookup_condition_used_fields: List[nodes.Field] = []
        for lookup_condition in lookup_conditions:
            lookup_condition_used_fields.extend(used_fields(lookup_condition))
        if not lookup_condition_used_fields:
            return aux_nodes.ErrorNode.make(
                err_code=exc.LookupFunctionConstantLookupDimensionError.default_code,
                message=f"Cannot use a constant expression as lookup dimension of function {func_name.upper()}",
                meta=old.meta,
            )

        dimensions, _, _ = self._generate_dimensions(node=old, parent_stack=parent_stack)

        condition_list: List[fork_nodes.JoinConditionBase] = []
        dim_list: List[nodes.FormulaItem] = []
        found_conditional_dimension = False
        for dimension_expr in dimensions:
            if dimension_expr in ignore_dimensions_set:
                # This dimension is specified in the function's IGNORE DIMENSIONS clause
                # so, do just that - skip it in the JOIN condition list
                continue

            dim_condition: fork_nodes.JoinConditionBase
            if dimension_expr == lookup_dimension:
                found_conditional_dimension = True
                condition_list.extend(lookup_conditions)
            else:
                dim_condition = fork_nodes.SelfEqualityJoinCondition.make(expr=dimension_expr)
                condition_list.append(dim_condition)
            dim_list.append(dimension_expr)

        if (not self._allow_empty_dimensions or dimensions) and not found_conditional_dimension:
            # This specific one wasn't found.
            # Dimensions can be empty during single-formula validation,
            # so we must allow this case to go on without errors
            return aux_nodes.ErrorNode.make(
                err_code=exc.LookupFunctionUnselectedDimensionError.default_code,
                message=(
                    f"Invalid dimension for function {func_name.upper()}. "
                    "It must be explicitly used in the data request as a dimension."
                ),
                meta=old.meta,
            )

        joining = fork_nodes.QueryForkJoiningWithList.make(condition_list=condition_list)
        lod = nodes.InheritedLodSpecifier()
        bfb_filter_mutations = fork_nodes.BfbFilterMutationCollectionSpec.make(
            *mutator.get_bfb_filter_mutations(lookup_dimension, old.args)
        )

        new_node = fork_nodes.QueryFork.make(
            join_type=fork_nodes.JoinType.left,
            result_expr=result_expr,
            joining=joining,
            before_filter_by=old.before_filter_by,
            lod=lod,
            bfb_filter_mutations=bfb_filter_mutations,
            meta=old.meta,
        )
        return new_node


@attr.s
class LookupDefaultBfbMutation(FormulaMutation):
    """
    A mutation that adds the lookup dimension of fork functions (`AGO` and `AT_DATE`)
    to the `BEFORE FILTER BY` clause.
    """

    def match_node(self, node: nodes.FormulaItem, parent_stack: Tuple[nodes.FormulaItem, ...]) -> bool:
        return isinstance(node, nodes.FuncCall) and node.name in LOOKUP_MUTATOR_REGISTRY

    def make_replacement(
        self, old: nodes.FormulaItem, parent_stack: Tuple[nodes.FormulaItem, ...]
    ) -> nodes.FormulaItem:
        assert isinstance(old, nodes.FuncCall)
        func_name = old.name
        assert func_name in LOOKUP_MUTATOR_REGISTRY

        err_node = _get_arg_error_node(old)
        if err_node is not None:
            return err_node

        mutator = LOOKUP_MUTATOR_REGISTRY[func_name]
        lookup_dimension = mutator.get_lookup_dimension(old.args)

        if isinstance(lookup_dimension, nodes.Field) and lookup_dimension.name not in old.before_filter_by.field_names:
            # Lookup dimension is not yet present in BFB, so add it.
            new_before_filter_by = nodes.BeforeFilterBy.make(
                field_names=old.before_filter_by.field_names | {lookup_dimension.name}
            )

            new_node = nodes.FuncCall.make(
                name=old.name,
                args=old.args,
                lod=old.lod,
                ignore_dimensions=old.ignore_dimensions,
                before_filter_by=new_before_filter_by,
                meta=old.meta,
            )

        else:
            # No changes
            new_node = old

        return new_node
