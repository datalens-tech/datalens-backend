from __future__ import annotations

from collections import defaultdict
from contextlib import contextmanager
from functools import (
    partial,
    wraps,
)
from itertools import chain
import logging
from typing import (
    AbstractSet,
    Callable,
    Collection,
    Generator,
    Iterable,
    Optional,
    Protocol,
    Sequence,
    Union,
    overload,
)
import uuid

from dl_constants.enums import (
    AggregationFunction,
    BinaryJoinOperator,
    CalcMode,
    ConditionPartCalcMode,
    FieldType,
    ManagedBy,
    OrderDirection,
    UserDataType,
)
from dl_core.components.ids import (
    AvatarId,
    FieldId,
    make_field_id,
)
from dl_core.fields import (
    BIField,
    DirectCalculationSpec,
    FormulaCalculationSpec,
)
from dl_core.multisource import (
    AvatarRelation,
    BinaryCondition,
    ConditionPart,
    ConditionPartDirect,
    ConditionPartFormula,
    ConditionPartResultField,
)
from dl_formula.collections import NodeValueMap
import dl_formula.core.aux_nodes as formula_aux_nodes
from dl_formula.core.datatype import DataType
import dl_formula.core.exc as formula_exc
from dl_formula.core.index import NodeHierarchyIndex
from dl_formula.core.message_ctx import (
    FormulaErrorCtx,
    MessageLevel,
)
import dl_formula.core.nodes as formula_nodes
from dl_formula.inspect.env import InspectionEnvironment
from dl_formula.inspect.expression import (
    enumerate_fields,
    infer_data_type,
    is_aggregate_expression,
    is_aggregated_above_sub_node,
    is_window_expression,
    used_fields,
    used_func_calls,
)
from dl_formula.mutation.bfb import RemapBfbMutation
from dl_formula.mutation.general import (
    ConvertBlocksToFunctionsMutation,
    IgnoreParenthesisWrapperMutation,
)
from dl_formula.mutation.lookup import LookupDefaultBfbMutation
from dl_formula.mutation.mutation import (
    FormulaMutation,
    apply_mutations,
)
from dl_formula.mutation.optimization import OptimizeConstMathOperatorMutation
from dl_formula.mutation.window import (
    AmongToWithinGroupingMutation,
    DefaultWindowOrderingMutation,
    IgnoreExtraWithinGroupingMutation,
)
from dl_formula.parser.base import FormulaParser
from dl_formula.validation.aggregation import AggregationChecker
from dl_formula.validation.env import ValidationEnvironment
from dl_formula.validation.validator import (
    Checker,
    validate,
)
from dl_formula.validation.window import WindowFunctionChecker
from dl_query_processing.column_registry import ColumnRegistry
from dl_query_processing.compilation.field_registry import FieldRegistry
from dl_query_processing.compilation.helpers import make_literal_node
from dl_query_processing.compilation.primitives import (
    CompiledFormulaInfo,
    CompiledJoinOnFormulaInfo,
)
from dl_query_processing.compilation.specs import (
    OrderByFieldSpec,
    ParameterValueSpec,
    SelectWrapperSpec,
)
from dl_query_processing.compilation.type_mapping import (
    BI_TO_FORMULA_TYPES,
    DEFAULT_DATA_TYPE,
    FORMULA_TO_BI_TYPES,
)
from dl_query_processing.compilation.wrapper_applicator import ExpressionWrapperApplicator
from dl_query_processing.enums import ProcessingStage
import dl_query_processing.exc
from dl_utils.utils import enum_not_none


LOGGER = logging.getLogger(__name__)


class FieldProcessingStageManager:
    """
    Provides handling and storage of:
    - field errors
    - processing results
    - expression data type
    from different processing stages
    """

    default_type_on_error = DEFAULT_DATA_TYPE
    save_type_stages = {
        ProcessingStage.substitution,
        ProcessingStage.aggregation,
        ProcessingStage.final,
    }

    def __init__(self, columns: ColumnRegistry, inspect_env: InspectionEnvironment):
        self._columns = columns
        self._inspect_env = inspect_env

        self._errors: dict[str, dict[ProcessingStage, list[FormulaErrorCtx]]] = defaultdict(lambda: defaultdict(list))
        self._exprs: dict[str, dict[ProcessingStage, Optional[formula_nodes.Formula]]] = defaultdict(
            lambda: defaultdict(lambda: None)
        )
        self._data_types: dict[str, dict[ProcessingStage, DataType]] = defaultdict(
            lambda: defaultdict(lambda: DEFAULT_DATA_TYPE)
        )

    def _get_formula_obj_data_type(self, formula_obj: formula_nodes.Formula) -> DataType:
        try:
            try:
                return infer_data_type(
                    node=formula_obj,
                    field_types=self._columns.get_column_formula_types(),
                    env=self._inspect_env,
                )
            except KeyError as e:
                raise formula_exc.DataTypeError("Unknown type for column") from e
        except formula_exc.FormulaError:
            return self.default_type_on_error

    def add_errors(self, *errors: FormulaErrorCtx, field: BIField, stage: ProcessingStage) -> None:
        registered_errors: list[FormulaErrorCtx] = self._errors[field.guid][stage]
        for error in errors:
            if error not in registered_errors:
                registered_errors.append(error)

    def get_errors(
        self,
        field: BIField,
        stage: Union[ProcessingStage, Iterable[ProcessingStage], None] = None,
    ) -> list[FormulaErrorCtx]:
        """Return errors from cache for the given field (and type)."""

        field_errors = self._errors[field.guid]
        if stage is not None and isinstance(stage, ProcessingStage):
            return field_errors[stage]

        return list(
            chain.from_iterable(
                [
                    errors
                    for st, errors in sorted(field_errors.items(), key=lambda el: el[0].name)
                    if stage is None or st in stage
                ]
            )
        )

    def set_result(self, formula_obj: formula_nodes.Formula, *, field: BIField, stage: ProcessingStage) -> None:
        self._exprs[field.guid][stage] = formula_obj
        if stage in self.save_type_stages:
            self._data_types[field.guid][stage] = self._get_formula_obj_data_type(formula_obj)

    def get_result(self, field: BIField, stage: ProcessingStage) -> Optional[formula_nodes.Formula]:
        return self._exprs[field.guid][stage]

    def get_data_type(self, field: BIField, stage: ProcessingStage) -> DataType:
        return self._data_types[field.guid][stage]

    def get_user_type(self, field: BIField, stage: ProcessingStage) -> UserDataType:
        return FORMULA_TO_BI_TYPES[self.get_data_type(field=field, stage=stage)]

    def raise_if_any(self, field: BIField, stage: ProcessingStage) -> None:
        """Raise ``FormulaHandlingError`` if there are any errors in cache for the given field (and type)."""

        errors = self.get_errors(field=field, stage=stage)
        if errors:
            raise dl_query_processing.exc.FormulaHandlingError(*errors, stage=stage, field=field)

    def clear(self, field: BIField, stage: Optional[ProcessingStage] = None) -> None:
        """Clear error cache for the given field (and type)."""

        if stage is not None:
            del self._errors[field.guid][stage]
            del self._exprs[field.guid][stage]
            del self._data_types[field.guid][stage]
        else:
            self._errors[field.guid].clear()
            self._exprs[field.guid].clear()
            self._data_types[field.guid].clear()

    @contextmanager
    def handle(
        self,
        field: BIField,
        stage: ProcessingStage,
        collect: bool = True,
        raise_: bool = True,
    ) -> Generator[None, None, None]:
        try:
            yield
        except formula_exc.FormulaError as err:
            if collect:
                # Save error for later use
                self.add_errors(*err.errors, field=field, stage=stage)
            if raise_:
                pass_on_errors = err.errors
                if collect:
                    # Do not add error messages to the re-raised exception
                    # to avoid their duplication in the error storage
                    # when the error is caught in the next-level wraper
                    pass_on_errors = []
                if pass_on_errors:
                    LOGGER.error(
                        f"Formula handling errors found on stage {stage.name} "
                        f"for field {field.title!r} ({field.guid}): "
                        f"{[str(e) for e in pass_on_errors]}"
                    )
                raise dl_query_processing.exc.FormulaHandlingError(*pass_on_errors, stage=stage, field=field) from err


class StageProcCallable(Protocol):
    def __call__(self, field: BIField, collect_errors: bool = False) -> formula_nodes.Formula:
        ...


# https://github.com/python/typing/discussions/1040
# and all of this just for the positional argument...
class StageProcType(Protocol):
    def __call__(_, self: FormulaCompiler, field: BIField, collect_errors: bool = False) -> formula_nodes.Formula:
        ...

    @overload
    def __get__(self, obj: FormulaCompiler, objtype: type[FormulaCompiler] | None = None) -> StageProcCallable:
        ...

    @overload
    def __get__(self, obj: None, objtype: type[FormulaCompiler] | None = None) -> StageProcCallable:
        ...

    def __get__(self, obj: FormulaCompiler | None, objtype: type[FormulaCompiler] | None = None) -> StageProcCallable:
        ...


def implements_stage(stage: ProcessingStage) -> Callable[[StageProcType], StageProcType]:
    """
    A parameterized decorator.

    Mark a ``FormulaCompiler`` method as an implementation of a certain formula processing stage.
    This adds the enabling of various caches and error handlers needed to optimize
    formula generation and validation.
    """

    def decorator(func: StageProcType) -> StageProcType:
        @wraps(func)
        def wrapper(self: "FormulaCompiler", field: BIField, collect_errors: bool = False) -> formula_nodes.Formula:
            # First check the error cache
            if not collect_errors:
                # Error collection is disabled, so we can just raise the first error we encounter
                self._stage_manager.raise_if_any(field=field, stage=stage)

            # Errors were either disabled or not found in cache,
            # so move on to the result cache
            formula_obj = self._stage_manager.get_result(field=field, stage=stage)
            if formula_obj is None:
                # No cached result was found, so generate it,
                # add it (and any errors encountered on the way) to cache
                # (all the cache stuff is done inside ``self._stage_manager.handle(...)``)
                with self._stage_manager.handle(field=field, stage=stage, collect=collect_errors):
                    try:
                        formula_obj = func(self, field, collect_errors)
                    except Exception:
                        LOGGER.debug(f"Stage {stage.name} failed for field {field.title!r} ({field.guid})")
                        raise
                assert formula_obj is not None
                self._stage_manager.set_result(formula_obj, field=field, stage=stage)
            return formula_obj

        return wrapper

    return decorator


def _unsupported_cast(typename: str) -> str:
    return f"__UNSUPPORTED_CAST_TO_{typename.upper()}__"


_SUPPORTED_CASTS_FUNCTIONS = {
    UserDataType.boolean: "bool",
    UserDataType.date: "date",
    UserDataType.datetime: "datetime",
    UserDataType.genericdatetime: "genericdatetime",
    UserDataType.float: "float",
    UserDataType.integer: "int",
    UserDataType.geopoint: "geopoint",
    UserDataType.geopolygon: "geopolygon",
    UserDataType.string: "str",
    UserDataType.markup: "markup",
}
_CAST_FUNCTIONS = {
    bi_type: _SUPPORTED_CASTS_FUNCTIONS.get(bi_type) or _unsupported_cast(bi_type.name) for bi_type in UserDataType
}
_ALLOWED_PARAMETER_TYPES = {
    UserDataType.string,
    UserDataType.integer,
    UserDataType.float,
    UserDataType.boolean,
    UserDataType.date,
    UserDataType.datetime,
    UserDataType.genericdatetime,
}


class FormulaCompiler:
    _agg_functions = {
        AggregationFunction.countunique: "countd",
        AggregationFunction.count: "count",
        AggregationFunction.sum: "sum",
        AggregationFunction.avg: "avg",
        AggregationFunction.max: "max",
        AggregationFunction.min: "min",
    }
    _join_condition_operators = {
        BinaryJoinOperator.eq: "_==",
        BinaryJoinOperator.ne: "_!=",
        BinaryJoinOperator.gt: ">",
        BinaryJoinOperator.gte: ">=",
        BinaryJoinOperator.lt: "<",
        BinaryJoinOperator.lte: "<=",
    }

    def __init__(
        self,
        *,
        all_fields: Iterable[BIField],
        columns: ColumnRegistry,
        formula_parser: FormulaParser,
        group_by_ids: Collection[str] = (),
        filter_ids: Collection[str] = (),
        order_by_specs: Sequence[OrderByFieldSpec] = (),
        mock_among_dimensions: bool = False,  # mock dimensions for AMONG clauses (in validation mode)
        inspect_env: Optional[InspectionEnvironment] = None,
        suppress_double_aggregations: bool = True,
        allow_nested_window_functions: bool = False,
        parameter_value_specs: Sequence[ParameterValueSpec] = (),
        field_wrappers: Optional[dict[str, SelectWrapperSpec]] = None,
        validate_aggregations: bool = True,
    ):
        self._fields = FieldRegistry()
        self._columns = columns
        self._formula_parser = formula_parser
        self._mock_among_dimensions = mock_among_dimensions
        self._suppress_double_aggregations = suppress_double_aggregations
        self._allow_nested_window_functions = allow_nested_window_functions
        self._field_wrappers = field_wrappers or {}
        self._validate_aggregations = validate_aggregations

        self._inspect_env = inspect_env if inspect_env is not None else InspectionEnvironment()
        self._valid_env = ValidationEnvironment()
        self._wrapper_applicator = ExpressionWrapperApplicator()

        for field in all_fields:
            self.register_field(field)

        self._group_by_ids = set(group_by_ids or ())
        self._filter_ids = set(filter_ids or ())
        self._order_by_specs = list(order_by_specs or ())
        self._parameter_value_specs = list(parameter_value_specs or ())

        # initialize caches
        # node/expression caches
        self._formula_parsed_cache: dict[str, formula_nodes.Formula | None] = {}
        self._substituted_formula_fields: NodeValueMap[formula_nodes.FormulaItem] = NodeValueMap()
        # error caches
        self._stage_manager = FieldProcessingStageManager(columns=self._columns, inspect_env=self._inspect_env)
        self._formula_error_cache: dict[str, list[FormulaErrorCtx]] = defaultdict(list)  # FIXME: ???
        # other caches
        self._field_types: dict[str, FieldType] = {}
        self._field_dependencies: dict[str, set[str]] = defaultdict(set)  # fields this field depends on
        self._field_inverse_dependencies: dict[str, set[str]] = defaultdict(set)  # fields that depend on this one

        self.update_environments(group_by_ids=self._group_by_ids, order_by_specs=self._order_by_specs)

    @property
    def columns(self) -> ColumnRegistry:
        return self._columns

    @property
    def inspect_env(self) -> InspectionEnvironment:
        return self._inspect_env

    def register_avatar(self, avatar_id: str, source_id: str) -> None:
        self._columns.register_avatar(avatar_id=avatar_id, source_id=source_id)

    def unregister_avatar(self, avatar_id: str) -> None:
        # Clear all cached values that depend on this avatar
        for field in self._fields:
            if field.calc_mode == CalcMode.direct and field.avatar_id == avatar_id:
                self.uncache_field(field=field)
        self._columns.unregister_avatar(avatar_id=avatar_id)

    def uncache_field(self, field: BIField, visited_guids: AbstractSet[str] = frozenset()) -> None:
        """Clear all field-related caches and caches of dependent fields too"""

        if field.guid in visited_guids:
            # recursion detected
            return
        visited_guids |= frozenset([field.guid])

        # must be fetched before the cleanup starts
        dep_field_guids = self._get_dependent_fields(field).copy()

        # clear own caches
        self._stage_manager.clear(field=field)
        this_field_dependencies = self._field_dependencies.pop(field.guid, set())
        self._field_types.pop(field.guid, None)

        # clear own dependencies
        for child_field_id in this_field_dependencies:
            self._field_inverse_dependencies.pop(child_field_id, None)

        # clear cache for dependent fields
        for dep_field_guid in dep_field_guids:
            dependent_field = self._fields.get(id=dep_field_guid)
            self.uncache_field(dependent_field, visited_guids=visited_guids)

    def unregister_field(self, field: BIField) -> None:
        """Remove field from all collections, mappings and caches"""

        # `field` may have been updated, so get the registered version
        field = self._fields.get(id=field.guid)
        if field is None:
            return

        self.uncache_field(field)  # this uncaches dependencies also

        # clear direct usages
        self._fields.remove(field)
        self._group_by_ids.discard(field.guid)
        found_order_bys = [spec for spec in self._order_by_specs if spec.field_id == field.guid]
        for found_spec in found_order_bys:
            self._order_by_specs.remove(found_spec)

    def register_field(self, field: BIField) -> None:
        """Register field in translation environment"""
        self._fields.add(field)

        if field.calc_mode == CalcMode.direct:
            avatar_id = field.avatar_id
            assert avatar_id is not None
            try:
                av_column = self._columns.get_avatar_column(avatar_id=avatar_id, name=field.source)
            except formula_exc.FormulaError:
                LOGGER.warning(f"Unknown column {field.source} for avatar {avatar_id}")
            else:
                if av_column.column.has_auto_aggregation:
                    # Better to check this property in the column, than in the field
                    # because the field may have been updated, and not all of its attributes are synchronized yet.
                    self._inspect_env.cache_is_aggregate_expr.add(
                        formula_nodes.Field.make(name=av_column.id), value=True
                    )

    def update_field(self, field: BIField) -> None:
        is_in_group_by: bool = False
        if field.guid in self._group_by_ids:
            is_in_group_by = True
        found_order_bys = [  # order is crucial here, so save index along with each entry
            (ind, spec) for ind, spec in enumerate(self._order_by_specs) if spec.field_id == field.guid
        ]

        self.unregister_field(field)
        self.register_field(field)

        # add it back to group by
        if is_in_group_by:
            self._group_by_ids.add(field.guid)
        # put it back in the same positions in order by
        for ind, spec in found_order_bys:
            self._order_by_specs.insert(ind, spec)

    def update_environments(
        self,
        group_by_ids: Collection[str],
        order_by_specs: Sequence[OrderByFieldSpec],
    ) -> None:
        self._group_by_ids = set(group_by_ids)
        self._order_by_specs = list(order_by_specs)

    def _try_parse_formula(
        self,
        field: BIField,
        collect_errors: bool = False,
    ) -> tuple[Optional[formula_nodes.Formula], list[FormulaErrorCtx]]:
        """Attempt to parse formula. If `collect_errors`"""

        formula = field.formula
        formula_obj = self._formula_parsed_cache.get(formula)
        errors = self._formula_error_cache.get(formula, [])

        if formula_obj is None and not errors:  # has not been parsed yet
            try:
                formula_obj = self._formula_parser.parse(formula)
            except formula_exc.ParseError as err:
                if not collect_errors:
                    raise dl_query_processing.exc.FormulaHandlingError(*err.errors) from err
                errors.extend(err.errors)

            self._formula_parsed_cache[formula] = formula_obj
            self._formula_error_cache[formula] = errors

        return formula_obj, errors

    def _is_field_recursive(self, field: BIField) -> bool:
        """Check whether field is recursive or not"""

        def _has_recursion(for_guid: str, visited_guids: AbstractSet[str] = frozenset()) -> bool:
            if for_guid in visited_guids:
                return True
            visited_guids |= frozenset([for_guid])
            for child in self._field_dependencies[for_guid]:
                if _has_recursion(child, visited_guids):
                    return True
            return False

        return _has_recursion(field.guid)

    def _make_dependencies_for_field(
        self,
        *,
        field: BIField,
        formula_obj: formula_nodes.Formula,
        collect_errors: bool = False,
    ) -> None:
        """Build dependency graph"""

        if field.calc_mode != CalcMode.formula:
            return

        if formula_obj is None:
            return

        child_fields = []
        for child_field_node in used_fields(formula_obj):
            if child_field_node.name not in self._fields.titles:
                continue

            child_field = self._fields.get(title=child_field_node.name)
            # register dependencies
            self._field_dependencies[field.guid].add(child_field.guid)
            self._field_inverse_dependencies[child_field.guid].add(field.guid)
            child_fields.append(child_field)

        if self._is_field_recursive(field):
            raise dl_query_processing.exc.FormulaHandlingError(
                FormulaErrorCtx("Recursion detected in field", level=MessageLevel.ERROR),
                field=field,
            )

        # generate dependencies of children
        for child_field in child_fields:
            self._generate_base_formula_obj_for_field(field=child_field, collect_errors=collect_errors)

    def get_referenced_fields(self, field: BIField) -> set[FieldId]:
        return self._field_dependencies[field.guid]

    def _substitute_fields_in_formula(
        self, field: BIField, formula_obj: formula_nodes.Formula, collect_errors: bool = False
    ) -> formula_nodes.Formula:
        to_substitute: dict[NodeHierarchyIndex, formula_nodes.FormulaItem] = {}

        sub_node: formula_nodes.FormulaItem
        for field_node_idx, child_field_node in enumerate_fields(formula_obj):
            if child_field_node.name not in self._fields.titles:
                # Not recognized as a field
                if child_field_node.name in self._columns:
                    # A DB column -> already substituted field
                    continue
                sub_node = formula_aux_nodes.ErrorNode.make(
                    err_code=formula_exc.UnknownFieldInFormulaError.default_code,
                    message=f"Unknown field found in formula: {child_field_node.name}",
                )

            else:
                # Recognized as an unsubstituted field, so substitute it
                child_field = self._fields.get(title=child_field_node.name)

                # Check for and handle double aggregations
                if (
                    self._suppress_double_aggregations
                    # Child has explicit aggregation
                    and child_field.aggregation != AggregationFunction.none
                    # And current field has formula (auto) aggregation
                    and is_aggregated_above_sub_node(node=formula_obj, index=field_node_idx)
                ):
                    # Double aggregation detected,
                    # so generate sub-node only up to the aggregation stage
                    # (`casting` is the previous one)
                    sub_node = self._process_field_stage_casting(child_field, collect_errors=collect_errors).expr

                else:
                    # No ignorable double aggregations, proceed as usual
                    sub_node = self._process_field_stage_aggregation(child_field, collect_errors=collect_errors).expr

                child_wrapper = self._field_wrappers.get(child_field.guid)
                if child_wrapper is not None:
                    sub_node = self._wrapper_applicator.apply_wrapper(sub_node, wrapper=child_wrapper)

            self._substituted_formula_fields.add(sub_node, value=child_field_node)

            to_substitute[field_node_idx] = sub_node

        formula_obj = formula_obj.substitute_batch(to_substitute)
        return formula_obj

    def _generate_formula_obj_for_formula_field(
        self,
        field: BIField,
        collect_errors: bool = False,
    ) -> formula_nodes.Formula:
        """Generate expression for field in formula mode"""
        errors: list[FormulaErrorCtx] = []

        formula_obj, parse_errors = self._try_parse_formula(field=field, collect_errors=collect_errors)
        errors.extend(parse_errors)

        if formula_obj is None:
            assert errors, "Got no formula and no errors. Should have either of the two"
            raise dl_query_processing.exc.FormulaHandlingError(*errors, field=field)

        return formula_obj

    def _generate_formula_obj_for_direct_field(self, field: BIField) -> formula_nodes.Formula:
        """Generate expression for field with direct type"""
        sub_node: formula_nodes.FormulaItem
        avatar_id = field.avatar_id
        assert avatar_id is not None
        try:
            av_column = self._columns.get_avatar_column(avatar_id=avatar_id, name=field.source)
        except formula_exc.FormulaError as err:
            error_info = err.errors[0]
            sub_node = formula_aux_nodes.ErrorNode.make(err_code=error_info.code, message=error_info.message)
        else:
            sub_node = formula_nodes.Field.make(name=av_column.id)
        return formula_nodes.Formula.make(expr=sub_node)

    def _generate_formula_obj_for_parameter_field(self, field: BIField) -> formula_nodes.Formula:
        """Generate expression for field with parameter type"""
        if field.cast not in _ALLOWED_PARAMETER_TYPES:
            raise formula_exc.ParameterUnsupportedTypeError(
                f"Unsupported type {field.cast} for parameter {field.title}"
            )

        value = None
        for spec in self._parameter_value_specs:
            if spec.field_id == field.guid:
                value = spec.value
                break

        if value is None and field.default_value is not None:
            value = field.default_value.value

        if value is None:
            raise formula_exc.ParameterValueError(f"No value can be found for parameter field {field.title}")

        value_constraint = field.value_constraint
        if value_constraint is not None and not value_constraint.is_valid(value):
            raise formula_exc.ParameterValueError(
                f"Invalid parameter value {value!r} for constraint {value_constraint}"
            )

        try:
            return formula_nodes.Formula.make(
                expr=make_literal_node(val=value, data_type=BI_TO_FORMULA_TYPES[enum_not_none(field.cast)])
            )
        except dl_query_processing.exc.InvalidLiteralError as e:
            raise formula_exc.ParameterValueError(f"Invalid parameter value {value!r} for type {field.cast}") from e

    def _generate_base_formula_obj_for_field(
        self,
        field: BIField,
        collect_errors: bool = False,
    ) -> formula_nodes.Formula:
        """Generate raw (without casts, aggregations, substitutions, etc.) formula object for field."""

        if field.calc_mode == CalcMode.direct:
            formula_obj = self._generate_formula_obj_for_direct_field(field=field)
        elif field.calc_mode == CalcMode.parameter:
            formula_obj = self._generate_formula_obj_for_parameter_field(field=field)
        else:
            formula_obj = self._generate_formula_obj_for_formula_field(field=field, collect_errors=collect_errors)

        return formula_obj

    def _get_mock_dimensions_for_among(self, formula_obj: formula_nodes.Formula) -> list[formula_nodes.FormulaItem]:
        """
        Collect dimensions from the AMONG clauses of the formula expression.
        """

        func_list = used_func_calls(formula_obj)
        required_dims: list[formula_nodes.FormulaItem] = []
        for func in func_list:
            if isinstance(func, formula_nodes.WindowFuncCall):
                if isinstance(func.grouping, formula_nodes.WindowGroupingAmong):
                    required_dims.extend(func.grouping.dim_list)

        return required_dims

    def apply_pre_sub_mutations(
        self,
        field: BIField,
        formula_obj: formula_nodes.Formula,
        collect_errors: bool = False,
    ) -> formula_nodes.Formula:
        """
        Apply pre-substitution mutations required for functions to be translated correctly.
        """

        # prepare default ordering (for patching RSUM, MSUM functions and the like)
        default_order_by = []
        ob_expr_obj: formula_nodes.FormulaItem
        for order_by_spec in self._order_by_specs:
            ob_expr_obj = formula_nodes.Field.make(name=self._fields.get(id=order_by_spec.field_id).title)
            if order_by_spec.direction == OrderDirection.desc:
                ob_expr_obj = formula_nodes.OrderDescending.make(expr=ob_expr_obj)
            default_order_by.append(ob_expr_obj)

        mutations = [
            IgnoreParenthesisWrapperMutation(),
            ConvertBlocksToFunctionsMutation(),
            DefaultWindowOrderingMutation(default_order_by=default_order_by),
            LookupDefaultBfbMutation(),
        ]
        formula_obj = apply_mutations(formula_obj, mutations=mutations)

        # Only measures can contain BFB clauses
        title_id_map = {f.title: f.guid for f in self._fields}
        formula_obj = apply_mutations(formula_obj, mutations=[RemapBfbMutation(name_mapping=title_id_map)])

        return formula_obj

    def _apply_function_by_name(self, formula_obj: formula_nodes.Formula, func_name: str) -> formula_nodes.Formula:
        """Apply a single-argument function to given expression"""

        return formula_nodes.Formula.make(expr=formula_nodes.FuncCall.make(name=func_name, args=[formula_obj.expr]))

    def apply_mutations(
        self,
        field: BIField,
        formula_obj: formula_nodes.Formula,
        collect_errors: bool = False,
    ) -> formula_nodes.Formula:
        """
        Apply the mutations required for functions to be translated correctly.
        """

        # prepare mutations
        mutation_lists: list[FormulaMutation] = [OptimizeConstMathOperatorMutation()]
        if self._field_types[field.guid] != FieldType.DIMENSION:
            # prepare global dimensions (for patching AMONG clauses)
            if self._mock_among_dimensions:
                # in mock mode real dimensions are unavailable, so we just use the ones listed in AMONG clauses
                global_dimensions = self._get_mock_dimensions_for_among(formula_obj)
            else:
                global_dimensions = [
                    # We will need to process some other fields to a more advanced stage,
                    # but since they are all dimensions, there will be no recursion loop
                    self._process_field_stage_aggregation(
                        self._fields.get(id=dim_id), collect_errors=collect_errors
                    ).expr
                    for dim_id in sorted(self._group_by_ids)
                ]

            if is_window_expression(node=formula_obj, env=self._inspect_env):
                mutation_lists.extend(
                    [
                        AmongToWithinGroupingMutation(global_dimensions=global_dimensions),
                        IgnoreExtraWithinGroupingMutation(
                            global_dimensions=global_dimensions, inspect_env=self._inspect_env
                        ),
                    ]
                )

        return apply_mutations(formula_obj, mutations=mutation_lists)

    def _apply_aggregation(
        self, formula_obj: formula_nodes.Formula, aggregation: AggregationFunction
    ) -> formula_nodes.Formula:
        """Apply an explicit aggregation to given expression"""

        if aggregation != AggregationFunction.none:
            formula_obj = self._apply_function_by_name(formula_obj, func_name=self._agg_functions[aggregation])

        return formula_obj

    def _apply_cast(
        self,
        formula_obj: formula_nodes.Formula,
        current_dtype: DataType,
        cast: Optional[UserDataType],
    ) -> formula_nodes.Formula:
        """Apply a type cast to given expression"""

        if current_dtype != DataType.NULL:
            expr_type = FORMULA_TO_BI_TYPES[current_dtype]
            if cast is not None and cast != expr_type:
                formula_obj = self._apply_function_by_name(formula_obj, func_name=_CAST_FUNCTIONS[cast])
        return formula_obj

    def apply_cast_to_formula(
        self,
        formula_obj: formula_nodes.Formula,
        current_dtype: DataType,
        cast: Optional[UserDataType],
    ) -> formula_nodes.Formula:
        return self._apply_cast(formula_obj=formula_obj, current_dtype=current_dtype, cast=cast)

    def _validate_field_formula(
        self,
        formula_obj: formula_nodes.Formula,
        field_id: FieldId,
        collect_errors: bool = False,
    ) -> None:
        unselected_dimension_ids = {
            f.guid for f in self._fields if f.type == FieldType.DIMENSION and f.guid not in self._group_by_ids
        }
        checkers: list[Checker] = []
        if self._field_types[field_id] != FieldType.DIMENSION and self._validate_aggregations:
            global_dimensions = []
            for dim_field_id in self._group_by_ids:
                dim_field = self._fields.get(id=dim_field_id)
                try:
                    dim_formula_obj = self._process_field_stage_mutation(dim_field, collect_errors=collect_errors)
                except formula_exc.FormulaError:
                    # error has been registered, so when the field will be rendered explicitly the error will be raised
                    # here we can just skip it since there's nothing to register as a dimension
                    continue
                global_dimensions.append(dim_formula_obj.expr)

            checkers.append(
                AggregationChecker(
                    allow_nested_agg=True,
                    allow_inconsistent_inside_agg=False,
                    valid_env=self._valid_env,
                    inspect_env=self._inspect_env,
                    global_dimensions=global_dimensions,
                )
            )
        checkers.append(
            WindowFunctionChecker(
                inspect_env=self._inspect_env,
                allow_nested=self._allow_nested_window_functions,
                filter_ids=self._filter_ids,
                unselected_dimension_ids=unselected_dimension_ids,
            )
        )
        validate(node=formula_obj, env=self._valid_env, collect_errors=collect_errors, checkers=checkers)

    @implements_stage(ProcessingStage.base)
    def _process_field_stage_base(self, field: BIField, collect_errors: bool = False) -> formula_nodes.Formula:
        return self._generate_base_formula_obj_for_field(field=field, collect_errors=collect_errors)

    @implements_stage(ProcessingStage.pre_sub_mutation)
    def _process_field_stage_pre_sub_mutation(
        self, field: BIField, collect_errors: bool = False
    ) -> formula_nodes.Formula:
        formula_obj = self._process_field_stage_base(field, collect_errors=collect_errors)
        return self.apply_pre_sub_mutations(field=field, formula_obj=formula_obj, collect_errors=collect_errors)

    @implements_stage(ProcessingStage.dep_generation)
    def _process_field_stage_dep_generation(
        self, field: BIField, collect_errors: bool = False
    ) -> formula_nodes.Formula:
        formula_obj = self._process_field_stage_pre_sub_mutation(field, collect_errors=collect_errors)
        self._make_dependencies_for_field(field=field, formula_obj=formula_obj)
        return formula_obj

    @implements_stage(ProcessingStage.substitution)
    def _process_field_stage_substitution(self, field: BIField, collect_errors: bool = False) -> formula_nodes.Formula:
        formula_obj = self._process_field_stage_dep_generation(field, collect_errors=collect_errors)
        return self._substitute_fields_in_formula(field=field, formula_obj=formula_obj, collect_errors=collect_errors)

    @implements_stage(ProcessingStage.casting)
    def _process_field_stage_casting(self, field: BIField, collect_errors: bool = False) -> formula_nodes.Formula:
        formula_obj = self._process_field_stage_substitution(field, collect_errors=collect_errors)
        return self._apply_cast(
            formula_obj=formula_obj,
            current_dtype=self._stage_manager.get_data_type(field=field, stage=ProcessingStage.substitution),
            cast=field.cast,
        )

    @implements_stage(ProcessingStage.aggregation)
    def _process_field_stage_aggregation(self, field: BIField, collect_errors: bool = False) -> formula_nodes.Formula:
        formula_obj = self._process_field_stage_casting(field, collect_errors=collect_errors)
        formula_obj = self._apply_aggregation(formula_obj=formula_obj, aggregation=field.aggregation)
        is_agg = is_aggregate_expression(formula_obj, env=self._inspect_env)
        self._field_types[field.guid] = FieldType.MEASURE if is_agg else FieldType.DIMENSION
        return formula_obj

    @implements_stage(ProcessingStage.mutation)
    def _process_field_stage_mutation(self, field: BIField, collect_errors: bool = False) -> formula_nodes.Formula:
        formula_obj = self._process_field_stage_aggregation(field, collect_errors=collect_errors)
        return self.apply_mutations(field=field, formula_obj=formula_obj, collect_errors=collect_errors)

    @implements_stage(ProcessingStage.validation)
    def _process_field_stage_validation(self, field: BIField, collect_errors: bool = False) -> formula_nodes.Formula:
        formula_obj = self._process_field_stage_mutation(field, collect_errors=collect_errors)
        self._validate_field_formula(formula_obj=formula_obj, field_id=field.guid, collect_errors=collect_errors)
        return formula_obj

    @implements_stage(ProcessingStage.final)
    def _process_field_stage_final(self, field: BIField, collect_errors: bool = False) -> formula_nodes.Formula:
        return self._process_field_stage_validation(field, collect_errors=collect_errors)

    def _compile_field_formula(self, field: BIField, collect_errors: bool = False) -> formula_nodes.Formula:
        return self._process_field_stage_final(field=field, collect_errors=collect_errors)

    @contextmanager
    def handle_formula_error(self, field_id: FieldId) -> Generator[None, None, None]:
        try:
            yield
        except formula_exc.FormulaError as err:
            field = self._fields.get(id=field_id)
            raise dl_query_processing.exc.DLFormulaError(field=field, formula_errors=err.errors) from err

    def _require_field_formula_preparation(self, field: BIField) -> None:
        """Make sure that the field's fully prepared and ready for translation formula is in the cache"""
        try:
            self._compile_field_formula(field, collect_errors=True)
        except formula_exc.FormulaError:
            pass  # ignore errors

    def get_field_validity(self, field: BIField) -> bool:
        """Return boolean flag indicating whether the field is valid."""
        return not self.get_field_errors(field)

    def get_field_initial_data_type(self, field: BIField) -> Optional[UserDataType]:
        """Return automatically determined data type of given field before cast and aggregation"""
        self._require_field_formula_preparation(field)
        return self._stage_manager.get_user_type(field=field, stage=ProcessingStage.substitution)

    def get_field_final_data_type(self, field: BIField) -> Optional[UserDataType]:
        """
        Return automatically determined user data type (``UserDataType``)
        of given field after cast and aggregation
        """
        self._require_field_formula_preparation(field)
        return self._stage_manager.get_user_type(field=field, stage=ProcessingStage.aggregation)

    def get_field_final_formula_data_type(self, field: BIField) -> Optional[DataType]:
        """
        Return automatically determined formula data type (``DataType``)
        of given field after cast and aggregation
        """
        self._require_field_formula_preparation(field)
        return self._stage_manager.get_data_type(field=field, stage=ProcessingStage.aggregation)

    def get_field_type(self, field: BIField) -> FieldType:
        """Return automatically determined field type"""
        self._require_field_formula_preparation(field)
        return self._field_types.get(field.guid, FieldType.DIMENSION)

    def get_field_errors(self, field: BIField) -> list[FormulaErrorCtx]:
        """Return list of errors found for given field"""
        self._require_field_formula_preparation(field)
        return self._stage_manager.get_errors(field)

    def _get_dependent_fields(self, field: BIField) -> set[str]:
        """Return GUIDs of dependent fields"""
        return (self._field_inverse_dependencies.get(field.guid) or set()).copy()

    def get_node_text(self, node: formula_nodes.FormulaItem) -> str:
        text = node.original_text
        if text is None:
            original_node = self._substituted_formula_fields.get(node)
            if original_node:
                text = original_node.original_text
        if text is None:
            raise ValueError(f"Not able to get node text for node {node}")
        return text

    def _compile_binary_relation_condition_formula(
        self, relation: AvatarRelation, condition: BinaryCondition, collect_errors: bool = False
    ) -> formula_nodes.Binary:
        """
        Create a ``FormulaItem`` (to be used as part of a ``Formula``)
        from description of join condition
        """

        def _condition_part_as_field(avatar_id: str, part: ConditionPart) -> BIField:
            """
            Create an auxiliary ``BIField`` object from join condition part
            so that it can be used in ``self._generate_field_expression``
            """
            new_field = True
            if part.calc_mode == ConditionPartCalcMode.result_field:
                assert isinstance(part, ConditionPartResultField)
                field = self._fields.get(id=part.field_id)
                new_field = False
            elif part.calc_mode == ConditionPartCalcMode.direct:
                assert isinstance(part, ConditionPartDirect)
                field = self.make_direct_field(avatar_id=avatar_id, source=part.source)
            elif part.calc_mode == ConditionPartCalcMode.formula:
                # TODO: Remove
                assert isinstance(part, ConditionPartFormula)
                field = self.make_formula_field(formula=part.formula)
            else:
                raise ValueError(f"Invalid part calc_mode {part.calc_mode}")

            # JOINs over aggregated fields are not supported yet
            if new_field and field.has_auto_aggregation or not new_field and field.type == FieldType.MEASURE:
                raise dl_query_processing.exc.DatasetError("Joining over aggregated expressions is not supported")

            return field

        left, right = [
            self._compile_field_formula(
                _condition_part_as_field(avatar_id=avatar_id, part=part), collect_errors=collect_errors
            )
            for (avatar_id, part) in (
                (relation.left_avatar_id, condition.left_part),
                (relation.right_avatar_id, condition.right_part),
            )
        ]
        return formula_nodes.Binary.make(
            name=self._join_condition_operators[condition.operator],
            left=left.expr,
            right=right.expr,
        )

    def compile_relation_formula(
        self,
        relation: AvatarRelation,
        collect_errors: bool = False,
    ) -> CompiledJoinOnFormulaInfo:
        if len(relation.conditions) < 1:
            raise dl_query_processing.exc.FormulaHandlingError(
                FormulaErrorCtx("Relation requires at least one condition", level=MessageLevel.ERROR),
            )
        compile_condition = partial(
            self._compile_binary_relation_condition_formula, relation=relation, collect_errors=collect_errors
        )
        result = compile_condition(condition=relation.conditions[0])
        for condition in relation.conditions[1:]:
            result = formula_nodes.Binary.make(name="and", left=result, right=compile_condition(condition=condition))
        formula_obj = formula_nodes.Formula.make(expr=result)

        left_avatar_id: Optional[AvatarId] = relation.left_avatar_id
        if relation.managed_by == ManagedBy.feature:
            left_avatar_id = None

        formula_info = CompiledJoinOnFormulaInfo(
            formula_obj=formula_obj,
            avatar_ids=self._columns.get_used_avatar_ids_for_formula_obj(formula_obj),
            original_field_id=None,
            left_id=left_avatar_id,  # type: ignore  # TODO: fix
            right_id=relation.right_avatar_id,
            join_type=relation.join_type,
            alias=None,
        )
        return formula_info

    def compile_field_formula(
        self,
        field: BIField,
        collect_errors: bool = False,
        original_field_id: Optional[str] = None,
    ) -> CompiledFormulaInfo:
        formula_obj = self._compile_field_formula(field=field, collect_errors=collect_errors)
        formula_info = CompiledFormulaInfo(
            formula_obj=formula_obj,
            avatar_ids=self._columns.get_used_avatar_ids_for_formula_obj(formula_obj),
            alias=field.guid,
            original_field_id=original_field_id or field.guid,
        )
        return formula_info

    def make_formula_field(self, formula: str) -> BIField:
        field = BIField.make(
            guid=make_field_id(),
            title=str(uuid.uuid4()),
            managed_by=ManagedBy.compiler_runtime,
            calc_spec=FormulaCalculationSpec(
                formula=formula,
            ),
        )
        self.register_field(field)
        return field

    def make_direct_field(self, avatar_id: AvatarId, source: str) -> BIField:
        column = self._columns.get_avatar_column(avatar_id=avatar_id, name=source).column
        field = BIField.make(
            guid=make_field_id(),
            title=str(uuid.uuid4()),
            type=FieldType.MEASURE if column.has_auto_aggregation else FieldType.DIMENSION,
            has_auto_aggregation=column.has_auto_aggregation,
            lock_aggregation=column.lock_aggregation,
            cast=column.user_type,
            initial_data_type=column.user_type,
            data_type=column.user_type,
            managed_by=ManagedBy.compiler_runtime,
            calc_spec=DirectCalculationSpec(
                avatar_id=avatar_id,
                source=source,
            ),
        )
        self.register_field(field)
        return field

    def compile_text_formula(self, formula: str, collect_errors: bool = False) -> CompiledFormulaInfo:
        field = self.make_formula_field(formula=formula)
        formula_obj = self._compile_field_formula(field=field, collect_errors=collect_errors)
        formula_info = CompiledFormulaInfo(
            formula_obj=formula_obj,
            avatar_ids=self._columns.get_used_avatar_ids_for_formula_obj(formula_obj),
            alias=field.guid,
            original_field_id=None,
        )
        return formula_info

    def get_formula_errors(self, formula: str) -> list[FormulaErrorCtx]:
        field = self.make_formula_field(formula=formula)
        errors = self.get_field_errors(field)
        return errors

    def field_has_auto_aggregation(self, field: BIField) -> bool:
        try:
            formula_obj = self._process_field_stage_casting(field=field, collect_errors=False)
            return is_aggregate_expression(formula_obj, env=self._inspect_env)
        except formula_exc.FormulaError:
            return False
