from __future__ import annotations

import datetime
import logging
import os
from typing import (
    Any,
    Callable,
    ClassVar,
    Dict,
    Optional,
    TypeVar,
)

import attr

from bi_constants.enums import WhereClauseOperation
from bi_core.fields import BIField
from bi_formula.core.datatype import DataType
import bi_formula.core.nodes as formula_nodes
from bi_formula.shortcuts import n
from bi_query_processing.compilation.formula_compiler import (
    FORMULA_TO_BI_TYPES,
    FormulaCompiler,
)
from bi_query_processing.compilation.helpers import (
    ARRAY_TYPES,
    TREE_TYPES,
    make_literal_node,
)
from bi_query_processing.compilation.primitives import CompiledFormulaInfo
from bi_query_processing.compilation.specs import (
    FilterFieldSpec,
    FilterSourceColumnSpec,
)
import bi_query_processing.exc
from bi_query_processing.utils.datetime import parse_datetime

LOGGER = logging.getLogger(__name__)

USE_DATE_TO_DATETIME_CONV = os.environ.get("USE_DATE_TO_DATETIME_CONV", "1") == "1"


_FILTER_PARAMS_TV = TypeVar("_FILTER_PARAMS_TV", bound="FilterParams")

CONTAINMENT_OPS = {
    WhereClauseOperation.STARTSWITH,
    WhereClauseOperation.ISTARTSWITH,
    WhereClauseOperation.CONTAINS,
    WhereClauseOperation.ICONTAINS,
    WhereClauseOperation.NOTCONTAINS,
    WhereClauseOperation.NOTICONTAINS,
    WhereClauseOperation.ENDSWITH,
    WhereClauseOperation.IENDSWITH,
}
ARRAY_LEN_OPS = {
    WhereClauseOperation.LENEQ,
    WhereClauseOperation.LENNE,
    WhereClauseOperation.LENGT,
    WhereClauseOperation.LENGTE,
    WhereClauseOperation.LENLT,
    WhereClauseOperation.LENLTE,
}


@attr.s(frozen=True)
class FilterDefinition:
    arg_cnt: Optional[int] = attr.ib(kw_only=True)
    callable: Callable[..., formula_nodes.FormulaItem] = attr.ib(kw_only=True)


@attr.s(auto_attribs=True)
class FilterParams:
    """
    Single-field filter state,
    primarily for mangling in subclasses.
    """

    field: BIField
    operation: WhereClauseOperation
    filter_args: list = None  # type: ignore  # TODO: fix
    data_type: DataType = None  # type: ignore  # TODO: fix
    field_cast_type: DataType = None  # type: ignore  # TODO: fix
    arg_cast_type: DataType = None  # type: ignore  # TODO: fix

    def clone(self: _FILTER_PARAMS_TV, **updates: Any) -> _FILTER_PARAMS_TV:
        """Convenience method so that callers don't need to know about `attr`"""
        return attr.evolve(self, **updates)


@attr.s
class FilterFormulaCompiler:
    _formula_compiler: FormulaCompiler = attr.ib(kw_only=True)

    # structure: {WhereClauseOperation: (add_arg_cnt, expr_callable)}
    FILTER_OPERATIONS: ClassVar[Dict[WhereClauseOperation, FilterDefinition]] = {
        # unary (arg_cnt = 0)
        WhereClauseOperation.ISNULL: FilterDefinition(arg_cnt=0, callable=n.func.ISNULL),
        WhereClauseOperation.ISNOTNULL: FilterDefinition(arg_cnt=0, callable=lambda f, *args: n.not_(n.func.ISNULL(f))),
        # binary (arg_cnt = 1)
        WhereClauseOperation.EQ: FilterDefinition(arg_cnt=1, callable=lambda f, val: n.binary("==", f, val)),
        WhereClauseOperation.NE: FilterDefinition(arg_cnt=1, callable=lambda f, val: n.binary("!=", f, val)),
        WhereClauseOperation.GT: FilterDefinition(arg_cnt=1, callable=lambda f, val: n.binary(">", f, val)),
        WhereClauseOperation.GTE: FilterDefinition(arg_cnt=1, callable=lambda f, val: n.binary(">=", f, val)),
        WhereClauseOperation.LT: FilterDefinition(arg_cnt=1, callable=lambda f, val: n.binary("<", f, val)),
        WhereClauseOperation.LTE: FilterDefinition(arg_cnt=1, callable=lambda f, val: n.binary("<=", f, val)),
        WhereClauseOperation.STARTSWITH: FilterDefinition(arg_cnt=1, callable=n.func.STARTSWITH),
        WhereClauseOperation.ISTARTSWITH: FilterDefinition(arg_cnt=1, callable=n.func.ISTARTSWITH),
        WhereClauseOperation.ENDSWITH: FilterDefinition(arg_cnt=1, callable=n.func.ENDSWITH),
        WhereClauseOperation.IENDSWITH: FilterDefinition(arg_cnt=1, callable=n.func.IENDSWITH),
        WhereClauseOperation.CONTAINS: FilterDefinition(arg_cnt=1, callable=n.func.CONTAINS),
        WhereClauseOperation.ICONTAINS: FilterDefinition(arg_cnt=1, callable=n.func.ICONTAINS),
        WhereClauseOperation.NOTCONTAINS: FilterDefinition(
            arg_cnt=1, callable=lambda f, val: n.not_(n.func.CONTAINS(f, val))
        ),
        WhereClauseOperation.NOTICONTAINS: FilterDefinition(
            arg_cnt=1, callable=lambda f, val: n.not_(n.func.ICONTAINS(f, val))
        ),
        WhereClauseOperation.LENEQ: FilterDefinition(
            arg_cnt=1, callable=lambda f, val: n.binary("==", n.func.LEN(f), val)
        ),
        WhereClauseOperation.LENNE: FilterDefinition(
            arg_cnt=1, callable=lambda f, val: n.binary("!=", n.func.LEN(f), val)
        ),
        WhereClauseOperation.LENGT: FilterDefinition(
            arg_cnt=1, callable=lambda f, val: n.binary(">", n.func.LEN(f), val)
        ),
        WhereClauseOperation.LENGTE: FilterDefinition(
            arg_cnt=1, callable=lambda f, val: n.binary(">=", n.func.LEN(f), val)
        ),
        WhereClauseOperation.LENLT: FilterDefinition(
            arg_cnt=1, callable=lambda f, val: n.binary("<", n.func.LEN(f), val)
        ),
        WhereClauseOperation.LENLTE: FilterDefinition(
            arg_cnt=1, callable=lambda f, val: n.binary("<=", n.func.LEN(f), val)
        ),
        # binary with list (arg_cnt = None)
        # None for arg_cnt means that all args are converted to a list,
        # which is used as a single argument for binary operation
        WhereClauseOperation.IN: FilterDefinition(arg_cnt=None, callable=lambda f, val: n.binary("in", f, val)),
        WhereClauseOperation.NIN: FilterDefinition(arg_cnt=None, callable=lambda f, val: n.binary("notin", f, val)),
        # ternary (arg_cnt = 2)
        WhereClauseOperation.BETWEEN: FilterDefinition(
            arg_cnt=2, callable=lambda f, second, third: n.ternary("between", f, second, third)
        ),
    }

    def _custom_filter_cast(self, filter_params: FilterParams) -> FilterParams:
        """
        Customize the parameters for a field filtering, e.g. change data type
        to which the field and filter arg values need to be cast.
        """
        return filter_params

    def compile_abstract_field_filter_formula(
        self,
        field: BIField,
        operation: WhereClauseOperation,
        filter_args: Optional[list] = None,
        original_field_id: Optional[str] = None,
        anonymous: bool = False,
    ) -> CompiledFormulaInfo:
        field_formula_obj = self._formula_compiler.compile_field_formula(field, collect_errors=False).formula_obj

        data_type = self._formula_compiler.get_field_final_formula_data_type(field=field)
        assert data_type is not None
        LOGGER.info(f"Filtered field {field.title!r} has data type {data_type.name} and is a {field.type.name}")

        filter_params = FilterParams(
            field=field,
            operation=operation,
            filter_args=filter_args,  # type: ignore  # TODO: fix
            data_type=data_type,
            # defaults:
            field_cast_type=data_type,
            arg_cast_type=data_type,
        )
        mangled_filter_params = self._custom_filter_cast(filter_params)
        assert mangled_filter_params.field == field, "not meant to be changed for now"
        operation = mangled_filter_params.operation
        filter_args = mangled_filter_params.filter_args
        assert mangled_filter_params.data_type == data_type, "not meant to be changed for now"
        field_cast_type = mangled_filter_params.field_cast_type
        arg_cast_type = mangled_filter_params.arg_cast_type

        LOGGER.info(
            f"Will cast field {field.title!r} to {field_cast_type.name} " f"and {filter_args!r} to {arg_cast_type.name}"
        )
        if field_cast_type != data_type:
            field_formula_obj = self._formula_compiler.apply_cast_to_formula(
                formula_obj=field_formula_obj,
                current_dtype=data_type,
                cast=FORMULA_TO_BI_TYPES[field_cast_type],
            )

        def _remove_timezone(arg: Any) -> Any:
            if isinstance(arg, str):
                return arg.removesuffix("Z")
            if isinstance(arg, datetime.datetime):
                return arg.replace(tzinfo=None)
            return arg

        # Determine and, if necessary, convert data types of the filter arguments
        args_nodes = []
        remove_timezone = data_type in (DataType.GENERICDATETIME, DataType.CONST_GENERICDATETIME)
        for arg in filter_args or ():
            if remove_timezone:
                arg = _remove_timezone(arg)
            # we assume that filter arguments all have the same data type as the filtered field
            try:
                arg_lit_node = make_literal_node(val=arg, data_type=arg_cast_type)
            except bi_query_processing.exc.InvalidLiteralError as e:
                raise bi_query_processing.exc.FilterValueError(
                    f"Invalid filter value {arg!r} for type {arg_cast_type.name}"
                ) from e
            args_nodes.append(arg_lit_node)

        # Translate the whole expression
        # 1. Get filter translation info
        try:
            filter_def = self.FILTER_OPERATIONS[operation]
            add_arg_cnt = filter_def.arg_cnt
            expr_callable = filter_def.callable
        except KeyError:
            raise ValueError(operation)

        # 2. Make list of args for function call
        if add_arg_cnt is None:  # for list lookup operations (IN, NOT IN)
            args = [formula_nodes.ExpressionList.make(*args_nodes)]
        else:
            args = args_nodes[:add_arg_cnt]  # type: ignore  # TODO: fix

        # 3. Create formula object
        formula_obj = formula_nodes.Formula.make(expr=expr_callable(field_formula_obj.expr, *args))

        original_field_id = original_field_id or field.guid
        if anonymous:
            original_field_id = None

        formula_info = CompiledFormulaInfo(
            formula_obj=formula_obj,
            avatar_ids=self._formula_compiler._columns.get_used_avatar_ids_for_formula_obj(formula_obj),  # FIXME
            original_field_id=original_field_id,
            alias=None,
        )
        return formula_info

    def compile_filter_formula(
        self,
        filter_spec: FilterFieldSpec,
        original_field_id: Optional[str] = None,
    ) -> CompiledFormulaInfo:
        """
        Prepare a filter expression for given ``field_id`` using given filter ``operation``.
        The result can be used in a ``WHERE`` or ``HAVING`` clause of a ``SELECT`` statement.

        :param field_id: ID of field to create filter for
        :param operation: one of the filter operations/functions defined in ``WhereClauseOperation``
        :param filter_args: additional arguments for the filter function
        :return:
        """
        field = self._formula_compiler._fields.get(id=filter_spec.field_id)  # FIXME: access to protected member
        return self.compile_abstract_field_filter_formula(
            field=field,
            operation=filter_spec.operation,
            filter_args=filter_spec.values,
            original_field_id=original_field_id,
            anonymous=filter_spec.anonymous,
        )

    def compile_source_column_filter_formula(
        self,
        source_column_filter_spec: FilterSourceColumnSpec,
        original_field_id: Optional[str] = None,
    ) -> CompiledFormulaInfo:
        """
        Prepare a formula based on a source column filter spec
        """
        field = self._formula_compiler.make_direct_field(
            avatar_id=source_column_filter_spec.avatar_id,
            source=source_column_filter_spec.column_name,
        )
        return self.compile_abstract_field_filter_formula(
            field=field,
            operation=source_column_filter_spec.operation,
            filter_args=source_column_filter_spec.values,
            original_field_id=original_field_id,
        )


class MainFilterFormulaCompiler(FilterFormulaCompiler):
    def _mangle_date_filter(self, filter_params: FilterParams) -> FilterParams:
        """
        Generally, date filters receive datetime arguments like '2017-01-01T00:00:00';

        casting them to date might change equality
        (`2017-01-01 < 2017-01-01T00:00:01`; but, `2017-01-01 == date(2017-01-01T00:00:01)`)

        Easy fix is to cast the *field* to datetime
        (`2017-01-01T00:00:00 < 2017-01-01T00:00:01`);

        However, this hurts database performance in some cases

        so, if the filter arg dt is at midnight, cast it to date,
        without cast-wrapping the field itself.
        """

        if filter_params.data_type != DataType.DATE or not filter_params.filter_args:
            return filter_params

        try:
            parsed_args = [parse_datetime(value) for value in filter_params.filter_args]
        except ValueError:
            # not a valid value, cannot check midnight-ness, and it should probably fail later anyway.
            parsed_args = []
            is_midnight = [False for _ in filter_params.filter_args]
        else:
            # Precision note: False for '2017-01-01T00:00:00.000001', True for '2017-01-01T00:00:00.0000001'
            # Timezone note: intentionally ignoring the tzinfo, i.e.
            # '2017-01-01T00:00:00' and '2017-01-01T00:00:00+03:00' are treated the
            # same for date (and naive-datetime) filtering.
            is_midnight = [
                arg.hour == 0 and arg.minute == 0 and arg.second == 0 and arg.microsecond == 0 for arg in parsed_args
            ]

        filter_as_date = False

        if all(is_midnight):
            # `date_col <cmp> 2020-01-01T00:00:00` is equivalent to
            # `date_col <cmp> 2020-01-01`
            filter_as_date = True
        elif filter_params.operation == WhereClauseOperation.GT and len(parsed_args) == 1:
            # `date_col > 2020-01-01T01:00:00` is equivalent to
            # `date_col > 2020-01-01`
            filter_as_date = True
        elif filter_params.operation == WhereClauseOperation.GTE and len(parsed_args) == 1:
            # `date_col >= 2020-01-01T01:00:00` is equivalent to
            # `date_col >  2020-01-01`
            filter_as_date = True
            filter_params = filter_params.clone(operation=WhereClauseOperation.GT)
        elif filter_params.operation == WhereClauseOperation.LTE and len(parsed_args) == 1:
            # `date_col <= 2020-01-01T01:00:00` is equivalent to
            # `date_col <= 2020-01-01`
            filter_as_date = True
        elif filter_params.operation == WhereClauseOperation.LT and len(parsed_args) == 1:
            # `date_col <  2020-01-01T01:00:00` is equivalent to
            # `date_col <= 2020-01-01`
            filter_as_date = True
            filter_params = filter_params.clone(operation=WhereClauseOperation.LTE)
        elif filter_params.operation == WhereClauseOperation.BETWEEN and len(parsed_args) == 2:
            # equivalent to `date_col >= arg_from and date_col <= arg_to`,
            # but prefer to keep it as `between`
            arg_from, arg_to = parsed_args
            midnight_from, _ = is_midnight
            if not midnight_from:
                # `date_col between 2020-01-01T01:00:00 and ...` equivalent to `date_col between 2020-01-02 and ...`
                arg_from += datetime.timedelta(days=1)
            parsed_args = [arg_from, arg_to]
            filter_as_date = True

        if filter_as_date:
            new_args = [arg.date().isoformat() for arg in parsed_args]
            filter_params = filter_params.clone(
                filter_args=new_args,
                arg_cast_type=DataType.DATE,  # actually unchanged, just making it explicit.
            )
        else:
            # Cast both sides to datetime.
            filter_params = filter_params.clone(
                field_cast_type=DataType.DATETIME,
                arg_cast_type=DataType.DATETIME,
            )
        if filter_params.operation in CONTAINMENT_OPS:
            # Cast both sides to string.
            filter_params = filter_params.clone(
                field_cast_type=DataType.STRING,
                arg_cast_type=DataType.STRING,
            )
        return filter_params

    def _mangle_containment_filter(self, filter_params: FilterParams) -> FilterParams:
        if filter_params.operation in CONTAINMENT_OPS:
            if filter_params.data_type in ARRAY_TYPES or filter_params.data_type in TREE_TYPES:
                if filter_params.operation in (WhereClauseOperation.CONTAINS, WhereClauseOperation.NOTCONTAINS):
                    arg_cast_type = {
                        DataType.ARRAY_FLOAT: DataType.FLOAT,
                        DataType.ARRAY_INT: DataType.INTEGER,
                        DataType.ARRAY_STR: DataType.STRING,
                        DataType.TREE_STR: DataType.STRING,
                    }[filter_params.data_type]
                    filter_params = filter_params.clone(arg_cast_type=arg_cast_type)
                else:
                    pass  # Fall back to default behavior - cast to the same type as the field
            else:
                filter_params = filter_params.clone(arg_cast_type=DataType.STRING)
        return filter_params

    def _mangle_array_filter(self, filter_params: FilterParams) -> FilterParams:
        if filter_params.operation in ARRAY_LEN_OPS:
            filter_params = filter_params.clone(arg_cast_type=DataType.INTEGER)
        return filter_params

    def _custom_filter_cast(self, filter_params: FilterParams) -> FilterParams:
        filter_params = self._mangle_containment_filter(filter_params)
        filter_params = self._mangle_array_filter(filter_params)
        if USE_DATE_TO_DATETIME_CONV:
            filter_params = self._mangle_date_filter(filter_params)
        return filter_params
