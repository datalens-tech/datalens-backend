from __future__ import annotations

import copy
import datetime
import re
import time
from typing import (
    Any,
    Collection,
    Optional,
    Sequence,
    Type,
    Union,
)

import attr
import sqlalchemy as sa
from sqlalchemy.sql.elements import ClauseElement

from dl_formula.core.datatype import DataType
from dl_formula.core.dialect import DialectCombo
from dl_formula.core.nodes import Formula
from dl_formula.definitions.flags import ContextFlag
from dl_formula.definitions.scope import Scope
import dl_formula.inspect.expression
from dl_formula.mutation.mutation import (
    FormulaMutation,
    apply_mutations,
)
from dl_formula.mutation.optimization import OptimizeConstMathOperatorMutation
from dl_formula.mutation.window import (
    AmongToWithinGroupingMutation,
    DefaultWindowOrderingMutation,
)
from dl_formula.parser.factory import (
    ParserType,
    get_parser,
)
from dl_formula.translation.ext_nodes import CompiledExpression
from dl_formula.translation.translator import (
    TranslationCtx,
    translate,
)
from dl_formula_testing.database import Db
from dl_formula_testing.forced_literal import forced_literal_use  # noqa


FIELD_TYPES = {
    # for the above table
    "id": DataType.INTEGER,
    "int_value": DataType.INTEGER,
    "date_value": DataType.DATE,
    "datetime_value": DataType.DATETIME,
    "str_value": DataType.STRING,
    "str_null_value": DataType.STRING,
    "arr_int_value": DataType.ARRAY_INT,
    "arr_float_value": DataType.ARRAY_FLOAT,
    "arr_str_value": DataType.ARRAY_STR,
    # for NULL data
    "int_null": DataType.INTEGER,
    "float_null": DataType.FLOAT,
    "bool_null": DataType.BOOLEAN,
    "str_null": DataType.STRING,
    "date_null": DataType.DATE,
    "datetime_null": DataType.DATETIME,
    "geopoint_null": DataType.GEOPOINT,
    "geopolygon_null": DataType.GEOPOLYGON,
    "uuid_null": DataType.UUID,
}


@attr.s
class DbEvaluator:
    db: Db = attr.ib(kw_only=True)
    attempts: int = attr.ib(kw_only=True, default=1)
    retry_on_exceptions: Collection[tuple[Type[Exception], re.Pattern]] = attr.ib(kw_only=True, default=())
    retry_delay: int = attr.ib(kw_only=True, default=5)

    @property
    def dialect(self) -> DialectCombo:
        return self.db.dialect

    def translate_formula(
        self,
        formula: str | Formula,
        context_flags: Optional[int] = None,
        other_fields: Optional[dict] = None,
        collect_errors: Optional[bool] = None,
        field_types: Optional[Sequence[DataType]] = None,
        group_by: Optional[list[str | Formula]] = None,
        order_by: Optional[list[str | Formula]] = None,
        required_scopes: int = Scope.EXPLICIT_USAGE,
    ) -> TranslationCtx:
        other_fields = other_fields or {}
        parser = get_parser(ParserType.antlr_py)
        if field_types is None:
            field_types = FIELD_TYPES  # type: ignore  # 2024-01-30 # TODO: Incompatible types in assignment (expression has type "dict[str, DataType]", variable has type "Sequence[DataType] | None")  [assignment]
        if isinstance(formula, str):
            formula = parser.parse(formula)
        if isinstance(formula, Formula):
            # substitute other fields
            for field_node_idx, field_node in dl_formula.inspect.expression.enumerate_fields(copy.copy(formula)):
                if field_node.name in other_fields:
                    formula = formula.replace_at_index(
                        index=field_node_idx,
                        expr=CompiledExpression.make(self.translate_formula((other_fields[field_node.name]))),
                    )

            # mutate
            mutations: list[FormulaMutation] = [OptimizeConstMathOperatorMutation()]
            group_by_objs = [parser.parse(expr).expr for expr in (group_by or ())]  # type: ignore  # 2024-01-30 # TODO: Argument 1 to "parse" of "FormulaParser" has incompatible type "str | Formula"; expected "str"  [arg-type]
            mutations.append(AmongToWithinGroupingMutation(global_dimensions=group_by_objs))
            if order_by is not None:
                order_by_objs = [parser.parse(expr).expr for expr in order_by]  # type: ignore  # 2024-01-30 # TODO: Argument 1 to "parse" of "FormulaParser" has incompatible type "str | Formula"; expected "str"  [arg-type]
                mutations.append(DefaultWindowOrderingMutation(default_order_by=order_by_objs))  # type: ignore  # 2024-01-30 # TODO: Argument 1 to "append" of "list" has incompatible type "DefaultWindowOrderingMutation"; expected "AmongToWithinGroupingMutation"  [arg-type]
            formula = apply_mutations(formula, mutations=mutations)

            # translate
            formula = translate(  # type: ignore  # 2024-01-30 # TODO: Incompatible types in assignment (expression has type "TranslationCtx", variable has type "str | Formula")  [assignment]
                formula=formula,
                dialect=self.dialect,
                field_types=field_types,  # type: ignore  # 2024-01-30 # TODO: Argument "field_types" to "translate" has incompatible type "Sequence[DataType] | None"; expected "dict[str, DataType] | None"  [arg-type]
                context_flags=context_flags,
                collect_errors=collect_errors,
                required_scopes=required_scopes,
            )
        if not isinstance(formula, TranslationCtx):
            raise TypeError(type(formula))

        return formula

    @staticmethod
    def print_as_example(formula: Union[str, Formula], result: Any) -> None:
        if isinstance(result, datetime.date):
            result_str = "#{}#".format(str(result))
        elif isinstance(result, bool):
            result_str = str(result).upper()
        else:
            result_str = repr(result)
        print("{},".format(repr("{} = {}".format(formula, result_str))))

    def eval(  # type: ignore  # 2024-01-29 # TODO: Function is missing a return type annotation  [no-untyped-def]
        self,
        formula: Union[str, Formula],
        from_: Optional[ClauseElement] = None,
        where: str | Formula | None = None,
        many: bool = False,
        other_fields: Optional[dict] = None,
        order_by: Optional[list[str | Formula]] = None,
        group_by: Optional[list[str | Formula]] = None,
        first: bool = False,
        required_scopes: int = Scope.EXPLICIT_USAGE,
    ):
        select_ctx = self.translate_formula(
            formula,
            other_fields=other_fields,
            order_by=order_by,
            group_by=group_by,
            required_scopes=required_scopes,
        )

        def convert(val):  # type: ignore  # 2024-01-29 # TODO: Function is missing a type annotation  [no-untyped-def]
            # a workaround for missing boolean type in DBs and no coercion to bool
            if select_ctx.data_type in (DataType.CONST_BOOLEAN, DataType.BOOLEAN) and val in (0, 1):
                val = bool(val)
            return val

        query = sa.select([select_ctx.expression])
        if from_ is not None:
            query = query.select_from(from_)
        if order_by is not None:
            query = query.order_by(
                *[self.translate_formula(expr, required_scopes=required_scopes).expression for expr in order_by]
            )
        if group_by:
            query = query.group_by(
                *[self.translate_formula(expr, required_scopes=required_scopes).expression for expr in group_by]
            )
        if where is not None:
            where_ctx = self.translate_formula(
                where,
                context_flags=ContextFlag.REQ_CONDITION,
                required_scopes=required_scopes,
            )
            query = query.where(where_ctx.expression)
        if first:
            query = query.limit(1)

        try:
            print("QUERY:", self.db.expr_as_str(query))
            for attempt in range(self.attempts):
                try:
                    if many:
                        return [convert(row[0]) for row in self.db.execute(query).fetchall()]
                    else:
                        result = convert(self.db.execute(query).scalar())
                        return result
                except Exception as exc:
                    exc_str = str(exc)
                    if attempt < self.attempts - 1 and (
                        any(
                            isinstance(exc, match_exc) and match_patt.search(exc_str)
                            for match_exc, match_patt in self.retry_on_exceptions
                        )
                    ):
                        time.sleep(self.retry_delay)
                        continue
                    raise

        except Exception:
            print("QUERY:", self.db.expr_as_str(query))
            raise
