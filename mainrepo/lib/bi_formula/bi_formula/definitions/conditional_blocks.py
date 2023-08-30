from __future__ import annotations

import sqlalchemy as sa
from sqlalchemy.sql.elements import Null

from bi_formula.core.dialect import StandardDialect as D
from bi_formula.core.datatype import DataType
from bi_formula.definitions.args import ArgFlagSequence, IfFlagDispenser
from bi_formula.definitions.flags import ContextFlag
from bi_formula.definitions.type_strategy import FromArgs, CaseTypeStrategy, IfTypeStrategy
from bi_formula.definitions.scope import Scope
from bi_formula.definitions.base import (
    MultiVariantTranslation,
    TranslationVariant,
    TranslationVariantWrapped,
)


V = TranslationVariant.make
VW = TranslationVariantWrapped.make


class CondBlock(MultiVariantTranslation):
    is_function = False
    scopes = MultiVariantTranslation.scopes & ~Scope.SUGGESTED


class CaseBlock(CondBlock):
    name = '_case_block_'
    arg_names = ['expression', 'value_1', 'result_1', 'value_2', 'result_2', 'default_result']

    _null_replace_map = {
        DataType.INTEGER: sa.literal(0),
        DataType.FLOAT: sa.literal(0.0),
        DataType.BOOLEAN: sa.literal(0),
        DataType.STRING: sa.literal(''),
        DataType.MARKUP: sa.literal(''),
        # Note that datetime types are not supported in this case
    }

    @staticmethod
    def _ifnull(value, data_type: DataType) -> bool:
        if value is None or isinstance(value, Null):
            return CaseBlock._null_replace_map.get(data_type, sa.null())

        return value

    @classmethod
    def translation(cls, *args, replace_nulls: bool = False, as_multiif: bool = False):
        value_arg, args = args[0], args[1:]
        else_expr, else_type = None, DataType.NULL
        if len(args) % 2 == 1:
            # block has an ELSE
            else_expr, else_type = args[-1].expression, args[-1].data_type
            args = args[:-1]
        when_args = [(args[ind], args[ind+1]) for ind in range(0, len(args), 2)]
        # validate value types (must all be castable to same type)
        # all 'WHEN' values must be the same type as 'CASE' value
        DataType.get_common_cast_type(value_arg.data_type, *[when_value.data_type for when_value, _ in when_args])

        return_type = DataType.get_common_cast_type(
            else_type, *[then_expr.data_type for _, then_expr in when_args])

        def _then_else_wrap(expr):
            if replace_nulls:
                # for ClickHouse only, a workaround for https://st.yandex-team.ru/CLICKHOUSE-4376
                expr = cls._ifnull(expr, data_type=return_type.non_const_type)
            return expr

        if as_multiif and not return_type.is_const:
            # ClickHouse does not support non-constant THENs
            # So use the multiIf function
            return sa.func.multiIf(
                *[
                    # when_1, then_1, when_2, then_2, ...
                    expr
                    for when_value, then_expr in when_args
                    for expr in (
                        value_arg.expression == when_value.expression,
                        _then_else_wrap(then_expr.expression)
                    )
                ],
                _then_else_wrap(else_expr if else_expr is not None else sa.null()),
            )

        # Use CASE syntax in SQL
        whens = {
            when_value.expression: _then_else_wrap(then_expr.expression)
            for when_value, then_expr in when_args
        }

        return sa.case(
            value=value_arg.expression, whens=whens,
            else_=_then_else_wrap(else_expr if else_expr is not None else sa.null())
        )

    arg_cnt = None
    variants = [
        VW(D.DUMMY, lambda *args: CaseBlock.translation(*args)),
    ]
    return_type = CaseTypeStrategy()


class IfBlock(CondBlock):
    name = '_if_block_'
    arg_names = ['condition_1', 'result_1', 'condition_2', 'result_2', 'default_result']


class IfBlock3(IfBlock):
    arg_cnt = 3
    variants = [
        V(D.DUMMY, lambda cond, expr, else_expr: sa.case(whens=[(cond, expr)], else_=else_expr)),
    ]
    argument_flags = ArgFlagSequence([ContextFlag.REQ_CONDITION, None, None])
    return_type = FromArgs(1, 2)


class IfBlockMulti(IfBlock):
    arg_cnt = None
    variants = [V(D.DUMMY | D.SQLITE, lambda *args: sa.case(
        whens=[(args[ind], args[ind+1]) for ind in range(0, len(args)-1, 2)],
        else_=args[-1]
    ))]
    argument_flags = IfFlagDispenser()
    return_type = IfTypeStrategy()


DEFINITIONS_COND_BLOCKS = [
    # _case_block_
    CaseBlock,
    # _if_block_
    IfBlock3,
    IfBlockMulti,
]
