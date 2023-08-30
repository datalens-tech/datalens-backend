from __future__ import annotations

from typing import Dict, Optional

import pytest
import sqlalchemy as sa

import bi_formula.core.exc as exc
from bi_formula.core.aux_nodes import ErrorNode
from bi_formula.core.datatype import DataType
from bi_formula.core.dialect import StandardDialect as D
from bi_formula.core.nodes import (
    Formula, Field, FuncCall,
    Null, Binary, LiteralInteger, LiteralString,
    ParenthesizedExpr, OrderDescending, NodeMeta
)
from bi_formula.core.position import Position
from bi_formula.definitions.registry import OPERATION_REGISTRY
from bi_formula.definitions.scope import Scope
from bi_formula.translation.context import TranslationCtx
from bi_formula.translation.env import TranslationEnvironment
from bi_formula.translation.translator import translate, translate_and_compile

FIELD_TYPES = {
    'f1': DataType.FLOAT,
    'n1': DataType.INTEGER,
    'n2': DataType.INTEGER,
    'n3': DataType.INTEGER,
    'field with spaces': DataType.INTEGER,
    'b1': DataType.BOOLEAN,
    'b2': DataType.BOOLEAN,
    's1': DataType.STRING,
    's2': DataType.STRING,
    'd1': DataType.DATE,
    'd2': DataType.DATE,
}


def T(
        x,
        dialect=D.DUMMY,
        restrict_funcs=False,
        field_types: Dict[str, DataType] = None,
        table_name: str = None,
        env: Optional[TranslationEnvironment] = None,
):
    field_types = field_types or FIELD_TYPES
    if table_name:
        field_names = {f: (table_name, f) for f in field_types}
    else:
        field_names = {f: (f,) for f in field_types}
    return translate_and_compile(
        Formula(x), env=env, dialect=dialect, restrict_funcs=restrict_funcs,
        field_types=field_types, field_names=field_names,
    )


def test_translate_error_node():
    with pytest.raises(exc.TranslationError) as exc_info:
        T(ErrorNode.make(err_code=exc.InconsistentAggregationError.default_code, message='Hello'))

    err_ctx = exc_info.value.errors[0]
    assert err_ctx.code == exc.InconsistentAggregationError.default_code
    assert err_ctx.message == 'Hello'


def test_translate_parenthesized_expressions():
    assert T(ParenthesizedExpr.make(expr=LiteralInteger.make(123)), dialect=D.DUMMY) == '123'


def test_translate_descending():
    assert T(OrderDescending.make(expr=LiteralInteger.make(123)), dialect=D.DUMMY) == '123 DESC'
    assert T(OrderDescending.make(expr=OrderDescending.make(expr=LiteralInteger.make(123))), dialect=D.DUMMY) == '123'


def test_operator_null_error():
    with pytest.raises(exc.TranslationError):
        T(Binary.make('==', Field.make('n1'), Null()), dialect=D.DUMMY)


def test_translate_multiple_errors():
    try:
        translate(
            Formula(FuncCall.make(name='GREATEST', args=[
                FuncCall.make(name='UNKNOWN_1', args=[], meta=NodeMeta(position=Position(4, 100, 0, 0, 4, 100))),
                Field.make(name='Unknown F', meta=NodeMeta(position=Position(104, 200, 0, 0, 104, 200))),
            ])),
            dialect=D.DUMMY, collect_errors=True
        )
        raised = False
    except exc.TranslationError as err:
        raised = True
        assert len(err.errors) == 2
        assert err.errors[0].token == 'UNKNOWN_1'
        assert err.errors[0].position.start == 4
        assert err.errors[0].code == ('FORMULA', 'TRANSLATION', 'UNKNOWN_FUNCTION')
        assert err.errors[1].token == 'Unknown F'
        assert err.errors[1].position.start == 104
        assert err.errors[1].code == ('FORMULA', 'TRANSLATION', 'UNKNOWN_FIELD')

    assert raised


def test_internal_functions():
    env = TranslationEnvironment(dialect=D.DUMMY, required_scopes=Scope.EXPLICIT_USAGE)
    with pytest.raises(exc.TranslationError) as exc_info:
        T(
            FuncCall.make(name='__str', args=[LiteralString.make('qwerty')]),
            env=env, restrict_funcs=True,
        )

    assert exc_info.value.errors[0].code == exc.TranslationUnknownFunctionError.default_code


def test_internal_function_registration():
    for func_key, func_tr in OPERATION_REGISTRY.items():
        name = func_key[0]
        if name.startswith('__'):
            assert func_tr.scopes & ~Scope.EXPLICIT_USAGE == func_tr.scopes, (
                'Functions with names starting with "__" must be registered as internal'
            )


def test_translation_cache():
    sub_node = Field.make(name='qwerty')
    env = TranslationEnvironment(dialect=D.DUMMY, required_scopes=Scope.EXPLICIT_USAGE)
    env.translation_cache.add(
        node=sub_node,
        value=TranslationCtx(
            required_scopes=env.required_scopes,
            node=sub_node,
            data_type=DataType.INTEGER,
            expression=sa.literal(123),
        ),
    )
    assert T(FuncCall.make(name='+', args=[Field.make('qwerty'), LiteralInteger.make(456)]), env=env) == (
        '123 + 456'
    )


def test_translation_replacements():
    sub_node = Field.make(name='qwerty')
    env = TranslationEnvironment(dialect=D.DUMMY, required_scopes=Scope.EXPLICIT_USAGE)
    env.replacements.add(
        node=sub_node,
        value=TranslationCtx(
            node=sub_node,
            data_type=DataType.INTEGER,
            expression=sa.literal(123),
        ),
    )
    assert T(FuncCall.make(name='+', args=[Field.make('qwerty'), LiteralInteger.make(456)]), env=env) == (
        '123 + 456'
    )
