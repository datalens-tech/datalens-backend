import re

import sqlalchemy as sa
from sqlalchemy.sql.elements import ClauseElement

from dl_formula.core.nodes import LiteralString
from dl_formula.definitions.base import (
    TranslationVariant,
    TranslationVariantWrapped,
)
import dl_formula.definitions.functions_native as base
from dl_formula.translation.context import TranslationCtx

from dl_connector_ydb.formula.constants import YqlDialect as D


VW = TranslationVariantWrapped.make


def _call_native_impl_yql(func_name_ctx: TranslationCtx, *args: TranslationCtx) -> ClauseElement:
    assert isinstance(func_name_ctx.node, LiteralString)
    func_name = func_name_ctx.node.value

    # Validate function name
    if re.match(r"^[a-zA-Z0-9_]+::[a-zA-Z0-9_]+$", func_name):
        namespace, function = func_name.split("::")

        namespace = getattr(sa.func, namespace)
        function = getattr(namespace, function)

        return function(*(arg.expression for arg in args))

    return base._call_native_impl(func_name_ctx, *args)


yql_variants: list[TranslationVariant] = [
    VW(
        D.YQL,
        _call_native_impl_yql,
    )
]

DEFINITIONS_NATIVE = [
    base.DBCallInt(variants=yql_variants),
    base.DBCallFloat(variants=yql_variants),
    base.DBCallString(variants=yql_variants),
    base.DBCallBool(variants=yql_variants),
    base.DBCallArrayInt(variants=yql_variants),
    base.DBCallArrayFloat(variants=yql_variants),
    base.DBCallArrayString(variants=yql_variants),
    base.DBCallAggInt(variants=yql_variants),
    base.DBCallAggFloat(variants=yql_variants),
    base.DBCallAggString(variants=yql_variants),
]
