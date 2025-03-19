import sqlalchemy as sa

from dl_formula.definitions.base import TranslationVariant
import dl_formula.definitions.functions_special as base

from dl_connector_trino.formula.constants import TrinoDialect as D


V = TranslationVariant.make


DEFINITIONS_SPECIAL = [
    # _make_nan
    base.FuncMakeNan(
        variants=[
            V(D.TRINO, sa.func.nan),
        ]
    ),
]
