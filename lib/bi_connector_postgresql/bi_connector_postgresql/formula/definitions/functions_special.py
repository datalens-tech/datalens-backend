import sqlalchemy as sa

from bi_connector_postgresql.formula.constants import PostgreSQLDialect as D
import bi_formula.definitions.functions_special as base
from bi_formula.definitions.base import TranslationVariant


V = TranslationVariant.make


DEFINITIONS_SPECIAL = [
    # _make_nan
    base.FuncMakeNan(variants=[
        V(D.POSTGRESQL, lambda: sa.column("double precision 'NaN'", is_literal=True)),
    ]),
]
