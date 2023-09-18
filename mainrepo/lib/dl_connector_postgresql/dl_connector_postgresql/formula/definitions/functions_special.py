import sqlalchemy as sa

from dl_connector_postgresql.formula.constants import PostgreSQLDialect as D
from dl_formula.definitions.base import TranslationVariant
import dl_formula.definitions.functions_special as base

V = TranslationVariant.make


DEFINITIONS_SPECIAL = [
    # _make_nan
    base.FuncMakeNan(
        variants=[
            V(D.POSTGRESQL, lambda: sa.column("double precision 'NaN'", is_literal=True)),
        ]
    ),
]
