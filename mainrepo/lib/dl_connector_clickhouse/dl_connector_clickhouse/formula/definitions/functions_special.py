import sqlalchemy as sa

from dl_connector_clickhouse.formula.constants import ClickHouseDialect as D
from dl_formula.definitions.base import TranslationVariant
import dl_formula.definitions.functions_special as base


V = TranslationVariant.make


DEFINITIONS_SPECIAL = [
    # _make_nan
    base.FuncMakeNan(
        variants=[
            V(D.CLICKHOUSE, lambda: sa.column("cast('NaN' AS Float64)", is_literal=True)),
        ]
    ),
]
