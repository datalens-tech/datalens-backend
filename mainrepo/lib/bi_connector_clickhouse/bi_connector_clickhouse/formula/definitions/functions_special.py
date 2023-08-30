import sqlalchemy as sa

import bi_formula.definitions.functions_special as base
from bi_formula.definitions.base import TranslationVariant

from bi_connector_clickhouse.formula.constants import ClickHouseDialect as D


V = TranslationVariant.make


DEFINITIONS_SPECIAL = [
    # _make_nan
    base.FuncMakeNan(variants=[
        V(D.CLICKHOUSE, lambda: sa.column("cast('NaN' AS Float64)", is_literal=True)),
    ]),
]
