import sqlalchemy as sa

from dl_formula.definitions.base import TranslationVariant
import dl_formula.definitions.functions_logical as base

from dl_connector_snowflake.formula.constants import SnowFlakeDialect as D

V = TranslationVariant.make


DEFINITIONS_LOGICAL = [
    # case
    base.FuncCase.for_dialect(D.SNOWFLAKE),
    # if
    base.FuncIf(
        variants=[
            V(D.SNOWFLAKE, sa.func.IFF),
        ]
    ),
    # ifnull
    base.FuncIfnull(
        variants=[
            V(D.SNOWFLAKE, sa.func.IFNULL),
        ]
    ),
    # isnan
    base.FuncIsnan(
        variants=[
            V(D.SNOWFLAKE, sa.func.IS_NAN),
        ]
    ),
    # isnull
    base.FuncIsnull.for_dialect(D.SNOWFLAKE),
    # zn
    base.FuncZn(
        variants=[
            V(D.SNOWFLAKE, lambda x: sa.func.IFNULL(x, 0)),
        ]
    ),
]
