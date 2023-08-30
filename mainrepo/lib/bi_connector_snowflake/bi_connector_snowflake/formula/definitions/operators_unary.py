import bi_formula.definitions.operators_unary as base
from bi_formula.definitions.base import TranslationVariant

from bi_connector_snowflake.formula.constants import SnowFlakeDialect as D


V = TranslationVariant.make


DEFINITIONS_UNARY = [
    # isfalse
    base.UnaryIsFalseStringGeo.for_dialect(D.SNOWFLAKE),
    base.UnaryIsFalseNumbers.for_dialect(D.SNOWFLAKE),
    base.UnaryIsFalseDateTime.for_dialect(D.SNOWFLAKE),
    base.UnaryIsFalseBoolean(
        variants=[
            V(D.SNOWFLAKE, lambda x: x == False),  # noqa: E712
        ]
    ),

    # istrue
    base.UnaryIsTrueStringGeo.for_dialect(D.SNOWFLAKE),
    base.UnaryIsTrueNumbers.for_dialect(D.SNOWFLAKE),
    base.UnaryIsTrueDateTime.for_dialect(D.SNOWFLAKE),
    base.UnaryIsTrueBoolean(
        variants=[
            V(D.SNOWFLAKE, lambda x: x == True),  # noqa: E712
        ]
    ),

    # neg
    base.UnaryNegate.for_dialect(D.SNOWFLAKE),

    # not
    base.UnaryNotBool.for_dialect(D.SNOWFLAKE),
    base.UnaryNotNumbers.for_dialect(D.SNOWFLAKE),
    base.UnaryNotStringGeo.for_dialect(D.SNOWFLAKE),
    base.UnaryNotDateDatetime.for_dialect(D.SNOWFLAKE),
]
