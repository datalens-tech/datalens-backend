import sqlalchemy as sa

from dl_formula.definitions.base import TranslationVariant
import dl_formula.definitions.conditional_blocks as base

from dl_connector_snowflake.formula.constants import SnowFlakeDialect as D


V = TranslationVariant.make


DEFINITIONS_COND_BLOCKS = [
    # _case_block_
    base.CaseBlock.for_dialect(D.SNOWFLAKE),
    # _if_block_
    base.IfBlock3(
        variants=[
            V(D.SNOWFLAKE, sa.func.IFF),
        ]
    ),
    base.IfBlockMulti.for_dialect(D.SNOWFLAKE),
]
