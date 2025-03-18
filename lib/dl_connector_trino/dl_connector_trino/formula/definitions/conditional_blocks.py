# import sqlalchemy as sa

from dl_formula.definitions.base import TranslationVariant
import dl_formula.definitions.conditional_blocks as base

from dl_connector_trino.formula.constants import TrinoDialect as D


V = TranslationVariant.make


DEFINITIONS_COND_BLOCKS = [
    # _case_block_
    base.CaseBlock.for_dialect(D.TRINO),
    # _if_block_
    # base.IfBlock3(
    #     variants=[
    #         V(D.TRINO, sa.func.IF),
    #     ]
    # ),
    base.IfBlock3.for_dialect(D.TRINO),
    base.IfBlockMulti.for_dialect(D.TRINO),
]
