import sqlalchemy as sa

from dl_formula.definitions.base import TranslationVariant
import dl_formula.definitions.conditional_blocks as base

from dl_connector_bigquery.formula.constants import BigQueryDialect as D

V = TranslationVariant.make


DEFINITIONS_COND_BLOCKS = [
    # _case_block_
    base.CaseBlock.for_dialect(D.BIGQUERY),
    # _if_block_
    base.IfBlock3(
        variants=[
            V(D.BIGQUERY, sa.func.IF),
        ]
    ),
    base.IfBlockMulti.for_dialect(D.BIGQUERY),
]
