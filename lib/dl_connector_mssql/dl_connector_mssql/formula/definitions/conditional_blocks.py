import sqlalchemy as sa

from dl_formula.definitions.base import TranslationVariant
import dl_formula.definitions.conditional_blocks as base

from dl_connector_mssql.formula.constants import MssqlDialect as D


V = TranslationVariant.make


DEFINITIONS_COND_BLOCKS = [
    # _case_block_
    base.CaseBlock.for_dialect(D.MSSQLSRV),
    # _if_block_
    base.IfBlock3(
        variants=[
            V(D.MSSQLSRV, sa.func.IIF),
        ]
    ),
    base.IfBlockMulti.for_dialect(D.MSSQLSRV),
]
