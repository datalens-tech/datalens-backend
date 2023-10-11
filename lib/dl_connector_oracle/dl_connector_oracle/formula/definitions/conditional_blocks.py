from dl_formula.definitions.base import TranslationVariant
import dl_formula.definitions.conditional_blocks as base

from dl_connector_oracle.formula.constants import OracleDialect as D


V = TranslationVariant.make


DEFINITIONS_COND_BLOCKS = [
    # _case_block_
    base.CaseBlock.for_dialect(D.ORACLE),
    # _if_block_
    base.IfBlock3.for_dialect(D.ORACLE),
    base.IfBlockMulti.for_dialect(D.ORACLE),
]
