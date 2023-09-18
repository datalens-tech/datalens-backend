from dl_connector_postgresql.formula.constants import PostgreSQLDialect as D
import dl_formula.definitions.conditional_blocks as base

DEFINITIONS_COND_BLOCKS = [
    # _case_block_
    base.CaseBlock.for_dialect(D.POSTGRESQL),
    # _if_block_
    base.IfBlock3.for_dialect(D.POSTGRESQL),
    base.IfBlockMulti.for_dialect(D.POSTGRESQL),
]
