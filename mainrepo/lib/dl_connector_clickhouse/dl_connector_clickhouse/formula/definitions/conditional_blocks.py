import sqlalchemy as sa

from dl_formula.definitions.base import (
    TranslationVariant,
    TranslationVariantWrapped,
)
import dl_formula.definitions.conditional_blocks as base

from dl_connector_clickhouse.formula.constants import ClickHouseDialect as D

V = TranslationVariant.make
VW = TranslationVariantWrapped.make


DEFINITIONS_COND_BLOCKS = [
    # _case_block_
    base.CaseBlock(
        variants=[
            VW(D.CLICKHOUSE, lambda *args: base.CaseBlock.translation(*args, as_multiif=True, replace_nulls=True)),
        ]
    ),
    # _if_block_
    base.IfBlock3(
        variants=[
            V(D.CLICKHOUSE, getattr(sa.func, "if")),
        ]
    ),
    base.IfBlockMulti.for_dialect(D.CLICKHOUSE),
]
