from dl_connector_bundle_chs3.file.formula.utils import clickhouse_funcs_for_file_dialect
from dl_connector_clickhouse.formula.definitions.conditional_blocks import (
    DEFINITIONS_COND_BLOCKS as CH_DEFINITIONS_COND_BLOCKS,
)


DEFINITIONS_COND_BLOCKS = clickhouse_funcs_for_file_dialect(CH_DEFINITIONS_COND_BLOCKS)
