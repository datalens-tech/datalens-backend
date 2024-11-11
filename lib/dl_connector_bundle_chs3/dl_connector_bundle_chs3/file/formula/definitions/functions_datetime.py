from dl_connector_bundle_chs3.file.formula.utils import clickhouse_funcs_for_file_dialect
from dl_connector_clickhouse.formula.definitions.functions_datetime import (
    DEFINITIONS_DATETIME as CH_DEFINITIONS_DATETIME,
)


DEFINITIONS_DATETIME = clickhouse_funcs_for_file_dialect(CH_DEFINITIONS_DATETIME)
