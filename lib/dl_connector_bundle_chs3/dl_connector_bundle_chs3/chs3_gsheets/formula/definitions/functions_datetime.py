from dl_connector_bundle_chs3.chs3_gsheets.formula.utils import clickhouse_funcs_for_gsheets_v2_dialect
from dl_connector_clickhouse.formula.definitions.functions_datetime import (
    DEFINITIONS_DATETIME as CH_DEFINITIONS_DATETIME,
)


DEFINITIONS_DATETIME = clickhouse_funcs_for_gsheets_v2_dialect(CH_DEFINITIONS_DATETIME)
