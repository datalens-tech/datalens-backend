from dl_connector_bundle_chs3.file.formula.utils import clickhouse_funcs_for_file_dialect
from dl_connector_clickhouse.formula.definitions.functions_aggregation import DEFINITIONS_AGG as CH_DEFINITIONS_AGG


DEFINITIONS_AGG = clickhouse_funcs_for_file_dialect(CH_DEFINITIONS_AGG)
