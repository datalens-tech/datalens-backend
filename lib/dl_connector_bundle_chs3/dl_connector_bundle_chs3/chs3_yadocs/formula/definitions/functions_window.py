from dl_connector_bundle_chs3.chs3_yadocs.formula.utils import clickhouse_funcs_for_yadocs_dialect
from dl_connector_clickhouse.formula.definitions.functions_window import DEFINITIONS_WINDOW as CH_DEFINITIONS_WINDOW


DEFINITIONS_WINDOW = clickhouse_funcs_for_yadocs_dialect(CH_DEFINITIONS_WINDOW)
