from dl_connector_bundle_chs3.chs3_yadocs.formula.utils import clickhouse_funcs_for_yadocs_dialect
from dl_connector_clickhouse.formula.definitions.operators_binary import DEFINITIONS_BINARY as CH_DEFINITIONS_BINARY


DEFINITIONS_BINARY = clickhouse_funcs_for_yadocs_dialect(CH_DEFINITIONS_BINARY)
