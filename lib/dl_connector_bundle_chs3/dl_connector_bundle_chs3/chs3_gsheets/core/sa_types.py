from dl_connector_bundle_chs3.chs3_gsheets.core.constants import BACKEND_TYPE_GSHEETS_V2
from dl_connector_clickhouse.core.clickhouse_base.sa_types import SQLALCHEMY_CLICKHOUSE_TYPES

SQLALCHEMY_GSHEETS_V2_TYPES = {
    (BACKEND_TYPE_GSHEETS_V2, nt): sa_type for (_, nt), sa_type in SQLALCHEMY_CLICKHOUSE_TYPES.items()
}
