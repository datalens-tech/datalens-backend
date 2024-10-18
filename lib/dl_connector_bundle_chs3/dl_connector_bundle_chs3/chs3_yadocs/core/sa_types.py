from dl_connector_bundle_chs3.chs3_yadocs.core.constants import BACKEND_TYPE_YADOCS
from dl_connector_clickhouse.core.clickhouse_base.sa_types import SQLALCHEMY_CLICKHOUSE_TYPES


SQLALCHEMY_YADOCS_TYPES = {
    (BACKEND_TYPE_YADOCS, nt): sa_type for (_, nt), sa_type in SQLALCHEMY_CLICKHOUSE_TYPES.items()
}
