from dl_connector_bundle_chs3.file.core.constants import BACKEND_TYPE_FILE
from dl_connector_clickhouse.core.clickhouse_base.sa_types import SQLALCHEMY_CLICKHOUSE_TYPES


SQLALCHEMY_FILE_TYPES = {(BACKEND_TYPE_FILE, nt): sa_type for (_, nt), sa_type in SQLALCHEMY_CLICKHOUSE_TYPES.items()}
