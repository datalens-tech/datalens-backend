from dl_constants.enums import (
    ConnectionType,
    DataSourceType,
)

from dl_connector_clickhouse.core.clickhouse_base.constants import BACKEND_TYPE_CLICKHOUSE


BACKEND_TYPE_CHYT = BACKEND_TYPE_CLICKHOUSE
CONNECTION_TYPE_CHYT = ConnectionType.declare("chyt")

SOURCE_TYPE_CHYT_YTSAURUS_TABLE = DataSourceType.declare("CHYT_YTSAURUS_TABLE")
SOURCE_TYPE_CHYT_YTSAURUS_SUBSELECT = DataSourceType.declare("CHYT_YTSAURUS_SUBSELECT")
SOURCE_TYPE_CHYT_YTSAURUS_TABLE_LIST = DataSourceType.declare("CHYT_YTSAURUS_TABLE_LIST")
SOURCE_TYPE_CHYT_YTSAURUS_TABLE_RANGE = DataSourceType.declare("CHYT_YTSAURUS_TABLE_RANGE")
