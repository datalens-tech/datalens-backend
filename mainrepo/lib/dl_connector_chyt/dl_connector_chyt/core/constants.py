from dl_connector_clickhouse.core.clickhouse_base.constants import BACKEND_TYPE_CLICKHOUSE
from dl_constants.enums import (
    ConnectionType,
    CreateDSFrom,
)

BACKEND_TYPE_CHYT = BACKEND_TYPE_CLICKHOUSE
CONNECTION_TYPE_CHYT = ConnectionType.declare("chyt")

SOURCE_TYPE_CHYT_YTSAURUS_TABLE = CreateDSFrom.declare("CHYT_YTSAURUS_TABLE")
SOURCE_TYPE_CHYT_YTSAURUS_SUBSELECT = CreateDSFrom.declare("CHYT_YTSAURUS_SUBSELECT")
SOURCE_TYPE_CHYT_YTSAURUS_TABLE_LIST = CreateDSFrom.declare("CHYT_YTSAURUS_TABLE_LIST")
SOURCE_TYPE_CHYT_YTSAURUS_TABLE_RANGE = CreateDSFrom.declare("CHYT_YTSAURUS_TABLE_RANGE")
