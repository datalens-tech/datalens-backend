from dl_connector_clickhouse.db_testing.engine_wrapper import (
    BiClickHouseEngineWrapper,
    ClickHouseEngineWrapper,
)
from dl_db_testing.connectors.base.connector import DbTestingConnector


class ClickHouseDbTestingConnector(DbTestingConnector):
    engine_wrapper_classes = (
        ClickHouseEngineWrapper,
        BiClickHouseEngineWrapper,
    )
