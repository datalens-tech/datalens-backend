from dl_db_testing.connectors.base.connector import DbTestingConnector

from dl_connector_clickhouse.db_testing.engine_wrapper import (
    BiClickHouseEngineWrapper,
    ClickHouseEngineWrapper,
)


class ClickHouseDbTestingConnector(DbTestingConnector):
    engine_wrapper_classes = (
        ClickHouseEngineWrapper,
        BiClickHouseEngineWrapper,
    )
