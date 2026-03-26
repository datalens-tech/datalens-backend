from dl_db_testing.connectors.base.connector import DbTestingConnector

from dl_connector_starrocks.db_testing.engine_wrapper import (
    BiStarRocksEngineWrapper,
    DLStarRocksEngineWrapper,
)


class StarRocksDbTestingConnector(DbTestingConnector):
    engine_wrapper_classes = (
        DLStarRocksEngineWrapper,
        BiStarRocksEngineWrapper,
    )
