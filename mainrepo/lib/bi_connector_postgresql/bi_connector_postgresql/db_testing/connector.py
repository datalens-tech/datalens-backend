from bi_db_testing.connectors.base.connector import DbTestingConnector

from bi_connector_postgresql.db_testing.engine_wrapper import (
    BiPGEngineWrapper,
    PGEngineWrapper,
)


class PostgreSQLDbTestingConnector(DbTestingConnector):
    engine_wrapper_classes = (
        PGEngineWrapper,
        BiPGEngineWrapper,
    )
