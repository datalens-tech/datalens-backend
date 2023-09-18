from dl_db_testing.connectors.base.connector import DbTestingConnector

from bi_connector_mysql.db_testing.engine_wrapper import (
    BiMySQLEngineWrapper,
    MySQLEngineWrapper,
)


class MySQLDbTestingConnector(DbTestingConnector):
    engine_wrapper_classes = (
        MySQLEngineWrapper,
        BiMySQLEngineWrapper,
    )
