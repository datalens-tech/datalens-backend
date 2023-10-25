from dl_db_testing.connectors.base.connector import DbTestingConnector

from dl_connector_mysql.db_testing.engine_wrapper import DLMYSQLEngineWrapper


class MySQLDbTestingConnector(DbTestingConnector):
    engine_wrapper_classes = (DLMYSQLEngineWrapper,)
