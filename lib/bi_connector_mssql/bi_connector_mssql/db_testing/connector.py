from dl_db_testing.connectors.base.connector import DbTestingConnector

from bi_connector_mssql.db_testing.engine_wrapper import MSSQLEngineWrapper


class MSSQLDbTestingConnector(DbTestingConnector):
    engine_wrapper_classes = (MSSQLEngineWrapper,)
