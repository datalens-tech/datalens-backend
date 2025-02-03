from dl_db_testing.connectors.base.connector import DbTestingConnector

from dl_connector_trino.db_testing.engine_wrapper import TrinoEngineWrapper


class TrinoDbTestingConnector(DbTestingConnector):
    engine_wrapper_classes = (TrinoEngineWrapper,)
