from dl_db_testing.connectors.base.connector import DbTestingConnector

from dl_connector_oracle.db_testing.engine_wrapper import OracleEngineWrapper


class OracleDbTestingConnector(DbTestingConnector):
    engine_wrapper_classes = (OracleEngineWrapper,)
