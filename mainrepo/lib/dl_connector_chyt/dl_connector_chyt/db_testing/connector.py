from dl_db_testing.connectors.base.connector import DbTestingConnector

from dl_connector_chyt.db_testing.engine_wrapper import CHYTEngineWrapperBase


class CHYTDbTestingConnector(DbTestingConnector):
    engine_wrapper_classes = (CHYTEngineWrapperBase,)
