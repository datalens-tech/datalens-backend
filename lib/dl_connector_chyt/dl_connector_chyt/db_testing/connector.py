from dl_connector_chyt.db_testing.engine_wrapper import CHYTEngineWrapperBase
from dl_db_testing.connectors.base.connector import DbTestingConnector


class CHYTDbTestingConnector(DbTestingConnector):
    engine_wrapper_classes = (CHYTEngineWrapperBase,)
