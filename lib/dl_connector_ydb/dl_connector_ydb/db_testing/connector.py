from dl_db_testing.connectors.base.connector import DbTestingConnector

from dl_connector_ydb.db_testing.engine_wrapper import YQLEngineWrapper


class YQLDbTestingConnector(DbTestingConnector):
    engine_wrapper_classes = (YQLEngineWrapper,)
