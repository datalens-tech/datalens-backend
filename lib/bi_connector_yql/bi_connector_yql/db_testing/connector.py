from dl_db_testing.connectors.base.connector import DbTestingConnector

from bi_connector_yql.db_testing.engine_wrapper import YQLEngineWrapper


class YQLDbTestingConnector(DbTestingConnector):
    engine_wrapper_classes = (YQLEngineWrapper,)
