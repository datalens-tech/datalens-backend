from bi_db_testing.connectors.base.connector import DbTestingConnector

from bi_connector_chyt.db_testing.engine_wrapper import CHYTEngineWrapperBase


class CHYTDbTestingConnector(DbTestingConnector):
    engine_wrapper_classes = (
        CHYTEngineWrapperBase,
    )
