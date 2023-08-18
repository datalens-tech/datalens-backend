from bi_db_testing.connectors.base.connector import DbTestingConnector

from bi_connector_snowflake.db_testing.engine_wrapper import SnowFlakeEngineWrapper


class SnowFlakeDbTestingConnector(DbTestingConnector):
    engine_wrapper_classes = (
        SnowFlakeEngineWrapper,
    )
