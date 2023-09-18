from dl_connector_snowflake.db_testing.engine_wrapper import SnowFlakeEngineWrapper
from dl_db_testing.connectors.base.connector import DbTestingConnector


class SnowFlakeDbTestingConnector(DbTestingConnector):
    engine_wrapper_classes = (SnowFlakeEngineWrapper,)
