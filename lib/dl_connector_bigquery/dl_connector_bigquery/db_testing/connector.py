from dl_connector_bigquery.db_testing.engine_wrapper import BigQueryEngineWrapper
from dl_db_testing.connectors.base.connector import DbTestingConnector


class BigQueryDbTestingConnector(DbTestingConnector):
    engine_wrapper_classes = (BigQueryEngineWrapper,)
