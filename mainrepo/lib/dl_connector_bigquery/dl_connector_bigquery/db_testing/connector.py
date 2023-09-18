from dl_db_testing.connectors.base.connector import DbTestingConnector

from dl_connector_bigquery.db_testing.engine_wrapper import BigQueryEngineWrapper


class BigQueryDbTestingConnector(DbTestingConnector):
    engine_wrapper_classes = (BigQueryEngineWrapper,)
