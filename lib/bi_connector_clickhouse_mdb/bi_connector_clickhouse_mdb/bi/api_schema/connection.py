from bi_api_connector.api_schema.connection_mixins import MDBDatabaseSchemaMixin

from bi_connector_clickhouse.bi.api_schema.connection import ClickHouseConnectionSchema


class ClickHouseMDBConnectionSchema(ClickHouseConnectionSchema, MDBDatabaseSchemaMixin):
    pass
