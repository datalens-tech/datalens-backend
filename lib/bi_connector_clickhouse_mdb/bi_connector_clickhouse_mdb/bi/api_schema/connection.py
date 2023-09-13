from bi_connector_mdb_base.bi.api_schema.connection_mixins import MDBDatabaseSchemaMixin

from bi_connector_clickhouse.bi.api_schema.connection import ClickHouseConnectionSchema


class ClickHouseMDBConnectionSchema(MDBDatabaseSchemaMixin, ClickHouseConnectionSchema):
    pass
