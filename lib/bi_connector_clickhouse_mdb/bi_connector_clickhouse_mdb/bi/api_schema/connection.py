from dl_connector_clickhouse.bi.api_schema.connection import ClickHouseConnectionSchema

from bi_connector_mdb_base.bi.api_schema.connection_mixins import MDBDatabaseSchemaMixin

from bi_connector_clickhouse_mdb.core.us_connection import ConnectionClickhouseMDB


class ClickHouseMDBConnectionSchema(MDBDatabaseSchemaMixin, ClickHouseConnectionSchema):
    TARGET_CLS = ConnectionClickhouseMDB
