from dl_connector_greenplum.core.storage_schemas.connection import GreenplumConnectionDataStorageSchema

from bi_connector_greenplum_mdb.core.us_connection import GreenplumMDBConnection
from bi_connector_mdb_base.core.storage_schemas import ConnectionMDBStorageDataSchemaMixin


class GreenplumMDBConnectionDataStorageSchema(
    ConnectionMDBStorageDataSchemaMixin,
    GreenplumConnectionDataStorageSchema,
):
    TARGET_CLS = GreenplumMDBConnection.DataModel
