from typing import Optional

from dl_constants.enums import (
    ConnectionType,
    DataSourceType,
)
from dl_core import connection_models
from dl_core.connectors.base.connector import (
    CoreBackendDefinition,
    CoreConnectionDefinition,
    CoreConnector,
    CoreSourceDefinition,
)
from dl_core.us_connection_base import ConnectionBase
from dl_core.us_manager.storage_schemas.connection import BaseConnectionDataStorageSchema
from dl_type_transformer.type_transformer import TypeTransformer


CONNECTION_TYPE_TESTING = ConnectionType.declare("testing")
SOURCE_TYPE_TESTING = DataSourceType.declare("TESTING")


class TestingConnection(ConnectionBase):
    source_type = SOURCE_TYPE_TESTING

    def get_conn_dto(self) -> connection_models.ConnDTO:
        raise NotImplementedError

    @property
    def cache_ttl_sec_override(self) -> Optional[int]:
        return 0


class TestingTypeTransformer(TypeTransformer):
    pass


class TestingConnectionDataStorageSchema(BaseConnectionDataStorageSchema):
    TARGET_CLS = TestingConnection.DataModel


class TestingCoreSourceDefinition(CoreSourceDefinition):
    source_type = SOURCE_TYPE_TESTING


class TestingCoreConnectionDefinition(CoreConnectionDefinition):
    connection_cls = TestingConnection
    conn_type = CONNECTION_TYPE_TESTING
    type_transformer_cls = TestingTypeTransformer
    us_storage_schema_cls = TestingConnectionDataStorageSchema
    dialect_string = "testing"  # No such dialect


class TestingCoreConnector(CoreConnector):
    backend_definition = CoreBackendDefinition
    connection_definitions = (TestingCoreConnectionDefinition,)
    source_definitions = (TestingCoreSourceDefinition,)
