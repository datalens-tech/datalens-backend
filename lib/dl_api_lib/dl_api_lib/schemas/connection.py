from __future__ import annotations

import logging
from typing import (
    Any,
    ClassVar,
)

from marshmallow import fields as ma_fields
from marshmallow_oneofschema import OneOfSchema

from dl_api_connector.api_schema.connection_base import (
    ConnectionMetaMixin,
    ConnectionSchema,
)
from dl_api_connector.api_schema.source import DataSourceSchema
from dl_api_connector.api_schema.source_base import (
    DataSourceTemplateResponseField,
    RawSchemaColumnSchema,
)
from dl_api_lib.schemas.main import NotificationContentSchema
from dl_constants.enums import ConnectionType as CT
from dl_core.us_connection_base import (
    ConnectionBase,
    UnknownConnection,
)
from dl_model_tools.schema.base import BaseSchema


LOGGER = logging.getLogger(__name__)


class ConnectionSourcesQuerySchema(BaseSchema):
    search_text = ma_fields.String(required=False, load_default=None)
    limit = ma_fields.Integer(required=False, load_default=10_000)


class ConnectionItemQuerySchema(BaseSchema):
    rev_id = ma_fields.String()


class ConnectionInfoSourceSchemaQuerySchema(BaseSchema):
    source = ma_fields.Nested(DataSourceSchema, required=True)


class ConnectionSourceTemplatesResponseSchema(BaseSchema):
    # # Complete:
    # sources = ma_fields.Nested(DataSourceTemplateResponseSchema, many=True)
    # freeform_sources = ma_fields.Nested(DataSourceTemplateResponseSchema, many=True)
    # # Minimal processing:
    sources = ma_fields.List(DataSourceTemplateResponseField)
    freeform_sources = ma_fields.List(DataSourceTemplateResponseField)


class ConnectionInfoSourceSchemaResponseSchema(BaseSchema):
    raw_schema = ma_fields.Nested(RawSchemaColumnSchema, many=True, allow_none=True)


class ConnectionExportResponseSchema(BaseSchema):
    connection = ma_fields.Raw(required=True)
    notifications = ma_fields.Nested(NotificationContentSchema, many=True)


class ConnectionDataContentImportSchema(BaseSchema):
    connection = ma_fields.Raw(required=True)
    workbook_id = ma_fields.String()


class ConnectionImportRequestSchema(BaseSchema):
    data = ma_fields.Nested(ConnectionDataContentImportSchema, required=True)


class GenericConnectionSchema(OneOfSchema):
    type_schemas: dict[str, type[ConnectionSchema]] = {}  # type: ignore  # 2024-01-24 # TODO: Incompatible types in assignment (expression has type "dict[str, type[ConnectionSchema]]", base class "OneOfSchema" defined the type as "dict[str, type[Schema]]")  [assignment]
    supported_connections: ClassVar[set[type[ConnectionBase]]] = set()

    def get_data_type(self, data):  # type: ignore  # 2024-01-30 # TODO: Function is missing a type annotation  [no-untyped-def]
        data_type = super().get_data_type(data)
        self.context[ConnectionSchema.CONN_TYPE_CTX_KEY] = data_type
        return data_type

    @classmethod
    def is_supported_connection(cls, conn: ConnectionBase) -> bool:
        return type(conn) in cls.supported_connections

    @classmethod
    def is_supported_type_discriminator(cls, type_discriminator: str) -> bool:
        return type_discriminator in cls.type_schemas

    def get_obj_type(self, obj: ConnectionBase) -> str:
        return obj.conn_type.name

    def get_edit_schema_cls(self, obj: ConnectionBase) -> type[ConnectionSchema]:
        """
        Returns schema for connection editing
        :param obj: connection to modify
        :return: Schema class that will modify connection on `load`
        """
        type_discriminator = self.get_obj_type(obj)
        return self.type_schemas[type_discriminator]

    # TODO FIX: remove after type discriminator key will be normalized now we use 'db_type' in GET and 'type' in POST
    def _dump(self, obj: ConnectionBase, *, update_fields: bool = True, **kwargs: Any) -> dict[str, Any]:
        ret = super()._dump(obj, update_fields=update_fields, **kwargs)
        ret.pop(self.type_field)
        return ret


def register_sub_schema_class(conn_type: CT, schema_cls: type[ConnectionSchema]) -> None:
    if conn_type.name in GenericConnectionSchema.type_schemas:
        registered_schema = GenericConnectionSchema.type_schemas[conn_type.name]
        assert (
            registered_schema == schema_cls
        ), f"Already have {registered_schema} serving {conn_type}, can not register {schema_cls} for it"

    GenericConnectionSchema.type_schemas[conn_type.name] = schema_cls
    GenericConnectionSchema.supported_connections.add(schema_cls.TARGET_CLS)


class UnknownConnectionSchema(ConnectionMetaMixin, ConnectionSchema):
    TARGET_CLS = UnknownConnection


register_sub_schema_class(CT.unknown, UnknownConnectionSchema)
