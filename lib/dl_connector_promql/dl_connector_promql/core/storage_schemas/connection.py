from marshmallow import fields as ma_fields

from dl_core.us_manager.storage_schemas.connection import ConnectionSQLDataStorageSchema
from dl_model_tools.schema.dynamic_enum_field import DynamicEnumField

from dl_connector_promql.core.constants import PromQLAuthType
from dl_connector_promql.core.us_connection import PromQLConnection


class PromQLConnectionDataStorageSchema(ConnectionSQLDataStorageSchema[PromQLConnection.DataModel]):
    TARGET_CLS = PromQLConnection.DataModel

    path = ma_fields.String(required=False, allow_none=True, load_default=None, dump_default=None)
    secure = ma_fields.Boolean(required=False, allow_none=False, dump_default=False, load_default=False)
    auth_type = DynamicEnumField(
        PromQLAuthType,
        allow_none=False,
        dump_default=PromQLAuthType.password,
        load_default=PromQLAuthType.password,
    )
    auth_header = ma_fields.String(
        required=False,
        allow_none=True,
        dump_default=None,
        load_default=None,
    )
