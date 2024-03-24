from __future__ import annotations

from typing import Any

import marshmallow.fields as ma_fields

from dl_constants.enums import DataSourceType
from dl_core.base_models import InternalMaterializationConnectionRef
from dl_core.data_source.type_mapping import get_data_source_class
from dl_core.data_source_spec.base import DataSourceSpec
from dl_core.data_source_spec.sql import (
    SQLDataSourceSpecBase,
    StandardSchemaSQLDataSourceSpec,
    StandardSQLDataSourceSpec,
    SubselectDataSourceSpec,
)
from dl_core.marshmallow import FrozenSetField
from dl_core.us_manager.storage_schemas.base import (
    BaseStorageSchema,
    CtxKey,
)
from dl_core.us_manager.storage_schemas.connection_ref_field import ConnectionRefField
from dl_core.us_manager.storage_schemas.index_info import DataSourceIndexInfoStorageSchema
from dl_core.us_manager.storage_schemas.raw_schema import DataSourceRawSchemaEntryStorageSchema
from dl_model_tools.schema.dynamic_enum_field import DynamicEnumField


_DSRC_TOPLEVEL_PARAMS = ("connection_id", "created_from", "raw_schema")


class DataSourceSpecStorageSchema(BaseStorageSchema):  # noqa
    TARGET_CLS = DataSourceSpec

    connection_id = ConnectionRefField(allow_none=True, load_default=None, attribute="connection_ref")
    data_dump_id = ma_fields.String(allow_none=True)
    raw_schema = ma_fields.List(
        ma_fields.Nested(DataSourceRawSchemaEntryStorageSchema),
        load_default=list,
        allow_none=True,
    )
    created_from = DynamicEnumField(DataSourceType, attribute="source_type")

    def pre_process_input_data(self, data: dict[str, Any]) -> dict:
        data = data.copy()
        if "parameters" in data:
            data = {
                **{k: v for k, v in data.items() if k != "parameters"},
                **data["parameters"],
            }
        return data

    def post_process_output_data(self, data: dict[str, Any]) -> dict[str, Any]:  # noqa
        # TODO: Remove. Temporary hack for compatibility with old schemas:
        #  Pack all non-standard keys into "parameters" sub-dict
        parameters = {}
        for param_key in data.copy():
            if param_key not in _DSRC_TOPLEVEL_PARAMS:
                param_value = data.pop(param_key)
                if param_value is not None:  # FIXME: Remove
                    # Save only non-None values here
                    parameters[param_key] = param_value
        data["parameters"] = parameters
        return data

    def push_ctx(self, data: dict) -> None:
        dsrc_cls = get_data_source_class(DataSourceType[data["created_from"]])
        self.context[CtxKey.ds_conn_type] = dsrc_cls.conn_type

    def pop_ctx(self, data: dict) -> None:
        self.context.pop(CtxKey.ds_conn_type, None)

    def constructor_kwargs(self, data: dict[str, Any]) -> dict[str, Any]:
        data["connection_ref"] = data.pop("connection_ref", None)
        if data["connection_ref"] is None:
            data["connection_ref"] = InternalMaterializationConnectionRef()
        return data

    def to_object(self, data: dict) -> Any:
        kw = self.constructor_kwargs(data)
        return self.TARGET_CLS(**kw)


class BaseSQLDataSourceSpecStorageSchema(DataSourceSpecStorageSchema):  # noqa
    TARGET_CLS = SQLDataSourceSpecBase

    db_version = ma_fields.String(required=False, allow_none=True, load_default=None)


class SubselectSQLDataSourceSpecStorageSchemaMixin(BaseSQLDataSourceSpecStorageSchema):  # noqa
    subsql = ma_fields.String(required=False, allow_none=True, load_default=None)


class SubselectDataSourceSpecStorageSchema(
    SubselectSQLDataSourceSpecStorageSchemaMixin,
    BaseSQLDataSourceSpecStorageSchema,
):
    TARGET_CLS = SubselectDataSourceSpec


class DbSQLDataSourceSpecStorageSchemaMixin(BaseSQLDataSourceSpecStorageSchema):  # noqa
    db_name = ma_fields.String(required=False, allow_none=True, load_default=None)


class TableSQLDataSourceSpecStorageSchemaMixin(BaseSQLDataSourceSpecStorageSchema):  # noqa
    table_name = ma_fields.String(required=False, allow_none=True, load_default=None)


class IndexedSQLDataSourceSpecStorageSchemaMixin(BaseSQLDataSourceSpecStorageSchema):  # noqa
    index_info_set = FrozenSetField(
        ma_fields.Nested(DataSourceIndexInfoStorageSchema),
        sort_output=True,
        required=False,
        load_default=None,
        allow_none=True,
    )


class SQLDataSourceSpecStorageSchema(
    DbSQLDataSourceSpecStorageSchemaMixin,
    TableSQLDataSourceSpecStorageSchemaMixin,
    IndexedSQLDataSourceSpecStorageSchemaMixin,
    BaseSQLDataSourceSpecStorageSchema,
):  # noqa
    TARGET_CLS = StandardSQLDataSourceSpec


class SchemaSQLDataSourceSpecStorageSchemaMixin(BaseSQLDataSourceSpecStorageSchema):  # noqa
    schema_name = ma_fields.String(required=False, allow_none=True, load_default=None)


class SchemaSQLDataSourceSpecStorageSchema(SchemaSQLDataSourceSpecStorageSchemaMixin, SQLDataSourceSpecStorageSchema):
    TARGET_CLS = StandardSchemaSQLDataSourceSpec
