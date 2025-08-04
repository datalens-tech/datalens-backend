from __future__ import annotations

from typing import (
    Any,
    Union,
)

from marshmallow import fields

from dl_constants.enums import DashSQLQueryType
from dl_core.connection_executors.adapters.common_base import CommonBaseDirectAdapter
from dl_core.connection_executors.models.connection_target_dto_base import ConnTargetDTO
from dl_core.connection_executors.qe_serializer import dba_actions as dba_actions
from dl_core.connection_executors.qe_serializer.schema_base import BaseQEAPISchema
from dl_core.connection_executors.qe_serializer.schemas_common import (
    DBAdapterScopedRCISchema,
    DBIdentSchema,
    GenericDBAQuerySchema,
    SchemaIdentSchema,
    TableDefinitionSchema,
    TableIdentSchema,
)
from dl_model_tools.schema.dynamic_enum_field import DynamicEnumField


class DBAdapterActionBaseSchema(BaseQEAPISchema):
    target_conn_dto = fields.Method(serialize="dump_target_conn_dto", deserialize="load_target_conn_dto")
    dba_cls = fields.Method(serialize="dump_dba_cls", deserialize="load_dba_cls")
    req_ctx_info = fields.Nested(DBAdapterScopedRCISchema)

    @property
    def allowed_dba_classes(self) -> frozenset[type[CommonBaseDirectAdapter]]:
        # TODO FIX: Ensure no classes with same qualname or use FQDN
        return self.context["allowed_dba_classes"]

    def dump_dba_cls(self, act: dba_actions.ActionExecuteQuery) -> str:
        return f"{act.dba_cls.__module__}.{act.dba_cls.__qualname__}"

    def load_dba_cls(self, value: str) -> Union[type[CommonBaseDirectAdapter]]:
        mod_name, cls_name = value.rsplit(".", 1) if "." in value else (None, value)
        candidate = next(
            filter(
                lambda clz: clz.__module__ == mod_name and clz.__qualname__ == cls_name or clz.__qualname__ == value,
                self.allowed_dba_classes,
            ),  # TODO clz.__qualname__ == value method is deprecated, to be removed someday
            None,
        )
        if candidate is None:
            raise ValueError(f"Can not restore DBA class from string '{value}'\nAvailable classes: {self.allowed_dba_classes}")
        return candidate

    def dump_target_conn_dto(self, act: dba_actions.ActionExecuteQuery) -> dict:
        return act.target_conn_dto.to_jsonable_dict()

    def load_target_conn_dto(self, value: dict) -> ConnTargetDTO:
        return ConnTargetDTO.from_polymorphic_jsonable_dict(value)

    def to_object(self, data: dict[str, Any]) -> Any:
        raise NotImplementedError()


class ActionExecuteQuerySchema(DBAdapterActionBaseSchema):
    db_adapter_query = fields.Nested(GenericDBAQuerySchema)

    def to_object(self, data: dict[str, Any]) -> dba_actions.ActionExecuteQuery:
        return dba_actions.ActionExecuteQuery(**data)


class ActionNonStreamExecuteQuerySchema(DBAdapterActionBaseSchema):
    db_adapter_query = fields.Nested(GenericDBAQuerySchema)

    def to_object(self, data: dict[str, Any]) -> dba_actions.ActionNonStreamExecuteQuery:
        return dba_actions.ActionNonStreamExecuteQuery(**data)


class ActionGetDBVersionSchema(DBAdapterActionBaseSchema):
    db_ident = fields.Nested(DBIdentSchema)

    def to_object(self, data: dict[str, Any]) -> Any:
        return dba_actions.ActionGetDBVersion(**data)


class ActionTestSchema(DBAdapterActionBaseSchema):
    def to_object(self, data: dict[str, Any]) -> dba_actions.ActionTest:
        return dba_actions.ActionTest(**data)


class ActionGetSchemaNamesSchema(DBAdapterActionBaseSchema):
    db_ident = fields.Nested(DBIdentSchema)

    def to_object(self, data: dict[str, Any]) -> dba_actions.ActionGetSchemaNames:
        return dba_actions.ActionGetSchemaNames(**data)


class ActionGetTablesSchema(DBAdapterActionBaseSchema):
    schema_ident = fields.Nested(SchemaIdentSchema)

    def to_object(self, data: dict[str, Any]) -> dba_actions.ActionGetTables:
        return dba_actions.ActionGetTables(**data)


class ActionGetTableInfoSchema(DBAdapterActionBaseSchema):
    table_def = fields.Nested(TableDefinitionSchema)
    fetch_idx_info = fields.Bool()

    def to_object(self, data: dict[str, Any]) -> dba_actions.ActionGetTableInfo:
        return dba_actions.ActionGetTableInfo(**data)


class ActionIsTableExistsSchema(DBAdapterActionBaseSchema):
    table_ident = fields.Nested(TableIdentSchema)

    def to_object(self, data: dict[str, Any]) -> dba_actions.ActionIsTableExists:
        return dba_actions.ActionIsTableExists(**data)


class ActionExecuteTypedQuerySchema(DBAdapterActionBaseSchema):
    query_type = DynamicEnumField(DashSQLQueryType)
    typed_query_str = fields.String()

    def to_object(self, data: dict[str, Any]) -> dba_actions.ActionExecuteTypedQuery:
        return dba_actions.ActionExecuteTypedQuery(**data)


class ActionExecuteTypedQueryRawSchema(DBAdapterActionBaseSchema):
    query_type = DynamicEnumField(DashSQLQueryType)
    typed_query_str = fields.String()

    def to_object(self, data: dict[str, Any]) -> dba_actions.ActionExecuteTypedQueryRaw:
        return dba_actions.ActionExecuteTypedQueryRaw(**data)
