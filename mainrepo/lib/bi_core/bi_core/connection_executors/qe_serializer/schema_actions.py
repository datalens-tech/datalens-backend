from __future__ import annotations

from typing import Dict, Any, Union, Type, FrozenSet

from marshmallow import fields

from bi_core.connection_executors.adapters.common_base import CommonBaseDirectAdapter
from bi_core.connection_executors.models.connection_target_dto_base import ConnTargetDTO
from bi_core.connection_executors.qe_serializer import dba_actions as dba_actions
from bi_core.connection_executors.qe_serializer.schema_base import BaseQEAPISchema
from bi_core.connection_executors.qe_serializer.schemas_common import (
    DBAdapterScopedRCISchema,
    GenericDBAQuerySchema,
    TableDefinitionSchema,
    TableIdentSchema,
    DBIdentSchema,
    SchemaIdentSchema,
)


class DBAdapterActionBaseSchema(BaseQEAPISchema):
    target_conn_dto = fields.Method(serialize='dump_target_conn_dto', deserialize='load_target_conn_dto')
    dba_cls = fields.Method(serialize='dump_dba_cls', deserialize='load_dba_cls')
    req_ctx_info = fields.Nested(DBAdapterScopedRCISchema)

    @property
    def allowed_dba_classes(self) -> FrozenSet[Type[CommonBaseDirectAdapter]]:
        # TODO FIX: Ensure no classes with same qualname or use FQDN
        return self.context['allowed_dba_classes']

    def dump_dba_cls(self, act: dba_actions.ActionExecuteQuery) -> str:
        return act.dba_cls.__qualname__

    def load_dba_cls(self, value: str) -> Union[Type[CommonBaseDirectAdapter]]:
        candidate = next(filter(
            lambda clz: clz.__qualname__ == value,
            self.allowed_dba_classes
        ), None)
        if candidate is None:
            raise ValueError(f"Can not restore DBA class from string '{value}'")
        return candidate

    def dump_target_conn_dto(self, act: dba_actions.ActionExecuteQuery) -> Dict:
        return act.target_conn_dto.to_jsonable_dict()

    def load_target_conn_dto(self, value: Dict) -> ConnTargetDTO:
        return ConnTargetDTO.from_polymorphic_jsonable_dict(value)

    def to_object(self, data: Dict[str, Any]) -> Any:
        raise NotImplementedError()


class ActionExecuteQuerySchema(DBAdapterActionBaseSchema):
    db_adapter_query = fields.Nested(GenericDBAQuerySchema)

    def to_object(self, data: Dict[str, Any]) -> dba_actions.ActionExecuteQuery:
        return dba_actions.ActionExecuteQuery(**data)


class ActionGetDBVersionSchema(DBAdapterActionBaseSchema):
    db_ident = fields.Nested(DBIdentSchema)

    def to_object(self, data: Dict[str, Any]) -> Any:
        return dba_actions.ActionGetDBVersion(**data)


class ActionTestSchema(DBAdapterActionBaseSchema):

    def to_object(self, data: Dict[str, Any]) -> dba_actions.ActionTest:
        return dba_actions.ActionTest(**data)


class ActionGetSchemaNamesSchema(DBAdapterActionBaseSchema):
    db_ident = fields.Nested(DBIdentSchema)

    def to_object(self, data: Dict[str, Any]) -> dba_actions.ActionGetSchemaNames:
        return dba_actions.ActionGetSchemaNames(**data)


class ActionGetTablesSchema(DBAdapterActionBaseSchema):
    schema_ident = fields.Nested(SchemaIdentSchema)

    def to_object(self, data: Dict[str, Any]) -> dba_actions.ActionGetTables:
        return dba_actions.ActionGetTables(**data)


class ActionGetTableInfoSchema(DBAdapterActionBaseSchema):
    table_def = fields.Nested(TableDefinitionSchema)
    fetch_idx_info = fields.Bool()

    def to_object(self, data: Dict[str, Any]) -> dba_actions.ActionGetTableInfo:
        return dba_actions.ActionGetTableInfo(**data)


class ActionIsTableExistsSchema(DBAdapterActionBaseSchema):
    table_ident = fields.Nested(TableIdentSchema)

    def to_object(self, data: Dict[str, Any]) -> dba_actions.ActionIsTableExists:
        return dba_actions.ActionIsTableExists(**data)
