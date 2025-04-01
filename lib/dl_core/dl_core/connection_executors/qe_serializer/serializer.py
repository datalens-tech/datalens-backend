from __future__ import annotations

from typing import (
    ClassVar,
    List,
    Optional,
    Union,
)

from marshmallow import Schema

from dl_core.connection_executors.adapters.common_base import CommonBaseDirectAdapter
from dl_core.connection_executors.models.db_adapter_data import RawSchemaInfo
from dl_core.connection_executors.qe_serializer import (
    dba_actions,
    schema_actions,
    schemas_exc,
)


ResponseTypes = Union[RawSchemaInfo, List[str], Optional[str], None, bool, int]


class ActionSerializer:
    MAP_ACT_TYPE_SCHEMA_CLS: ClassVar[dict[type[dba_actions.RemoteDBAdapterAction], type[Schema]]] = {
        dba_actions.ActionExecuteQuery: schema_actions.ActionExecuteQuerySchema,
        dba_actions.ActionNonStreamExecuteQuery: schema_actions.ActionNonStreamExecuteQuerySchema,
        dba_actions.ActionTest: schema_actions.ActionTestSchema,
        dba_actions.ActionGetDBVersion: schema_actions.ActionGetDBVersionSchema,
        dba_actions.ActionGetSchemaNames: schema_actions.ActionGetSchemaNamesSchema,
        dba_actions.ActionGetTables: schema_actions.ActionGetTablesSchema,
        dba_actions.ActionGetTableInfo: schema_actions.ActionGetTableInfoSchema,
        dba_actions.ActionIsTableExists: schema_actions.ActionIsTableExistsSchema,
        dba_actions.ActionExecuteTypedQuery: schema_actions.ActionExecuteTypedQuerySchema,
        dba_actions.ActionExecuteTypedQueryRaw: schema_actions.ActionExecuteTypedQueryRawSchema,
    }

    EXC_SCHEMA_CLS = schemas_exc.GenericExcSchema

    def serialize_action(self, obj: dba_actions.RemoteDBAdapterAction) -> dict:
        schema = self.MAP_ACT_TYPE_SCHEMA_CLS[type(obj)]()
        result = schema.dump(obj)
        result["type"] = type(obj).__qualname__
        return result

    def deserialize_action(
        self,
        data: dict,
        allowed_dba_classes: frozenset[type[CommonBaseDirectAdapter]],
    ) -> dba_actions.RemoteDBAdapterAction:
        action_cls_qualname = data.pop("type", None)
        action_cls = next(
            filter(lambda clz: clz.__qualname__ == action_cls_qualname, self.MAP_ACT_TYPE_SCHEMA_CLS.keys()), None
        )
        if action_cls is None:
            raise TypeError(f"Action {action_cls_qualname} is not listed in serializable actions")

        schema = self.MAP_ACT_TYPE_SCHEMA_CLS[action_cls](context=dict(allowed_dba_classes=allowed_dba_classes))
        obj = schema.load(data)

        if not isinstance(obj, action_cls):
            raise TypeError(f"Unexpected type of action after deserialization: {type(obj)} instead of {action_cls}")

        return obj

    def serialize_exc(self, exc: Exception) -> dict:
        return self.EXC_SCHEMA_CLS().dump(exc)

    def deserialize_exc(self, data: dict) -> Exception:
        # TODO FIX: Validate type
        return self.EXC_SCHEMA_CLS().load(data)
