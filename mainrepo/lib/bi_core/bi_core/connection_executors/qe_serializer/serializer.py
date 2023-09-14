from __future__ import annotations

from typing import (
    ClassVar,
    Dict,
    FrozenSet,
    List,
    Optional,
    Type,
    Union,
)

from marshmallow import Schema

from . import dba_actions as actions
from . import schema_actions as act_schemas
from . import schemas_exc as exc_schemas
from ..adapters.common_base import CommonBaseDirectAdapter
from ..models.db_adapter_data import RawSchemaInfo

ResponseTypes = Union[RawSchemaInfo, List[str], Optional[str], None, bool, int]


class ActionSerializer:
    MAP_ACT_TYPE_SCHEMA_CLS: ClassVar[Dict[Type[actions.RemoteDBAdapterAction], Type[Schema]]] = {
        actions.ActionExecuteQuery: act_schemas.ActionExecuteQuerySchema,
        actions.ActionTest: act_schemas.ActionTestSchema,
        actions.ActionGetDBVersion: act_schemas.ActionGetDBVersionSchema,
        actions.ActionGetSchemaNames: act_schemas.ActionGetSchemaNamesSchema,
        actions.ActionGetTables: act_schemas.ActionGetTablesSchema,
        actions.ActionGetTableInfo: act_schemas.ActionGetTableInfoSchema,
        actions.ActionIsTableExists: act_schemas.ActionIsTableExistsSchema,
    }

    EXC_SCHEMA_CLS = exc_schemas.GenericExcSchema

    def serialize_action(self, obj: actions.RemoteDBAdapterAction) -> Dict:
        schema = self.MAP_ACT_TYPE_SCHEMA_CLS[type(obj)]()
        result = schema.dump(obj)
        result["type"] = type(obj).__qualname__
        return result

    def deserialize_action(
        self,
        data: Dict,
        allowed_dba_classes: FrozenSet[Type[CommonBaseDirectAdapter]],
    ) -> actions.RemoteDBAdapterAction:
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

    def serialize_exc(self, exc: Exception) -> Dict:
        return self.EXC_SCHEMA_CLS().dump(exc)

    def deserialize_exc(self, data: Dict) -> Exception:
        # TODO FIX: Validate type
        return self.EXC_SCHEMA_CLS().load(data)
