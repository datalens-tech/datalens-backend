from __future__ import annotations

import abc
from typing import (
    TYPE_CHECKING,
    Any,
    ClassVar,
    Dict,
    Generic,
    List,
    Optional,
    Type,
    TypeVar,
)

import attr
from marshmallow import fields

from bi_core.connection_executors.adapters.common_base import CommonBaseDirectAdapter
from bi_core.connection_executors.models.db_adapter_data import (
    DBAdapterQuery,
    RawSchemaInfo,
)
from bi_core.connection_executors.models.scoped_rci import DBAdapterScopedRCI
from bi_core.connection_executors.qe_serializer.schema_base import BaseQEAPISchema
from bi_core.connection_executors.qe_serializer.schemas_common import TableIdentSchema
from bi_core.connection_executors.qe_serializer.schemas_responses import (
    PrimitivesResponseSchema,
    RawSchemaInfoSchema,
)
from bi_core.connection_models import (
    DBIdent,
    SchemaIdent,
    TableDefinition,
    TableIdent,
)

if TYPE_CHECKING:
    from bi_core.connection_executors.models.connection_target_dto_base import ConnTargetDTO


@attr.s(frozen=True)
class RemoteDBAdapterAction:
    target_conn_dto: ConnTargetDTO = attr.ib()
    req_ctx_info: DBAdapterScopedRCI = attr.ib()
    dba_cls: Type[CommonBaseDirectAdapter] = attr.ib()


@attr.s(frozen=True)
class ActionExecuteQuery(RemoteDBAdapterAction):
    db_adapter_query: DBAdapterQuery = attr.ib()


_RES_SCHEMA_TV = TypeVar("_RES_SCHEMA_TV")


class NonStreamAction(Generic[_RES_SCHEMA_TV], RemoteDBAdapterAction, metaclass=abc.ABCMeta):
    ResultSchema = ClassVar[Type[BaseQEAPISchema]]

    def serialize_response(self, val: _RES_SCHEMA_TV) -> Dict:
        return self.ResultSchema().dump(val)  # type: ignore  # TODO: fix

    def deserialize_response(self, data: Dict) -> _RES_SCHEMA_TV:
        return self.ResultSchema().load(data)  # type: ignore  # TODO: fix


@attr.s(frozen=True)
class ActionTest(NonStreamAction[None]):
    class ResultSchema(BaseQEAPISchema):
        def to_object(self, data: Dict[str, Any]) -> Any:
            return None


@attr.s(frozen=True)
class ActionGetDBVersion(NonStreamAction[Optional[str]]):
    db_ident: DBIdent = attr.ib()

    class ResultSchema(PrimitivesResponseSchema):
        value = fields.String(allow_none=True)  # type: ignore  # TODO: fix


@attr.s(frozen=True)
class ActionGetSchemaNames(NonStreamAction[List[str]]):
    db_ident: DBIdent = attr.ib()

    class ResultSchema(PrimitivesResponseSchema):
        value = fields.List(fields.String())  # type: ignore  # TODO: fix


@attr.s(frozen=True)
class ActionGetTables(NonStreamAction[List[TableIdent]]):
    schema_ident: SchemaIdent = attr.ib()

    class ResultSchema(PrimitivesResponseSchema):
        value = fields.Nested(TableIdentSchema, many=True)  # type: ignore  # TODO: fix


@attr.s(frozen=True)
class ActionGetTableInfo(NonStreamAction[RawSchemaInfo]):
    table_def: TableDefinition = attr.ib()
    fetch_idx_info: bool = attr.ib()

    ResultSchema = RawSchemaInfoSchema


@attr.s(frozen=True)
class ActionIsTableExists(NonStreamAction[bool]):
    table_ident: TableIdent = attr.ib()

    class ResultSchema(PrimitivesResponseSchema):
        value = fields.Boolean()  # type: ignore  # TODO: fix
