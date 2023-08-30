from __future__ import annotations

from base64 import b64decode, b64encode
from typing import Any, Dict, List, Tuple, Sequence, Optional

import json
import sqlalchemy as sa
from marshmallow import fields
from marshmallow_oneofschema import OneOfSchema
from multidict import CIMultiDict
from sqlalchemy import MetaData, Table, Column
from sqlalchemy.ext.serializer import (
    dumps as sa_dumps,
)
from sqlalchemy.ext.serializer import loads

from bi_core.enums import QueryExecutorMode
from bi_core.connection_executors.models.db_adapter_data import DBAdapterQuery
from bi_core.connection_executors.models.scoped_rci import DBAdapterScopedRCI
from bi_core.connection_executors.qe_serializer.schema_base import BaseQEAPISchema
from bi_core.connection_models import TableIdent, DBIdent, SchemaIdent, SATextTableDefinition, TableDefinition
from bi_core.serialization import RedisDatalensDataJSONEncoder, RedisDatalensDataJSONDecoder
from bi_utils.utils import get_type_full_name


class SATextField(fields.String):
    def _serialize(self, value: Any, attr: Any, obj: Any, **kwargs: Any) -> Optional[str]:
        assert isinstance(value, sa.sql.elements.TextClause)
        text_value = value.text
        return super()._serialize(text_value, attr, obj, **kwargs)

    def _deserialize(self, value: Any, attr: Any, data: Any, **kwargs: Any) -> Any:
        text_value = super()._deserialize(value, attr, data, **kwargs)
        assert text_value is not None
        return sa.sql.elements.TextClause(text_value)


class DBAdapterQueryStrSchema(BaseQEAPISchema):
    chunk_size = fields.Integer(allow_none=True)
    query = fields.String()
    db_name = fields.String(allow_none=True)
    disable_streaming = fields.Boolean()
    connector_specific_params = fields.Method(
        allow_none=True, dump_default=None, serialize='dump_conn_params', deserialize='load_conn_params'
    )
    is_dashsql_query = fields.Boolean()

    def to_object(self, data: Dict) -> DBAdapterQuery:
        return DBAdapterQuery(
            query=data['query'],
            chunk_size=data['chunk_size'],
            db_name=data['db_name'],
            disable_streaming=data['disable_streaming'],
            connector_specific_params=data['connector_specific_params'],
            is_dashsql_query=data['is_dashsql_query'],
        )

    def dump_conn_params(self, dba_query: DBAdapterQuery) -> Optional[dict]:
        conn_params = dba_query.connector_specific_params
        if conn_params is not None:
            for k, v in conn_params.items():
                conn_params[k] = json.dumps(v, cls=RedisDatalensDataJSONEncoder)
        return conn_params

    def load_conn_params(self, conn_params: Optional[dict]) -> Optional[Dict[str, Any]]:
        if conn_params is not None:
            for k, v in conn_params.items():
                conn_params[k] = json.loads(v, cls=RedisDatalensDataJSONDecoder)
        return conn_params


# noinspection PyMethodMayBeStatic
class DBAdapterQuerySQLASchema(DBAdapterQueryStrSchema):
    query = fields.Method(serialize='dump_query', deserialize='load_query')  # type: ignore  # TODO: fix
    tables = fields.Method(serialize='dump_tables', deserialize='load_tables')

    def dump_query(self, dba_query: DBAdapterQuery) -> str:
        query = dba_query.query
        return b64encode(sa_dumps(query)).decode()

    def load_query(self, data: str) -> str:
        return data

    def dump_tables(self, dba_query: DBAdapterQuery) -> Dict[str, List[str]]:
        tables_dict = {}

        query = dba_query.query
        if isinstance(query, str):
            raise ValueError("DBAdapterQuerySQLASchema should be used only for plain-text queries serialization.")

        for table in getattr(query, 'froms', []):
            if isinstance(table, sa.Table):
                tables_dict[table.name] = [c.name for c in table.c]

        return tables_dict

    def load_tables(self, data: dict) -> MetaData:
        metadata = MetaData()

        for table_name in data:
            Table(table_name, metadata, *[Column(col_name) for col_name in data[table_name]])

        return metadata

    def to_object(self, data: Dict[str, Any]) -> DBAdapterQuery:
        patched_data = dict(data)
        metadata = patched_data.pop('tables')
        dumped_sa_query = patched_data.pop('query')
        patched_data['query'] = loads(b64decode(dumped_sa_query), metadata=metadata)
        return super().to_object(patched_data)


class GenericDBAQuerySchema(OneOfSchema):
    type_field = 'mode'
    type_schemas = {
        t.name: s
        for t, s in {
            QueryExecutorMode.sqla_dump: DBAdapterQuerySQLASchema,
            QueryExecutorMode.text: DBAdapterQueryStrSchema,
        }.items()
    }

    def get_obj_type(self, obj: DBAdapterQuery) -> str:
        if isinstance(obj.query, str):
            return QueryExecutorMode.text.name
        else:
            return QueryExecutorMode.sqla_dump.name


# TODO FIX: Validation
class CIMultiDictField(fields.Field):
    def _serialize(self, value: CIMultiDict, attr: Any, obj: Any, **kwargs: Any) -> Tuple[Tuple[str, str], ...]:
        return tuple(
            (item[0], item[1])
            for item in value.items()
        )

    def _deserialize(self, value: Sequence[Tuple[str, str]], attr: Any, data: Any, **kwargs: Any) -> CIMultiDict:
        return CIMultiDict(value)


class DBAdapterScopedRCISchema(BaseQEAPISchema):
    request_id = fields.String(allow_none=True)
    user_name = fields.String(allow_none=True)
    x_dl_debug_mode = fields.Boolean(allow_none=True)
    client_ip = fields.String(allow_none=True)

    def to_object(self, data: Dict[str, Any]) -> DBAdapterScopedRCI:
        return DBAdapterScopedRCI(
            request_id=data['request_id'],
            user_name=data['user_name'],
            x_dl_debug_mode=data['x_dl_debug_mode'],
            client_ip=data['client_ip'],
        )


class DBIdentSchema(BaseQEAPISchema):
    db_name = fields.String(allow_none=True)

    def to_object(self, data: Dict[str, Any]) -> DBIdent:
        return DBIdent(**data)


class SchemaIdentSchema(BaseQEAPISchema):
    schema_name = fields.String(allow_none=True)
    db_name = fields.String(allow_none=True)

    def to_object(self, data: Dict[str, Any]) -> Any:
        return SchemaIdent(**data)


class TableIdentSchema(BaseQEAPISchema):
    table_name = fields.String()
    db_name = fields.String(allow_none=True)
    schema_name = fields.String(allow_none=True)

    def to_object(self, data: Dict[str, Any]) -> TableIdent:
        return TableIdent(**data)


class SATextTableDefinitionSchema(BaseQEAPISchema):
    text = SATextField()

    def to_object(self, data: Dict[str, Any]) -> SATextTableDefinition:
        return SATextTableDefinition(**data)


class TableDefinitionSchema(OneOfSchema):
    type_field = 'mode'
    supported_types = {
        TableIdent: TableIdentSchema,
        SATextTableDefinition: SATextTableDefinitionSchema,
    }
    type_schemas = {type_obj.def_type.name: schema_cls for type_obj, schema_cls in supported_types.items()}  # type: ignore  # TODO: fix

    def get_obj_type(self, obj: TableDefinition) -> str:
        if type(obj) in self.supported_types:
            return obj.def_type.name

        raise TypeError(f"Class is not supported by TableDefinitionSchema: {get_type_full_name(type(obj))}")
