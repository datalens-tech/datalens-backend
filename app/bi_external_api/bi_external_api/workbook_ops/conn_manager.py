from __future__ import annotations

from functools import singledispatchmethod
from typing import Any, Sequence, TypeVar, Type

import attr
from marshmallow import EXCLUDE

from bi_external_api.domain import external as ext
from bi_external_api.domain.external import get_external_model_mapper
from bi_external_api.enums import ExtAPIType
from bi_external_api.internal_api_clients.main import InternalAPIClients
from bi_external_api.workbook_ops.private_exceptions import GeneralConfigValidationException


@attr.s(frozen=True)
class ConnectionSecretsHolder:
    all_secrets: Sequence[ext.ConnectionSecret] = attr.ib()

    def resolve_single_plain_secret(self, conn_name: str) -> ext.PlainSecret:
        candidates = [sec for sec in self.all_secrets]
        if len(candidates) == 1:
            secret = candidates[0].secret
            assert isinstance(secret, ext.PlainSecret)
            return secret
        raise GeneralConfigValidationException(f"No secret for connection {conn_name!r} was provided")


@attr.s(frozen=True)
class ConnectionManager:
    int_api_cli: InternalAPIClients = attr.ib()
    secret_holder: ConnectionSecretsHolder = attr.ib()

    def get_connection_converter(self, conn_name: str) -> ConnectionConverter:
        return ConnectionConverter(
            secret_holder=self.secret_holder,
            conn_name=conn_name,
        )

    async def create_connection(
            self,
            conn_inst: ext.ConnectionInstance,
            *,
            wb_id: str,
    ) -> ext.EntryInfo:
        converter = self.get_connection_converter(conn_inst.name)

        # TODO FIX: Test connection before create
        bi_conn = await self.int_api_cli.datasets_cp.create_connection(
            wb_id=wb_id,
            name=conn_inst.name,
            conn_data=converter.ext_conn_to_data(conn_inst.connection),
        )
        return ext.EntryInfo(kind=ext.EntryKind.connection, name=conn_inst.name, id=bi_conn.id)

    async def modify_connection(
        self,
        conn_inst: ext.ConnectionInstance,
        *,
        wb_id: str,
        conn_id: str,
    ) -> ext.EntryInfo:
        converter = self.get_connection_converter(conn_inst.name)

        # TODO: Handle connection not found
        # TODO FIX: Test connection before modify
        bi_conn = await self.int_api_cli.datasets_cp.modify_connection(
            wb_id=wb_id,
            conn_id=conn_id,
            name=conn_inst.name,
            conn_data=converter.ext_conn_to_data(conn_inst.connection),
        )
        return ext.EntryInfo(kind=ext.EntryKind.connection, name=conn_inst.name, id=bi_conn.id)

    async def delete_connection(
        self,
        *,
        conn_id: str,
    ) -> None:
        await self.int_api_cli.datasets_cp.delete_connection(conn_id)

    def validate_conn(self, conn_inst: ext.ConnectionInstance) -> None:
        self.get_connection_converter(conn_inst.name).ext_conn_to_data(conn_inst.connection)


_TV_CONN_TYPE = TypeVar("_TV_CONN_TYPE", bound=ext.Connection)


def int_conn_data_to_ext(api_type: ExtAPIType, conn_type: Type[_TV_CONN_TYPE], data: dict[str, Any]) -> _TV_CONN_TYPE:
    model_mapper = get_external_model_mapper(api_type)
    schema = model_mapper.get_schema_for_attrs_class(conn_type)()
    return schema.load(data, unknown=EXCLUDE)


@attr.s(frozen=True)
class ConnectionConverter:
    secret_holder: ConnectionSecretsHolder = attr.ib()
    conn_name: str = attr.ib()

    def resolve_secret(self) -> str:
        return self.secret_holder.resolve_single_plain_secret(self.conn_name).secret

    @singledispatchmethod
    def ext_conn_to_data(self, conn: Any) -> dict[str, Any]:
        raise NotImplementedError(f"Unsupported type of connection: {type(conn)}")

    @ext_conn_to_data.register
    def _chyt_conn_to_data(self, conn: ext.CHYTConnection) -> dict[str, Any]:
        return dict(
            type="ch_over_yt",
            cluster=conn.cluster,
            alias=conn.clique_alias,
            token=self.resolve_secret(),
            raw_sql_level=conn.raw_sql_level.name,
            cache_ttl_sec=conn.cache_ttl_sec,
        )

    @ext_conn_to_data.register
    def _chyt_ua_conn_to_data(self, conn: ext.CHYTUserAuthConnection) -> dict[str, Any]:
        return dict(
            type="ch_over_yt_user_auth",
            cluster=conn.cluster,
            alias=conn.clique_alias,
            raw_sql_level=conn.raw_sql_level.name,
            cache_ttl_sec=conn.cache_ttl_sec,
        )

    @ext_conn_to_data.register
    def _ch_conn_to_data(self, conn: ext.ClickHouseConnection) -> dict[str, Any]:
        return dict(
            type="clickhouse",
            host=conn.host,
            port=conn.port,
            username=conn.username,
            password=self.resolve_secret(),
            secure=conn.secure,
            raw_sql_level=conn.raw_sql_level.name,
            cache_ttl_sec=conn.cache_ttl_sec,
        )

    @ext_conn_to_data.register
    def _pg_conn_to_data(self, conn: ext.PostgresConnection) -> dict[str, Any]:
        return dict(
            type="postgres",
            host=conn.host,
            port=conn.port,
            username=conn.username,
            password=self.resolve_secret(),
            db_name=conn.database_name,
            raw_sql_level=conn.raw_sql_level.name,
            cache_ttl_sec=conn.cache_ttl_sec,
        )
