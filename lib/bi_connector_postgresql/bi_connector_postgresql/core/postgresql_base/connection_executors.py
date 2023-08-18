from __future__ import annotations

from typing import Dict, List, Optional, TypeVar

import attr

from bi_connector_postgresql.core.postgresql_base.adapters_postgres import PostgresAdapter
from bi_connector_postgresql.core.postgresql_base.async_adapters_postgres import AsyncPostgresAdapter
from bi_core.connection_executors.adapters.common_base import CommonBaseDirectAdapter
from bi_core.connection_executors.async_sa_executors import DefaultSqlAlchemyConnExecutor
from bi_connector_postgresql.core.postgresql_base.target_dto import PostgresConnTargetDTO
from bi_connector_postgresql.core.postgresql_base.dto import PostgresConnDTOBase


_BASE_POSTGRES_ADAPTER_TV = TypeVar('_BASE_POSTGRES_ADAPTER_TV', bound=CommonBaseDirectAdapter)


@attr.s(cmp=False, hash=False)
class BasePostgresConnExecutor(DefaultSqlAlchemyConnExecutor[_BASE_POSTGRES_ADAPTER_TV]):
    _conn_dto: PostgresConnDTOBase = attr.ib()

    async def _make_target_conn_dto_pool(self) -> List[PostgresConnTargetDTO]:  # type: ignore  # TODO: fix
        dto_pool = []
        for host in self._conn_hosts_pool:
            effective_host = await self._mdb_mgr.normalize_mdb_host(host)
            dto_pool.append(PostgresConnTargetDTO(
                conn_id=self._conn_dto.conn_id,
                pass_db_messages_to_user=self._conn_options.pass_db_messages_to_user,
                pass_db_query_to_user=self._conn_options.pass_db_query_to_user,
                host=effective_host,
                port=self._conn_dto.port,
                db_name=self._conn_dto.db_name,
                username=self._conn_dto.username,
                password=self._conn_dto.password,
                enforce_collate=self._conn_dto.enforce_collate,
                ssl_enable=self._conn_dto.ssl_enable,
                ssl_ca=self._conn_dto.ssl_ca,
            ))
        return dto_pool

    def mutate_for_dashsql(self, db_params: Optional[Dict[str, str]] = None):  # type: ignore  # TODO: fix
        if db_params:
            # TODO: better exception class for HTTP 4xx response
            raise Exception("No db_params supported here at the moment")
        return self


@attr.s(cmp=False, hash=False)
class PostgresConnExecutor(BasePostgresConnExecutor[PostgresAdapter]):
    TARGET_ADAPTER_CLS = PostgresAdapter


@attr.s(cmp=False, hash=False)
class AsyncPostgresConnExecutor(BasePostgresConnExecutor[AsyncPostgresAdapter]):
    TARGET_ADAPTER_CLS = AsyncPostgresAdapter
