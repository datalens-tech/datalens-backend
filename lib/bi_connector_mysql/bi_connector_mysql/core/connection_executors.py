from __future__ import annotations

from typing import List, Sequence, TypeVar

import attr

from dl_core.connection_models.conn_options import ConnectOptions
from bi_connector_mysql.core.dto import MySQLConnDTO
from bi_connector_mysql.core.adapters_mysql import MySQLAdapter
from bi_connector_mysql.core.async_adapters_mysql import AsyncMySQLAdapter
from dl_core.connection_executors.adapters.common_base import CommonBaseDirectAdapter
from dl_core.connection_executors.async_sa_executors import DefaultSqlAlchemyConnExecutor
from bi_connector_mysql.core.target_dto import MySQLConnTargetDTO


_BASE_MYSQL_ADAPTER_TV = TypeVar('_BASE_MYSQL_ADAPTER_TV', bound=CommonBaseDirectAdapter)


class _BaseMySQLConnExecutor(DefaultSqlAlchemyConnExecutor[_BASE_MYSQL_ADAPTER_TV]):
    _conn_dto: MySQLConnDTO = attr.ib()
    _conn_options: ConnectOptions = attr.ib()
    _conn_hosts_pool: Sequence[str] = attr.ib()

    async def _make_target_conn_dto_pool(self) -> List[MySQLConnTargetDTO]:  # type: ignore  # TODO: fix
        dto_pool = []
        for host in self._conn_hosts_pool:
            dto_pool.append(MySQLConnTargetDTO(
                conn_id=self._conn_dto.conn_id,
                pass_db_messages_to_user=self._conn_options.pass_db_messages_to_user,
                pass_db_query_to_user=self._conn_options.pass_db_query_to_user,
                host=host,
                port=self._conn_dto.port,
                db_name=self._conn_dto.db_name,
                username=self._conn_dto.username,
                password=self._conn_dto.password,
            ))
        return dto_pool


@attr.s(cmp=False, hash=False)
class MySQLConnExecutor(_BaseMySQLConnExecutor[MySQLAdapter]):
    TARGET_ADAPTER_CLS = MySQLAdapter


@attr.s(cmp=False, hash=False)
class AsyncMySQLConnExecutor(_BaseMySQLConnExecutor[AsyncMySQLAdapter]):
    TARGET_ADAPTER_CLS = AsyncMySQLAdapter
