from __future__ import annotations

from typing import TypeVar

import attr

from dl_core.connection_executors.adapters.common_base import CommonBaseDirectAdapter
from dl_core.connection_executors.async_sa_executors import DefaultSqlAlchemyConnExecutor

from dl_connector_starrocks.core.adapters_starrocks import StarRocksAdapter
from dl_connector_starrocks.core.async_adapters_starrocks import AsyncStarRocksAdapter
from dl_connector_starrocks.core.dto import StarRocksConnDTO
from dl_connector_starrocks.core.target_dto import StarRocksConnTargetDTO


_BASE_STARROCKS_ADAPTER_TV = TypeVar("_BASE_STARROCKS_ADAPTER_TV", bound=CommonBaseDirectAdapter)


class _BaseStarRocksConnExecutor(DefaultSqlAlchemyConnExecutor[_BASE_STARROCKS_ADAPTER_TV]):
    _conn_dto: StarRocksConnDTO = attr.ib()

    async def _make_target_conn_dto_pool(self) -> list[StarRocksConnTargetDTO]:  # type: ignore  # TODO: fix
        dto_pool = []
        for host in self._conn_hosts_pool:
            dto_pool.append(
                StarRocksConnTargetDTO(
                    conn_id=self._conn_dto.conn_id,
                    pass_db_messages_to_user=self._conn_options.pass_db_messages_to_user,
                    pass_db_query_to_user=self._conn_options.pass_db_query_to_user,
                    host=host,
                    port=self._conn_dto.port,
                    db_name=self._conn_dto.db_name,
                    username=self._conn_dto.username,
                    password=self._conn_dto.password,
                )
            )
        return dto_pool


@attr.s(cmp=False, hash=False)
class StarRocksConnExecutor(_BaseStarRocksConnExecutor[StarRocksAdapter]):
    TARGET_ADAPTER_CLS = StarRocksAdapter


@attr.s(cmp=False, hash=False)
class AsyncStarRocksConnExecutor(_BaseStarRocksConnExecutor[AsyncStarRocksAdapter]):
    TARGET_ADAPTER_CLS = AsyncStarRocksAdapter
