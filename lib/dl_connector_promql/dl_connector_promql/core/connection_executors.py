from __future__ import annotations

from typing import (
    TYPE_CHECKING,
    Sequence,
)

import attr

from dl_core.connection_executors.async_sa_executors import DefaultSqlAlchemyConnExecutor

from dl_connector_promql.core.adapter import (
    AsyncPromQLAdapter,
    PromQLAdapter,
)
from dl_connector_promql.core.target_dto import PromQLConnTargetDTO


if TYPE_CHECKING:
    from dl_connector_promql.core.dto import PromQLConnDTO


@attr.s(cmp=False, hash=False)
class PromQLConnExecutor(DefaultSqlAlchemyConnExecutor[PromQLAdapter]):
    TARGET_ADAPTER_CLS = PromQLAdapter

    _conn_dto: PromQLConnDTO = attr.ib()

    async def _make_target_conn_dto_pool(self) -> Sequence[PromQLConnTargetDTO]:
        return [
            PromQLConnTargetDTO(
                conn_id=self._conn_dto.conn_id,
                pass_db_messages_to_user=self._conn_options.pass_db_messages_to_user,
                pass_db_query_to_user=self._conn_options.pass_db_query_to_user,
                host=self._conn_dto.host,
                port=self._conn_dto.port,
                username=self._conn_dto.username,
                password=self._conn_dto.password,
                protocol=self._conn_dto.protocol,
                db_name=self._conn_dto.db_name,
            )
        ]


@attr.s(cmp=False, hash=False)
class PromQLAsyncAdapterConnExecutor(DefaultSqlAlchemyConnExecutor[AsyncPromQLAdapter]):
    TARGET_ADAPTER_CLS = AsyncPromQLAdapter

    _conn_dto: PromQLConnDTO = attr.ib()

    async def _make_target_conn_dto_pool(self) -> Sequence[PromQLConnTargetDTO]:
        return [
            PromQLConnTargetDTO(
                conn_id=self._conn_dto.conn_id,
                pass_db_messages_to_user=self._conn_options.pass_db_messages_to_user,
                pass_db_query_to_user=self._conn_options.pass_db_query_to_user,
                host=self._conn_dto.host,
                port=self._conn_dto.port,
                username=self._conn_dto.username,
                password=self._conn_dto.password,
                protocol=self._conn_dto.protocol,
                db_name=self._conn_dto.db_name,
            )
        ]
