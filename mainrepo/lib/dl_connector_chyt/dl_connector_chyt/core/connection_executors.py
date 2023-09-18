from __future__ import annotations

import abc
import logging

import attr

from dl_core.connection_executors.async_sa_executors import DefaultSqlAlchemyConnExecutor

from dl_connector_chyt.core.adapters import (
    BaseCHYTAdapter,
    CHYTAdapter,
)
from dl_connector_chyt.core.async_adapters import AsyncCHYTAdapter
from dl_connector_chyt.core.conn_options import CHYTConnectOptions
from dl_connector_chyt.core.dto import (
    CHYTDTO,
    BaseCHYTDTO,
)
from dl_connector_chyt.core.target_dto import (
    BaseCHYTConnTargetDTO,
    CHYTConnTargetDTO,
)

LOGGER = logging.getLogger(__name__)


@attr.s(cmp=False, hash=False)
class BaseCHYTAdapterConnExecutor(DefaultSqlAlchemyConnExecutor[BaseCHYTAdapter], abc.ABC):
    _conn_dto: BaseCHYTDTO = attr.ib()
    _conn_options: CHYTConnectOptions = attr.ib()

    @abc.abstractmethod
    async def _get_target_conn_dto(self) -> BaseCHYTConnTargetDTO:
        raise NotImplementedError()

    async def _make_target_conn_dto_pool(self) -> list[BaseCHYTConnTargetDTO]:
        target_dto = await self._get_target_conn_dto()
        return [target_dto]


@attr.s(cmp=False, hash=False)
class CHYTAdapterConnExecutor(BaseCHYTAdapterConnExecutor):
    _conn_dto: CHYTDTO = attr.ib()

    async def _get_target_conn_dto(self) -> CHYTConnTargetDTO:
        return CHYTConnTargetDTO(
            conn_id=self._conn_dto.conn_id,
            pass_db_messages_to_user=self._conn_options.pass_db_messages_to_user,
            pass_db_query_to_user=self._conn_options.pass_db_query_to_user,
            host=self._conn_dto.host,
            port=self._conn_dto.port,
            username="default",
            password=self._conn_dto.token,
            db_name=self._conn_dto.clique_alias,
            cluster_name=None,
            endpoint="query",
            protocol=self._conn_dto.protocol,
            max_execution_time=self._conn_options.max_execution_time,
            total_timeout=self._conn_options.total_timeout,
            connect_timeout=self._conn_options.connect_timeout,
            insert_quorum=self._conn_options.insert_quorum,
            insert_quorum_timeout=self._conn_options.insert_quorum_timeout,
            disable_value_processing=self._conn_options.disable_value_processing,
        )


@attr.s(cmp=False, hash=False)
class CHYTSyncAdapterConnExecutor(CHYTAdapterConnExecutor):
    TARGET_ADAPTER_CLS = CHYTAdapter


@attr.s(cmp=False, hash=False)
class CHYTAsyncAdapterConnExecutor(CHYTAdapterConnExecutor):
    TARGET_ADAPTER_CLS = AsyncCHYTAdapter  # type: ignore
