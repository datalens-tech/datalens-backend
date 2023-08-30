from __future__ import annotations

from typing import List

import attr

from bi_core.connection_executors.async_sa_executors import DefaultSqlAlchemyConnExecutor
from bi_core.connectors.clickhouse_base.target_dto import ClickHouseConnTargetDTO
from bi_core.connectors.clickhouse_base.conn_options import CHConnectOptions
from bi_core.connectors.chydb.adapters import CHYDBAdapter
from bi_core.connectors.chydb.async_adapters import AsyncCHYDBAdapter
from bi_core.connectors.chydb.dto import CHOverYDBDTO


@attr.s(cmp=False, hash=False)
class CHYDBSyncAdapterConnExecutor(DefaultSqlAlchemyConnExecutor[CHYDBAdapter]):
    TARGET_ADAPTER_CLS = CHYDBAdapter

    _conn_dto: CHOverYDBDTO = attr.ib()
    _conn_options: CHConnectOptions = attr.ib()

    async def _make_target_conn_dto_pool(self) -> List[ClickHouseConnTargetDTO]:  # type: ignore  # TODO: fix
        return [ClickHouseConnTargetDTO(
            conn_id=self._conn_dto.conn_id,
            pass_db_messages_to_user=self._conn_options.pass_db_messages_to_user,
            pass_db_query_to_user=self._conn_options.pass_db_query_to_user,
            host=self._conn_dto.host,
            port=self._conn_dto.port,
            username='default',
            password=self._conn_dto.token,
            db_name=None,
            cluster_name=None,
            endpoint=self._conn_dto.endpoint,
            protocol=self._conn_dto.protocol,
            max_execution_time=self._conn_options.max_execution_time,
            total_timeout=self._conn_options.total_timeout,
            connect_timeout=self._conn_options.connect_timeout,
            insert_quorum=self._conn_options.insert_quorum,
            insert_quorum_timeout=self._conn_options.insert_quorum_timeout,
            disable_value_processing=self._conn_options.disable_value_processing,
        )]


@attr.s(cmp=False, hash=False)
class CHYDBAsyncAdapterConnExecutor(CHYDBSyncAdapterConnExecutor):
    TARGET_ADAPTER_CLS = AsyncCHYDBAdapter  # type: ignore  # TODO: fix
