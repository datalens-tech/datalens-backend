from __future__ import annotations

from typing import Optional

import attr

from bi_core.connection_executors.async_sa_executors import DefaultSqlAlchemyConnExecutor
from bi_core.connectors.clickhouse_base.adapters import ClickHouseAdapter, AsyncClickHouseAdapter
from bi_core.connectors.clickhouse_base.dto import ClickHouseConnDTO
from bi_core.connectors.clickhouse_base.target_dto import ClickHouseConnTargetDTO
from bi_core.connectors.clickhouse_base.conn_options import CHConnectOptions


@attr.s(cmp=False, hash=False)
class ClickHouseSyncAdapterConnExecutor(DefaultSqlAlchemyConnExecutor[ClickHouseAdapter]):
    TARGET_ADAPTER_CLS = ClickHouseAdapter

    _conn_dto: ClickHouseConnDTO = attr.ib()
    _conn_options: CHConnectOptions = attr.ib()

    async def _make_target_conn_dto_pool(self) -> list[ClickHouseConnTargetDTO]:  # type: ignore  # TODO: fix
        dto_pool = []
        for host in self._conn_hosts_pool:
            # TODO FIX: May be pin host here?
            effective_host = await self._mdb_mgr.normalize_mdb_host(host)
            dto_pool.append(ClickHouseConnTargetDTO(
                conn_id=self._conn_dto.conn_id,
                pass_db_messages_to_user=self._conn_options.pass_db_messages_to_user,
                pass_db_query_to_user=self._conn_options.pass_db_query_to_user,
                protocol=self._conn_dto.protocol,
                host=effective_host,
                port=self._conn_dto.port,
                db_name=self._conn_dto.db_name,
                cluster_name=self._conn_dto.cluster_name,
                endpoint=self._conn_dto.endpoint,
                username=self._conn_dto.username,
                password=self._conn_dto.password,
                max_execution_time=self._conn_options.max_execution_time,
                total_timeout=self._conn_options.total_timeout,
                connect_timeout=self._conn_options.connect_timeout,
                insert_quorum=self._conn_options.insert_quorum,
                insert_quorum_timeout=self._conn_options.insert_quorum_timeout,
                disable_value_processing=self._conn_options.disable_value_processing,
                secure=self._conn_dto.secure,
                ssl_ca=self._conn_dto.ssl_ca,
            ))
        return dto_pool

    def mutate_for_dashsql(self, db_params: Optional[dict[str, str]] = None):  # type: ignore  # TODO: fix
        if db_params:  # TODO.
            raise Exception("`db_params` are not supported at the moment")
        return self.clone(
            conn_options=self._conn_options.clone(
                disable_value_processing=True,
            ),
        )


@attr.s(cmp=False, hash=False)
class ClickHouseAsyncAdapterConnExecutor(ClickHouseSyncAdapterConnExecutor):
    TARGET_ADAPTER_CLS = AsyncClickHouseAdapter  # type: ignore  # TODO: fix
