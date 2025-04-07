from __future__ import annotations

from typing import TypeVar

import attr

from dl_core.connection_executors.adapters.common_base import CommonBaseDirectAdapter

from dl_connector_clickhouse.core.clickhouse.adapters import (
    DLAsyncClickHouseAdapter,
    DLClickHouseAdapter,
)
from dl_connector_clickhouse.core.clickhouse.dto import DLClickHouseConnDTO
from dl_connector_clickhouse.core.clickhouse.target_dto import DLClickHouseConnTargetDTO
from dl_connector_clickhouse.core.clickhouse_base.conn_options import CHConnectOptions
from dl_connector_clickhouse.core.clickhouse_base.connection_executors import _BaseClickHouseConnExecutor


_BASE_CLICKHOUSE_ADAPTER_TV = TypeVar("_BASE_CLICKHOUSE_ADAPTER_TV", bound=CommonBaseDirectAdapter)


@attr.s(cmp=False, hash=False)
class _DLBaseClickHouseConnExecutor(_BaseClickHouseConnExecutor[_BASE_CLICKHOUSE_ADAPTER_TV]):
    _conn_dto: DLClickHouseConnDTO = attr.ib()
    _conn_options: CHConnectOptions = attr.ib()

    async def _make_target_conn_dto_pool(self) -> list[DLClickHouseConnTargetDTO]:  # type: ignore  # TODO: fix
        dto_pool = []
        for host in self._conn_hosts_pool:
            dto_pool.append(
                DLClickHouseConnTargetDTO(
                    conn_id=self._conn_dto.conn_id,
                    pass_db_messages_to_user=self._conn_options.pass_db_messages_to_user,
                    pass_db_query_to_user=self._conn_options.pass_db_query_to_user,
                    protocol=self._conn_dto.protocol,
                    host=host,
                    port=self._conn_dto.port,
                    db_name=self._conn_dto.db_name,
                    cluster_name=self._conn_dto.cluster_name,
                    endpoint=self._conn_dto.endpoint,
                    username=self._conn_dto.username,
                    password=self._conn_dto.password,
                    max_execution_time=self._conn_options.max_execution_time,
                    total_timeout=self._conn_options.total_timeout,
                    connect_timeout=self._conn_options.connect_timeout,
                    disable_value_processing=self._conn_options.disable_value_processing,
                    secure=self._conn_dto.secure,
                    ssl_ca=self._conn_dto.ssl_ca,
                    ca_data=self._ca_data.decode("ascii"),
                    readonly=self._conn_dto.readonly,
                )
            )
        return dto_pool


@attr.s(cmp=False, hash=False)
class DLClickHouseConnExecutor(_DLBaseClickHouseConnExecutor[DLClickHouseAdapter]):
    TARGET_ADAPTER_CLS = DLClickHouseAdapter


@attr.s(cmp=False, hash=False)
class DLAsyncClickHouseConnExecutor(_DLBaseClickHouseConnExecutor[DLAsyncClickHouseAdapter]):
    TARGET_ADAPTER_CLS = DLAsyncClickHouseAdapter
