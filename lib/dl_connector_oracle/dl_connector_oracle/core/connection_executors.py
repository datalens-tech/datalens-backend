from __future__ import annotations

import attr

from dl_core.connection_executors.async_sa_executors import DefaultSqlAlchemyConnExecutor

from dl_connector_oracle.core.adapters_oracle import OracleDefaultAdapter
from dl_connector_oracle.core.dto import OracleConnDTO
from dl_connector_oracle.core.target_dto import OracleConnTargetDTO


@attr.s(cmp=False, hash=False)
class OracleDefaultConnExecutor(DefaultSqlAlchemyConnExecutor[OracleDefaultAdapter]):
    TARGET_ADAPTER_CLS = OracleDefaultAdapter

    _conn_dto: OracleConnDTO = attr.ib()

    async def _make_target_conn_dto_pool(self) -> list[OracleConnTargetDTO]:  # type: ignore  # TODO: fix
        dto_pool = []
        for host in self._conn_hosts_pool:
            dto_pool.append(
                OracleConnTargetDTO(
                    conn_id=self._conn_dto.conn_id,
                    pass_db_messages_to_user=self._conn_options.pass_db_messages_to_user,
                    pass_db_query_to_user=self._conn_options.pass_db_query_to_user,
                    host=host,
                    port=self._conn_dto.port,
                    db_name=self._conn_dto.db_name,
                    db_name_type=self._conn_dto.db_name_type,
                    username=self._conn_dto.username,
                    password=self._conn_dto.password,
                    ssl_enable=self._conn_dto.ssl_enable,
                    ssl_ca=self._conn_dto.ssl_ca,
                )
            )
        return dto_pool
