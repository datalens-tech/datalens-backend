from __future__ import annotations

import attr

from dl_core.connection_executors.async_sa_executors import DefaultSqlAlchemyConnExecutor

from dl_connector_mssql.core.adapters_mssql import MSSQLDefaultAdapter
from dl_connector_mssql.core.dto import MSSQLConnDTO
from dl_connector_mssql.core.target_dto import MSSQLConnTargetDTO


@attr.s(cmp=False, hash=False)
class MSSQLConnExecutor(DefaultSqlAlchemyConnExecutor[MSSQLDefaultAdapter]):
    TARGET_ADAPTER_CLS = MSSQLDefaultAdapter

    _conn_dto: MSSQLConnDTO = attr.ib()

    async def _make_target_conn_dto_pool(self) -> list[MSSQLConnTargetDTO]:  # type: ignore  # TODO: fix
        dto_pool = []
        for host in self._conn_hosts_pool:
            dto_pool.append(
                MSSQLConnTargetDTO(
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
