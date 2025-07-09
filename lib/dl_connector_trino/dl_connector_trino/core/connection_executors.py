import attr

from dl_core.connection_executors.async_sa_executors import DefaultSqlAlchemyConnExecutor

from dl_connector_trino.core.adapters import TrinoDefaultAdapter
from dl_connector_trino.core.dto import (
    TrinoConnDTO,
    TrinoConnDTOBase,
)
from dl_connector_trino.core.target_dto import TrinoConnTargetDTO


@attr.s(cmp=False, hash=False)
class TrinoConnExecutorBase(DefaultSqlAlchemyConnExecutor[TrinoDefaultAdapter]):
    TARGET_ADAPTER_CLS = TrinoDefaultAdapter
    _conn_dto: TrinoConnDTOBase = attr.ib()


@attr.s(cmp=False, hash=False)
class TrinoConnExecutor(TrinoConnExecutorBase):
    _conn_dto: TrinoConnDTO = attr.ib()

    async def _make_target_conn_dto_pool(self) -> list[TrinoConnTargetDTO]:
        return [
            TrinoConnTargetDTO(
                conn_id=self._conn_dto.conn_id,
                pass_db_messages_to_user=self._conn_options.pass_db_messages_to_user,
                pass_db_query_to_user=self._conn_options.pass_db_query_to_user,
                host=self._conn_dto.host,
                port=self._conn_dto.port,
                username=self._conn_dto.username,
                auth_type=self._conn_dto.auth_type,
                password=self._conn_dto.password,
                jwt=self._conn_dto.jwt,
                ssl_enable=self._conn_dto.ssl_enable,
                ssl_ca=self._conn_dto.ssl_ca,
            )
        ]
