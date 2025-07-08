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

    def _get_base_target_conn_dto_pool(self, conn_dto: TrinoConnDTO) -> list[TrinoConnTargetDTO]:
        return [
            TrinoConnTargetDTO(
                conn_id=conn_dto.conn_id,
                pass_db_messages_to_user=self._conn_options.pass_db_messages_to_user,
                pass_db_query_to_user=self._conn_options.pass_db_query_to_user,
                host=conn_dto.host,
                port=conn_dto.port,
                username=conn_dto.username,
                auth_type=conn_dto.auth_type,
                password=conn_dto.password,
                jwt=conn_dto.jwt,
                ssl_enable=conn_dto.ssl_enable,
                ssl_ca=conn_dto.ssl_ca,
            )
        ]


@attr.s(cmp=False, hash=False)
class TrinoConnExecutor(TrinoConnExecutorBase):
    _conn_dto: TrinoConnDTO = attr.ib()

    async def _make_target_conn_dto_pool(self) -> list[TrinoConnTargetDTO]:
        return self._get_base_target_conn_dto_pool(self._conn_dto)
