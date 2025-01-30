from typing import TypeVar

import attr

from dl_core.connection_executors.adapters.common_base import CommonBaseDirectAdapter
from dl_core.connection_executors.async_sa_executors import DefaultSqlAlchemyConnExecutor

from dl_connector_trino.core.adapters import TrinoDefaultAdapter
from dl_connector_trino.core.dto import TrinoConnDTO
from dl_connector_trino.core.target_dto import TrinoConnTargetDTO


_BASE_TRINO_ADAPTER_TV = TypeVar("_BASE_TRINO_ADAPTER_TV", bound=CommonBaseDirectAdapter)


@attr.s(cmp=False, hash=False)
class BaseTrinoConnExecutor(DefaultSqlAlchemyConnExecutor[_BASE_TRINO_ADAPTER_TV]):
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
                ssl_ca=self._conn_dto.ssl_ca,
            )
        ]

    # def mutate_for_dashsql(self, db_params: Optional[dict[str, str]] = None) -> Self:
    #     if db_params:  # TODO.
    #         raise Exception("`db_params` are not supported at the moment")
    #     return self.clone(
    #         conn_options=self._conn_options.clone(
    #             disable_value_processing=True,
    #         ),
    #     )


@attr.s(cmp=False, hash=False)
class TrinoConnExecutor(BaseTrinoConnExecutor[TrinoDefaultAdapter]):
    TARGET_ADAPTER_CLS = TrinoDefaultAdapter
