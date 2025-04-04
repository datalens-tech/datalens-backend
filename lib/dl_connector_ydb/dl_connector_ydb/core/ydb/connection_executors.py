from __future__ import annotations

from typing import (
    TYPE_CHECKING,
    Sequence,
)

import attr

from dl_core.connection_executors.async_sa_executors import DefaultSqlAlchemyConnExecutor

from dl_connector_ydb.core.ydb.adapter import YDBAdapter
from dl_connector_ydb.core.ydb.target_dto import YDBConnTargetDTO


if TYPE_CHECKING:
    from dl_connector_ydb.core.ydb.dto import YDBConnDTO


@attr.s(cmp=False, hash=False)
class YDBAsyncAdapterConnExecutor(DefaultSqlAlchemyConnExecutor[YDBAdapter]):
    TARGET_ADAPTER_CLS = YDBAdapter

    _conn_dto: YDBConnDTO = attr.ib()

    async def _make_target_conn_dto_pool(self) -> Sequence[YDBConnTargetDTO]:
        return [
            YDBConnTargetDTO(
                conn_id=self._conn_dto.conn_id,
                pass_db_messages_to_user=self._conn_options.pass_db_messages_to_user,
                pass_db_query_to_user=self._conn_options.pass_db_query_to_user,
                host=self._conn_dto.host,
                port=self._conn_dto.port,
                db_name=self._conn_dto.db_name,
                username=self._conn_dto.username or "",
                password=self._conn_dto.password or "",
                auth_type=self._conn_dto.auth_type,
                ssl_enable=self._conn_dto.ssl_enable,
                ssl_ca=self._conn_dto.ssl_ca,
            )
        ]
