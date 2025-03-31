from __future__ import annotations


import attr

from dl_core.connection_executors.async_sa_executors import DefaultSqlAlchemyConnExecutor

from dl_connector_bigquery.core.adapters import BigQueryDefaultAdapter
from dl_connector_bigquery.core.dto import BigQueryConnDTO
from dl_connector_bigquery.core.target_dto import BigQueryConnTargetDTO


@attr.s(cmp=False, hash=False)
class BigQueryAsyncConnExecutor(DefaultSqlAlchemyConnExecutor[BigQueryDefaultAdapter]):
    TARGET_ADAPTER_CLS = BigQueryDefaultAdapter

    _conn_dto: BigQueryConnDTO = attr.ib()

    async def _make_target_conn_dto_pool(self) -> list[BigQueryConnTargetDTO]:
        dto_pool: list[BigQueryConnTargetDTO] = []
        dto_pool.append(
            BigQueryConnTargetDTO(
                conn_id=self._conn_dto.conn_id,
                pass_db_messages_to_user=self._conn_options.pass_db_messages_to_user,
                pass_db_query_to_user=self._conn_options.pass_db_query_to_user,
                project_id=self._conn_dto.project_id,
                credentials=self._conn_dto.credentials,
            )
        )
        return dto_pool
