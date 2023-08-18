from __future__ import annotations

import attr

from bi_connector_metrica.core.adapters_metrica_x import (
    MetricaAPIDefaultAdapter,
    AppMetricaAPIDefaultAdapter,
)
from bi_core.connection_executors.async_sa_executors import DefaultSqlAlchemyConnExecutor
from bi_connector_metrica.core.target_dto import MetricaAPIConnTargetDTO, AppMetricaAPIConnTargetDTO
from bi_connector_metrica.core.dto import MetricaAPIConnDTO, AppMetricaAPIConnDTO


@attr.s(cmp=False, hash=False)
class MetricaAPIConnExecutor(DefaultSqlAlchemyConnExecutor[MetricaAPIDefaultAdapter]):
    TARGET_ADAPTER_CLS = MetricaAPIDefaultAdapter

    _conn_dto: MetricaAPIConnDTO = attr.ib()

    async def _make_target_conn_dto_pool(self) -> list[MetricaAPIConnTargetDTO]:
        return [MetricaAPIConnTargetDTO(
            conn_id=self._conn_dto.conn_id,
            pass_db_messages_to_user=self._conn_options.pass_db_messages_to_user,
            pass_db_query_to_user=self._conn_options.pass_db_query_to_user,
            accuracy=self._conn_dto.accuracy,
            token=self._conn_dto.token,
        )]


@attr.s(cmp=False, hash=False)
class AppMetricaAPIConnExecutor(DefaultSqlAlchemyConnExecutor[AppMetricaAPIDefaultAdapter]):
    TARGET_ADAPTER_CLS = AppMetricaAPIDefaultAdapter

    _conn_dto: AppMetricaAPIConnDTO = attr.ib()

    async def _make_target_conn_dto_pool(self) -> list[AppMetricaAPIConnTargetDTO]:
        return [AppMetricaAPIConnTargetDTO(
            conn_id=self._conn_dto.conn_id,
            pass_db_messages_to_user=self._conn_options.pass_db_messages_to_user,
            pass_db_query_to_user=self._conn_options.pass_db_query_to_user,
            accuracy=self._conn_dto.accuracy,
            token=self._conn_dto.token,
        )]
