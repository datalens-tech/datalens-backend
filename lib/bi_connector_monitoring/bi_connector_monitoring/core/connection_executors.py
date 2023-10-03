from __future__ import annotations

from typing import (
    TYPE_CHECKING,
    Optional,
    Sequence,
)

import attr

from bi_cloud_integration.service_registry_cloud import BaseCloudServiceRegistry
from dl_core.connection_executors.async_sa_executors import DefaultSqlAlchemyConnExecutor

from bi_connector_monitoring.core.adapter import AsyncMonitoringAdapter
from bi_connector_monitoring.core.target_dto import MonitoringConnTargetDTO


if TYPE_CHECKING:
    from bi_connector_monitoring.core.dto import MonitoringConnDTO


@attr.s(cmp=False, hash=False)
class MonitoringAsyncAdapterConnExecutor(DefaultSqlAlchemyConnExecutor[AsyncMonitoringAdapter]):
    TARGET_ADAPTER_CLS = AsyncMonitoringAdapter

    _conn_dto: MonitoringConnDTO = attr.ib()

    async def _make_target_conn_dto_pool(self) -> Sequence[MonitoringConnTargetDTO]:
        service_account_id = self._conn_dto.service_account_id
        iam_token: Optional[str] = None
        if service_account_id:
            services_registry = self._services_registry  # Do not use. To be deprecated. Somehow.
            if services_registry is None:
                raise Exception("`services_registry` is not available")
            yc_sr = services_registry.get_installation_specific_service_registry(BaseCloudServiceRegistry)
            yc_ts_client = await yc_sr.get_yc_ts_client()
            if yc_ts_client is None:
                raise Exception("`yc_ts_client` is not available")
            with yc_ts_client:
                sa_creds_retriever = yc_sr.get_sa_creds_retriever_factory().get_retriever()
                sa_token = await sa_creds_retriever.get_sa_token()
                yc_ts_client = yc_ts_client.clone(bearer_token=sa_token)
                iam_token = await yc_ts_client.create_iam_token_for_service_account(service_account_id)

        return [
            MonitoringConnTargetDTO(
                conn_id=self._conn_dto.conn_id,
                pass_db_messages_to_user=self._conn_options.pass_db_messages_to_user,
                pass_db_query_to_user=self._conn_options.pass_db_query_to_user,
                host=self._conn_dto.host,
                url_path=self._conn_dto.url_path,
                iam_token=iam_token,
                folder_id=self._conn_dto.folder_id,
            )
        ]
