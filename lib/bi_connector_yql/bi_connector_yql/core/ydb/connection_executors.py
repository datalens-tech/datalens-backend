from __future__ import annotations

from typing import TYPE_CHECKING, Optional, Sequence

import attr

from bi_core.connection_executors.async_sa_executors import DefaultSqlAlchemyConnExecutor
from bi_connector_yql.core.ydb.adapter import YDBAdapter
from bi_connector_yql.core.ydb.target_dto import YDBConnTargetDTO
from bi_service_registry_ya_cloud.yc_service_registry import YCServiceRegistry

if TYPE_CHECKING:
    from bi_connector_yql.core.ydb.dto import YDBConnDTO  # type: ignore  # TODO: fix


@attr.s(cmp=False, hash=False)
class YDBAsyncAdapterConnExecutor(DefaultSqlAlchemyConnExecutor[YDBAdapter]):
    TARGET_ADAPTER_CLS = YDBAdapter

    _conn_dto: YDBConnDTO = attr.ib()

    async def _make_target_conn_dto_pool(self) -> Sequence[YDBConnTargetDTO]:
        service_account_id = self._conn_dto.service_account_id
        iam_token: Optional[str] = None
        if service_account_id:
            services_registry = self._services_registry   # Do not use. To be deprecated. Somehow.
            if services_registry is None:
                raise Exception("`services_registry` is not available")
            yc_sr = services_registry.get_installation_specific_service_registry(YCServiceRegistry)
            yc_ts_client = await yc_sr.get_yc_ts_client()
            if yc_ts_client is None:
                raise Exception("`yc_ts_client` is not available")
            with yc_ts_client:
                yc_ts_client = await yc_ts_client.ensure_fresh_token()
                iam_token = await yc_ts_client.create_iam_token_for_service_account(service_account_id)

        return [YDBConnTargetDTO(
            conn_id=self._conn_dto.conn_id,
            pass_db_messages_to_user=self._conn_options.pass_db_messages_to_user,
            pass_db_query_to_user=self._conn_options.pass_db_query_to_user,
            host=self._conn_dto.host,
            port=self._conn_dto.port,
            db_name=self._conn_dto.db_name,
            username=self._conn_dto.username or "",
            password=self._conn_dto.password or "",
            is_cloud=self._conn_options.is_cloud,  # type: ignore  # TODO: fix
            iam_token=iam_token,
        )]
