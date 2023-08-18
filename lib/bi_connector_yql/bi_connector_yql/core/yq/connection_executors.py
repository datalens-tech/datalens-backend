from __future__ import annotations

import json
from typing import TYPE_CHECKING, Sequence

import attr

from bi_cloud_integration.model import IAMServiceAccount

from bi_core.connection_executors.async_sa_executors import DefaultSqlAlchemyConnExecutor
from bi_connector_yql.core.yq.adapter import YQAdapter
from bi_connector_yql.core.yq.target_dto import YQConnTargetDTO
from bi_service_registry_ya_cloud.yc_service_registry import YCServiceRegistry

if TYPE_CHECKING:
    from bi_connector_yql.core.yq.dto import YQConnDTO


@attr.s(cmp=False, hash=False)
class YQAsyncAdapterConnExecutor(DefaultSqlAlchemyConnExecutor[YQAdapter]):
    TARGET_ADAPTER_CLS = YQAdapter

    _conn_dto: YQConnDTO = attr.ib()

    async def _make_target_conn_dto_pool(self) -> Sequence[YQConnTargetDTO]:
        service_account_id = self._conn_dto.service_account_id
        folder_id = self._conn_dto.folder_id
        key_data_s = self._conn_dto.password

        services_registry = self._services_registry   # Do not use. To be deprecated. Somehow.
        if services_registry is None:
            raise Exception("`services_registry` is not available")
        yc_sr = services_registry.get_installation_specific_service_registry(YCServiceRegistry)
        if service_account_id:
            yc_ts_client = await yc_sr.get_yc_ts_client()
            if yc_ts_client is None:
                raise Exception("`yc_ts_client` is not available")
            with yc_ts_client:
                sa_creds_retriever = yc_sr.get_sa_creds_retriever_factory().get_retriever()
                sa_token = await sa_creds_retriever.get_sa_token()
                yc_ts_client = yc_ts_client.clone(bearer_token=sa_token)
                user_sa_iam_token = await yc_ts_client.create_iam_token_for_service_account(service_account_id)
        elif key_data_s:
            key_data = json.loads(key_data_s)
            yc_ts_client = await yc_sr.get_yc_ts_client()
            if yc_ts_client is None:
                raise Exception("`yc_ts_client` is not available")
            with yc_ts_client:
                user_sa_iam_token = await yc_ts_client.create_token(
                    service_account_id=key_data['service_account_id'],
                    key_id=key_data.get('key_id') or key_data['id'],
                    private_key=key_data['private_key'],
                    expiration=3600,
                )
        else:
            raise Exception("Either `password` or `service_account_id` must be specified.")

        if not folder_id:
            # Token -> folder_id:
            # Hopefully, this is not permanent.
            yc_as_client = await yc_sr.get_yc_as_client()
            if yc_as_client is None:
                raise Exception("`yc_as_client` is not available")
            with yc_as_client:
                authenticate_res = await yc_as_client.authenticate(iam_token=user_sa_iam_token)
            assert isinstance(authenticate_res, IAMServiceAccount)
            folder_id = authenticate_res.folder_id
            # Alternative:
            # https://bb.yandex-team.ru/projects/CLOUD/repos/cloud-go/browse/private-api/yandex/cloud/priv/iam/v1/service_account_service.proto#19

        # folder_id -> cloud_id:
        # https://bb.yandex-team.ru/projects/CLOUD/repos/cloud-go/browse/private-api/yandex/cloud/priv/resourcemanager/v1/folder_service.proto#28
        yc_fs_client = await yc_sr.get_yc_fs_client(bearer_token=user_sa_iam_token)
        if yc_fs_client is None:
            raise Exception("`yc_fs_client` is not available")
        with yc_fs_client:
            cloud_id = await yc_fs_client.resolve_folder_id_to_cloud_id(folder_id)

        return [YQConnTargetDTO(
            conn_id=self._conn_dto.conn_id,
            pass_db_messages_to_user=self._conn_options.pass_db_messages_to_user,
            pass_db_query_to_user=self._conn_options.pass_db_query_to_user,
            host=self._conn_dto.host,
            port=self._conn_dto.port,
            db_name=self._conn_dto.db_name,
            username='',
            password=user_sa_iam_token,
            cloud_id=cloud_id,
            folder_id=folder_id,
        )]
