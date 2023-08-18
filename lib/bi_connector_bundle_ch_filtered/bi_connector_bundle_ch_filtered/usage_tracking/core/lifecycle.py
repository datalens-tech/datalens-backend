import logging

from bi_app_tools.profiling_base import GenericProfiler

from bi_cloud_integration.exc import YCBadRequest, YCPermissionDenied
from bi_cloud_integration.model import IAMResource

from bi_api_commons.base_models import IAMAuthData
from bi_core.connectors.base.lifecycle import ConnectionLifecycleManager
from bi_connector_bundle_ch_filtered.usage_tracking.core.us_connection import UsageTrackingConnection
from bi_service_registry_ya_cloud.yc_service_registry import YCServiceRegistry
from bi_core import exc


LOGGER = logging.getLogger(__name__)


class UsageTrackingConnectionLifecycleManager(
        ConnectionLifecycleManager[UsageTrackingConnection]
):
    ENTRY_CLS = UsageTrackingConnection

    async def post_init_async_hook(self) -> None:
        await super().post_init_async_hook()

        sr = self._service_registry
        assert sr.rci.tenant is not None
        tenant_id = sr.rci.tenant.get_tenant_id()

        yc_sr = sr.get_installation_specific_service_registry(YCServiceRegistry)
        yc_as_client = await yc_sr.get_yc_as_client()
        assert yc_as_client is not None
        auth_data = sr.rci.auth_data
        assert isinstance(auth_data, IAMAuthData)

        iam_role = self.entry.required_iam_role
        resource_path = IAMResource.folder(tenant_id)
        with GenericProfiler("check-iam-role"):
            try:
                await yc_as_client.authorize(
                    iam_token=auth_data.iam_token,
                    permission=iam_role,
                    resource_path=[resource_path],
                )
            except (YCBadRequest, YCPermissionDenied) as err:
                LOGGER.error('yc_as auxiliary authorize: %r %r %r', iam_role, resource_path, err)
                raise exc.YCPermissionRequired(f'User does not have the required role: {iam_role}')

        self.entry.tenant_id = tenant_id
