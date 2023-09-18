from bi_service_registry_ya_cloud.yc_service_registry import YCServiceRegistry
from dl_core.connectors.base.lifecycle import ConnectionLifecycleManager

from bi_connector_bundle_ch_filtered_ya_cloud.base.core.us_connection import ConnectionCHFilteredSubselectByPuidBase


class CHFilteredSubselectByPuidBaseConnectionLifecycleManager(
    ConnectionLifecycleManager[ConnectionCHFilteredSubselectByPuidBase]
):
    ENTRY_CLS = ConnectionCHFilteredSubselectByPuidBase

    async def post_init_async_hook(self) -> None:
        await super().post_init_async_hook()

        yc_sr = self._service_registry.get_installation_specific_service_registry(YCServiceRegistry)
        bb_cli = yc_sr.get_blackbox_client()
        assert bb_cli is not None
        self.entry.passport_user_id = await bb_cli.get_user_id_by_oauth_token_async(self.entry.data.token)
