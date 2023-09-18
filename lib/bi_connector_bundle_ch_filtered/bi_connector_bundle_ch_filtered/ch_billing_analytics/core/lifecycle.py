from dl_app_tools.profiling_base import GenericProfiler

from dl_core.connectors.base.lifecycle import ConnectionLifecycleManager
from bi_connector_bundle_ch_filtered.ch_billing_analytics.core.us_connection import BillingAnalyticsCHConnection
from bi_service_registry_ya_cloud.yc_service_registry import YCServiceRegistry


class BillingAnalyticsCHConnectionLifecycleManager(
        ConnectionLifecycleManager[BillingAnalyticsCHConnection]
):
    ENTRY_CLS = BillingAnalyticsCHConnection

    async def post_init_async_hook(self) -> None:
        await super().post_init_async_hook()

        sr = self._service_registry
        yc_sr = sr.get_installation_specific_service_registry(YCServiceRegistry)
        yc_billing_cli = await yc_sr.get_yc_billing_client()
        with GenericProfiler("list-billing-accounts"):
            if yc_billing_cli is None:
                self.entry.billing_accounts = []  # keep old behavior of sync api
            else:
                self.entry.billing_accounts = await yc_billing_cli.list_subject_billing_accounts(iam_uid=sr.rci.user_id)  # type: ignore
