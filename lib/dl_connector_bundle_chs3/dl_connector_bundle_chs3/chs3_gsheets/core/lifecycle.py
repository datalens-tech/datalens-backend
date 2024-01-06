import datetime
from typing import ClassVar

from dl_constants.enums import FileProcessingStatus
from dl_core.connectors.base.lifecycle import ConnectionLifecycleManager
from dl_core.reporting.notifications import get_notification_record
from dl_core.utils import (
    make_user_auth_cookies,
    make_user_auth_headers,
)

from dl_connector_bundle_chs3.chs3_base.core.constants import NOTIF_TYPE_STALE_DATA
from dl_connector_bundle_chs3.chs3_base.core.lifecycle import BaseFileS3ConnectionLifecycleManager
from dl_connector_bundle_chs3.chs3_gsheets.core.us_connection import GSheetsFileS3Connection


class GSheetsFileS3ConnectionLifecycleManager(
    BaseFileS3ConnectionLifecycleManager,
    ConnectionLifecycleManager[GSheetsFileS3Connection],
):
    ENTRY_CLS = GSheetsFileS3Connection

    STALE_THRESHOLD_SECONDS: ClassVar[int] = 30 * 60

    async def post_exec_async_hook(self) -> None:
        await super().post_exec_async_hook()

        assert isinstance(self.entry, GSheetsFileS3Connection)
        assert isinstance(self.entry.data, GSheetsFileS3Connection.DataModel)
        data = self.entry.data

        if not data.refresh_enabled:
            return

        dt_now = datetime.datetime.now(datetime.timezone.utc)

        data_updated_at_all = data.oldest_data_update_time()
        if (
            data_updated_at_all is not None
            and (dt_now - data_updated_at_all).total_seconds() >= self.STALE_THRESHOLD_SECONDS
        ):
            reporting_registry = self._service_registry.get_reporting_registry()
            reporting_registry.save_reporting_record(get_notification_record(NOTIF_TYPE_STALE_DATA))

        data_updated_at = data.oldest_data_update_time(exclude_statuses={FileProcessingStatus.in_progress})
        if data_updated_at is None or (dt_now - data_updated_at).total_seconds() < self.STALE_THRESHOLD_SECONDS:
            return

        fu_client_factory = self._service_registry.get_file_uploader_client_factory()
        rci = self._us_manager.bi_context
        headers = make_user_auth_headers(rci=rci)
        cookies = make_user_auth_cookies(rci=rci)

        sources = [src.get_desc() for src in data.sources]
        assert self.entry.uuid is not None
        async with fu_client_factory.get_client(headers=headers, cookies=cookies) as fu_client:
            await fu_client.update_connection_data_internal(
                conn_id=self.entry.uuid,
                sources=sources,
                authorized=self.entry.authorized,
                tenant_id=rci.tenant.get_tenant_id() if rci.tenant is not None else None,
            )
