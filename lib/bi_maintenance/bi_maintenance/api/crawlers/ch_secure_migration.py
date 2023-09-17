from typing import Any, AsyncIterable, Optional

import attr

from bi_maintenance.core.us_crawler_base import USEntryCrawler
from bi_core.us_entry import USEntry, USMigrationEntry
from bi_core.us_manager.us_manager_async import AsyncUSManager

from bi_connector_clickhouse.core.clickhouse_base.constants import CONNECTION_TYPE_CLICKHOUSE


@attr.s
class CHTlsEnablingCrawler(USEntryCrawler):
    ENTRY_TYPE = USMigrationEntry

    def get_raw_entry_iterator(self, crawl_all_tenants: bool = True) -> AsyncIterable[dict[str, Any]]:
        assert self._usm is not None
        return self._usm.get_raw_collection(
            entry_scope='connection',
            entry_type=CONNECTION_TYPE_CLICKHOUSE,
            all_tenants=crawl_all_tenants,
        )

    async def process_entry_get_save_flag(self, entry: USEntry, logging_extra: dict[str, Any],
                                          usm: Optional[AsyncUSManager] = None) -> tuple[bool, str]:
        if entry.data['port'] in (8443, 443):
            entry.data['secure'] = True
            return True, 'TLS enabled'

        return False, 'Connection skipped'
