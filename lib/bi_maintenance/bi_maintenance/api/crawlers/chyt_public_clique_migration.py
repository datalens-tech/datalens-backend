from typing import Any, AsyncIterable, Optional

import attr

from bi_constants.enums import ConnectionType

from bi_maintenance.core.us_crawler_base import USEntryCrawler
from bi_core.us_entry import USEntry, USMigrationEntry
from bi_core.us_manager.us_manager_async import AsyncUSManager


@attr.s
class CHYTPublicCliqueCrawler(USEntryCrawler):
    ENTRY_TYPE = USMigrationEntry

    _old_clique: str = attr.ib(default='*ch_public')
    _new_clique: str = attr.ib(default='*ch_datalens')

    async def get_raw_entry_iterator(self, crawl_all_tenants: bool = True) -> AsyncIterable[dict[str, Any]]:
        async for entry in self.usm.get_raw_collection(
            entry_scope='connection',
            entry_type=ConnectionType.ch_over_yt.name,
            all_tenants=crawl_all_tenants,
        ):
            yield entry

        async for entry in self.usm.get_raw_collection(
            entry_scope='connection',
            entry_type=ConnectionType.ch_over_yt_user_auth.name,
            all_tenants=crawl_all_tenants,
        ):
            yield entry

    async def process_entry_get_save_flag(self, entry: USEntry, logging_extra: dict[str, Any], usm: Optional[AsyncUSManager] = None) -> tuple[bool, str]:
        if entry.data['alias'] == self._old_clique:
            entry.data['alias'] = self._new_clique
            return True, 'Clique replaced'

        return False, 'Connection skipped'
