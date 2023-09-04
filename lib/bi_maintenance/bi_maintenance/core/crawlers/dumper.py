from __future__ import annotations

import json
import logging
from typing import TYPE_CHECKING, Any, AsyncIterable, Optional

import attr

from bi_maintenance.core.us_crawler_base import USEntryCrawler
from bi_core.us_entry import USMigrationEntry
from bi_core.us_manager.us_manager_async import AsyncUSManager

if TYPE_CHECKING:
    from bi_core.us_entry import USEntry


LOGGER = logging.getLogger(__name__)


@attr.s
class USEntryDumperCrawler(USEntryCrawler):
    ENTRY_TYPE = USMigrationEntry

    _dry_run: bool = attr.ib(default=True)  # no point in locking the entries for this
    output_filename: str = attr.ib(default='us_entries_dump.ndjson')
    scope: str = attr.ib(default='dataset')  # connection, dataset, widget, dash

    def get_raw_entry_iterator(
            self, crawl_all_tenants: bool = True
    ) -> AsyncIterable[dict[str, Any]]:
        return self.usm.get_raw_collection(entry_scope=self.scope, all_tenants=crawl_all_tenants)

    async def process_entry_get_save_flag(
            self,
            entry: USEntry,
            logging_extra: Optional[dict[str, Any]] = None,
            usm: Optional[AsyncUSManager] = None
    ) -> tuple[bool, str]:
        full_data = entry._us_resp
        full_data_s = json.dumps(full_data) + '\n'
        with open(self.output_filename, 'a', 1) as fobj:
            fobj.write(full_data_s)
        return False, ''
