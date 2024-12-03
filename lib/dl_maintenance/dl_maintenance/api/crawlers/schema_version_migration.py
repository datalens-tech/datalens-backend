from typing import (
    Any,
    AsyncIterable,
    Optional,
)

import attr

from dl_core.us_entry import (
    USEntry,
    USMigrationEntry,
)
from dl_core.us_manager.us_manager_async import AsyncUSManager
from dl_maintenance.core.us_crawler_base import USEntryCrawler


ALLOWED_ENTRY_SCOPES = ("connection", "dataset")


@attr.s
class SchemaVersionCrawler(USEntryCrawler):
    ENTRY_TYPE = USMigrationEntry

    entry_scope: str = attr.ib(kw_only=True, validator=attr.validators.in_(ALLOWED_ENTRY_SCOPES))

    def get_raw_entry_iterator(self, crawl_all_tenants: bool = True) -> AsyncIterable[dict[str, Any]]:
        return self.usm.get_raw_collection(
            entry_scope=self.entry_scope,
            all_tenants=crawl_all_tenants,
        )

    async def process_entry_get_save_flag(
        self, entry: USEntry, logging_extra: dict[str, Any], usm: Optional[AsyncUSManager] = None
    ) -> tuple[bool, str]:
        if entry.data.get("schema_version") is None:
            entry.data["schema_version"] = "1"
            return True, "Schema version is set"

        return False, "Entry skipped"
