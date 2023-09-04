from __future__ import annotations

import logging
from typing import Any, AsyncIterable, Optional

import attr

from bi_maintenance.core.us_crawler_base import USEntryCrawler
from bi_core.us_entry import USEntry, USMigrationEntry
from bi_core.us_manager.us_manager_async import AsyncUSManager

LOGGER = logging.getLogger(__name__)


@attr.s
class RotateCryptoKeyInConnection(USEntryCrawler):
    ENTRY_TYPE = USMigrationEntry

    def get_raw_entry_iterator(self, crawl_all_tenants: bool) -> AsyncIterable[dict[str, Any]]:
        return self.usm.get_raw_collection(entry_scope='connection', all_tenants=crawl_all_tenants)

    async def process_entry_get_save_flag(self, entry: USEntry, logging_extra: dict[str, Any], usm: Optional[AsyncUSManager] = None) -> tuple[bool, str]:
        assert isinstance(entry, USMigrationEntry)
        fields_key_info = self.usm.get_sensitive_fields_key_info(entry)
        if len(fields_key_info) == 0:
            return False, "Entry does not contains sensitive fields"

        fields_with_non_actual_crypto_keys = [
            field_name for field_name, key_info in fields_key_info.items()
            if key_info is not None
               and key_info.key_id is not None  # Case if in versioned data and value of field is None
               and key_info.key_id != self.usm.actual_crypto_key_id
        ]
        fields_with_non_actual_crypto_keys.sort()

        missing_fields = [
            field_name for field_name, key_info in fields_key_info.items()
            if key_info is None
        ]

        if missing_fields:
            LOGGER.warning(
                "US entry has missing sensitive fields: %s %s",
                entry.uuid, missing_fields,
                extra=logging_extra
            )

        if len(fields_with_non_actual_crypto_keys) == 0:
            return False, "All sensitive fields are encrypted with actual keys"

        self.usm.actualize_crypto_keys(entry)
        return True, f"Some sensitive fields are encrypted with not actual keys: {fields_with_non_actual_crypto_keys}"
