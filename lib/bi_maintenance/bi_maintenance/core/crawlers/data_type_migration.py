import abc
from typing import (
    Any,
    AsyncIterable,
    Optional,
)

import attr

from bi_maintenance.core.us_crawler_base import USEntryCrawler
from dl_core.us_entry import (
    USEntry,
    USMigrationEntry,
)
from dl_core.us_manager.us_manager_async import AsyncUSManager

ALLOWED_CONNECTIONS = ("file", "gsheets_v2")


class DataTypeMigrationCrawler(USEntryCrawler, metaclass=abc.ABCMeta):
    _data_type_map: dict[str, str] = attr.ib(factory=dict)  # usually BIType.name


@attr.s
class DatasetDataTypeMigrationCrawler(DataTypeMigrationCrawler):
    ENTRY_TYPE = USMigrationEntry

    def get_raw_entry_iterator(self, crawl_all_tenants: bool = True) -> AsyncIterable[dict[str, Any]]:
        return self.usm.get_raw_collection(entry_scope="dataset", all_tenants=crawl_all_tenants)

    async def process_entry_get_save_flag(
        self, entry: USEntry, logging_extra: dict[str, Any], usm: Optional[AsyncUSManager] = None
    ) -> tuple[bool, str]:
        result = False
        for data_field in entry.data.result_schema:
            for type_field in ("initial_data_type", "cast", "data_type"):
                if type_field not in data_field:
                    continue

                to_type = self._data_type_map.get(data_field[type_field])
                if to_type is not None:
                    data_field[type_field] = to_type
                    result = True

        return result, ""


@attr.s
class ConnectionDataTypeMigrationCrawler(DataTypeMigrationCrawler):
    ENTRY_TYPE = USMigrationEntry

    _connection_type: str = attr.ib(kw_only=True, validator=attr.validators.in_(ALLOWED_CONNECTIONS))

    def get_raw_entry_iterator(self, crawl_all_tenants: bool = True) -> AsyncIterable[dict[str, Any]]:
        return self.usm.get_raw_collection(
            entry_scope="connection",
            entry_type=self._connection_type,
            all_tenants=crawl_all_tenants,
        )

    async def process_entry_get_save_flag(
        self, entry: USEntry, logging_extra: dict[str, Any], usm: Optional[AsyncUSManager] = None
    ) -> tuple[bool, str]:
        result = False
        for source in entry.data.sources:
            raw_schema = source.get("raw_schema") or []
            for data_field in raw_schema:
                if "user_type" not in data_field:
                    continue
                to_type = self._data_type_map.get(data_field["user_type"])
                if to_type is not None:
                    data_field["user_type"] = to_type
                    result = True

        return result, ""
