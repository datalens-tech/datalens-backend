from __future__ import annotations

from dl_core.data_source.sql import BaseSQLDataSource


class YQLDataSourceMixin(BaseSQLDataSource):
    @property
    def db_version(self) -> str | None:
        return None  # not expecting anything useful
