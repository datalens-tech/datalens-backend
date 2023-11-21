from __future__ import annotations

from typing import Optional

from dl_core.data_source.sql import BaseSQLDataSource


class YQLDataSourceMixin(BaseSQLDataSource):
    @property
    def db_version(self) -> Optional[str]:
        return None  # not expecting anything useful
