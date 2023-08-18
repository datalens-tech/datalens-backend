from __future__ import annotations

from typing import TYPE_CHECKING, Optional, Type

from bi_core.data_source.sql import BaseSQLDataSource
from bi_connector_yql.core.yql_base.query_compiler import YQLQueryCompiler

if TYPE_CHECKING:
    from bi_core.connectors.base.query_compiler import QueryCompiler


class YQLDataSourceMixin(BaseSQLDataSource):
    compiler_cls: Type[QueryCompiler] = YQLQueryCompiler

    @property
    def db_version(self) -> Optional[str]:
        return None  # not expecting anything useful
