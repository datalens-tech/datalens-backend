from __future__ import annotations

from typing import Optional, FrozenSet, Any

import attr

from bi_core.data_source_spec.base import DataSourceSpec
from bi_core.db import IndexInfo
from bi_utils.utils import get_type_full_name


@attr.s
class SQLDataSourceSpecBase(DataSourceSpec):
    db_version: Optional[str] = attr.ib(kw_only=True, default=None)


@attr.s
class SubselectDataSourceSpec(SQLDataSourceSpecBase):
    subsql: Optional[str] = attr.ib(kw_only=True, default=None)


@attr.s
class DbSQLDataSourceSpec(SQLDataSourceSpecBase):
    db_name: Optional[str] = attr.ib(kw_only=True, default=None)


@attr.s
class TableSQLDataSourceSpec(SQLDataSourceSpecBase):
    table_name: Optional[str] = attr.ib(kw_only=True, default=None)

    @property
    def is_configured(self) -> bool:
        return self.table_name is not None


@attr.s
class IndexedSQLDataSourceSpec(SQLDataSourceSpecBase):
    index_info_set: Optional[FrozenSet[IndexInfo]] = attr.ib(kw_only=True, default=None)

    @index_info_set.validator
    def check_indexes(self, attribute: Any, value: Any) -> None:
        if value is None or isinstance(value, frozenset):
            pass
        else:
            raise TypeError(f"Indexes must be a frozen set or non, not {get_type_full_name(type(value))!r}")


@attr.s
class StandardSQLDataSourceSpec(
        DbSQLDataSourceSpec,
        TableSQLDataSourceSpec,
        IndexedSQLDataSourceSpec,
):
    pass


@attr.s
class SchemaSQLDataSourceSpec(SQLDataSourceSpecBase):
    schema_name: Optional[str] = attr.ib(kw_only=True, default=None)


@attr.s
class StandardSchemaSQLDataSourceSpec(
        StandardSQLDataSourceSpec,
        SchemaSQLDataSourceSpec,
):
    pass
