from __future__ import annotations

import attr

from dl_core.data_source_spec.sql import (
    SQLDataSourceSpecBase,
    SubselectDataSourceSpec,
    TableSQLDataSourceSpec,
)


@attr.s
class BigQueryDataSourceSpecMixin(SQLDataSourceSpecBase):
    pass


@attr.s
class BigQueryTableDataSourceSpec(BigQueryDataSourceSpecMixin, TableSQLDataSourceSpec):
    dataset_name: str = attr.ib(kw_only=True)


@attr.s
class BigQuerySubselectDataSourceSpec(BigQueryDataSourceSpecMixin, SubselectDataSourceSpec):
    subsql: str | None = attr.ib(kw_only=True, default=None)
