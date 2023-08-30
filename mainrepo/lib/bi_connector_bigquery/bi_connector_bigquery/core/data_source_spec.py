from __future__ import annotations

from typing import Optional

import attr

from bi_core.data_source_spec.sql import (
    SQLDataSourceSpecBase, TableSQLDataSourceSpec, SubselectDataSourceSpec,
)


@attr.s
class BigQueryDataSourceSpecMixin(SQLDataSourceSpecBase):
    pass


@attr.s
class BigQueryTableDataSourceSpec(BigQueryDataSourceSpecMixin, TableSQLDataSourceSpec):
    dataset_name: str = attr.ib(kw_only=True)


@attr.s
class BigQuerySubselectDataSourceSpec(BigQueryDataSourceSpecMixin, SubselectDataSourceSpec):
    subsql: Optional[str] = attr.ib(kw_only=True, default=None)
