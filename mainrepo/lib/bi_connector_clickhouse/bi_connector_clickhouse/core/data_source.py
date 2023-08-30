from __future__ import annotations

import logging

from bi_core.connectors.clickhouse_base.data_source import (
    CommonClickHouseSubselectDataSource, ActualClickHouseBaseMixin, ClickHouseDataSourceBase,
)

LOGGER = logging.getLogger(__name__)


class ClickHouseDataSource(ClickHouseDataSourceBase):
    pass


class ClickHouseSubselectDataSource(ActualClickHouseBaseMixin, CommonClickHouseSubselectDataSource):  # type: ignore  # TODO: fix
    """ Clickhouse subselect """
