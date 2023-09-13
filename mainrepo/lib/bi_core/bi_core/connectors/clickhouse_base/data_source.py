from __future__ import annotations
from typing import TYPE_CHECKING, ClassVar

import abc
import logging

from bi_constants.enums import JoinType, ConnectionType

from bi_core.data_source.sql import BaseSQLDataSource, StandardSQLDataSource, SubselectDataSource

from bi_core.connectors.clickhouse_base.query_compiler import ClickHouseQueryCompiler


if TYPE_CHECKING:
    from bi_core.db import SchemaColumn, SchemaInfo  # noqa
    from bi_core import us_connection  # noqa


LOGGER = logging.getLogger(__name__)


class CommonClickHouseSubselectDataSource(SubselectDataSource):
    """ Common subselect base for ClickHouse-based databases """
    pass


class ClickHouseBaseMixin(BaseSQLDataSource):
    default_server_version = None
    supported_join_types: ClassVar[frozenset[JoinType]] = frozenset({
        JoinType.inner,
        JoinType.left,
        JoinType.full,
        JoinType.right,
    })
    compiler_cls = ClickHouseQueryCompiler

    def get_connect_args(self) -> dict:
        return dict(super().get_connect_args(), server_version=self.db_version or self.default_server_version)  # type: ignore  # TODO: fix  # noqa


class ActualClickHouseBaseMixin(ClickHouseBaseMixin):
    """
    Data source base class / mixin for actual ClickHouse (i.e. not chyt).
    """

    conn_type = ConnectionType.clickhouse


class ClickHouseDataSourceBase(ActualClickHouseBaseMixin, StandardSQLDataSource, metaclass=abc.ABCMeta):  # type: ignore  # TODO: fix
    """ ClickHouse table. Might not work correctly for views. """
