from __future__ import annotations
from typing import TYPE_CHECKING, ClassVar, FrozenSet, Optional, Iterable

import abc
import logging

import sqlalchemy as sa

from bi_constants.enums import JoinType, CreateDSFrom, ConnectionType

from bi_core import exc
from bi_core.base_models import SourceFilterSpec
from bi_core.connectors.clickhouse_base.query_compiler import ClickHouseQueryCompiler
from bi_core.data_source.sql import BaseSQLDataSource, StandardSQLDataSource, SubselectDataSource
from bi_core.data_source_spec.sql import StandardSQLDataSourceSpec


if TYPE_CHECKING:
    from bi_core.db import SchemaColumn, SchemaInfo  # noqa
    from bi_core import us_connection  # noqa
    from bi_core.services_registry import ServicesRegistry


LOGGER = logging.getLogger(__name__)


class CommonClickHouseSubselectDataSource(SubselectDataSource):
    """ Common subselect base for ClickHouse-based databases """
    pass


class ClickHouseBaseMixin(BaseSQLDataSource):
    default_server_version = None
    supported_join_types: ClassVar[FrozenSet[JoinType]] = frozenset({
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
    Data source base class / mixin for actual ClickHouse (i.e. not chyt/chydb).
    """

    conn_type = ConnectionType.clickhouse

    @classmethod
    def is_compatible_with_type(cls, source_type: CreateDSFrom) -> bool:
        return source_type in {
            CreateDSFrom.CH_TABLE,
            CreateDSFrom.CH_SUBSELECT,
            CreateDSFrom.CH_BILLING_ANALYTICS_TABLE,
            CreateDSFrom.CH_USAGE_TRACKING_TABLE,
        }


class ClickHouseDataSourceBase(ActualClickHouseBaseMixin, StandardSQLDataSource, metaclass=abc.ABCMeta):  # type: ignore  # TODO: fix
    """ ClickHouse table. Might not work correctly for views. """


class ClickHouseFilteredDataSourceBase(ClickHouseDataSourceBase, metaclass=abc.ABCMeta):
    preview_enabled: ClassVar[bool] = False

    def _check_db_table_is_allowed(self) -> None:
        if (self.db_name != self.connection.db_name
                or self.table_name not in self.connection.allowed_tables):
            raise exc.SourceDoesNotExist(db_message='', query='')

    def get_filters(self, service_registry: ServicesRegistry) -> Iterable[SourceFilterSpec]:
        self._check_db_table_is_allowed()
        return super().get_filters(service_registry)


class ClickHouseTemplatedSubselectDataSource(ActualClickHouseBaseMixin, CommonClickHouseSubselectDataSource):
    preview_enabled: ClassVar[bool] = True

    @property
    def spec(self) -> StandardSQLDataSourceSpec:   # type: ignore  # TODO: fix
        assert isinstance(self._spec, StandardSQLDataSourceSpec)
        return self._spec

    @property
    def subsql(self) -> Optional[str]:
        ss_template = self.connection.get_subselect_template_by_title(self.spec.table_name)
        parameters = self.connection.subselect_parameters
        LOGGER.debug('Got subselect parameters: %s', parameters)

        return str(
            sa.text(ss_template.sql_query).bindparams(
                **{param.name: param.values for param in parameters}  # TODO: param_type = single_value
            ).compile(
                dialect=self.connection.get_dialect(),
                compile_kwargs={"literal_binds": True}
            )
        )

    def get_parameters(self) -> dict:
        return dict(
            db_name=self.spec.db_name,
            table_name=self.spec.table_name,
        )
