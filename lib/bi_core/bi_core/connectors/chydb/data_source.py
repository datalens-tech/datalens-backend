from __future__ import annotations

import logging
from typing import Any, Callable, ClassVar, Optional, TYPE_CHECKING

import attr

from bi_constants.enums import CreateDSFrom, ConnectionType

from bi_core import exc
from bi_core.connectors.clickhouse_base.query_compiler import ClickHouseQueryCompiler
from bi_core.connection_models import TableDefinition, SATextTableDefinition
from bi_core.data_source.sql import BaseSQLDataSource, StandardSQLDataSource, require_table_name
from bi_core.connectors.clickhouse_base.data_source import CommonClickHouseSubselectDataSource
from bi_core.connectors.chydb.data_source_spec import CHYDBTableDataSourceSpec
from bi_core.utils import sa_plain_text

if TYPE_CHECKING:
    from bi_core.connection_executors.sync_base import SyncConnExecutorBase
    from bi_core.connection_executors.async_base import AsyncConnExecutorBase


LOGGER = logging.getLogger(__name__)


class CHYDBDataSourceMixin(BaseSQLDataSource):
    # JOINs need to be fixed on that side: https://st.yandex-team.ru/KIKIMR-8774

    supports_schema_update: ClassVar[bool] = False  # XXXXX: what does this even do?

    conn_type = ConnectionType.chydb

    @classmethod
    def is_compatible_with_type(cls, source_type: CreateDSFrom) -> bool:
        return source_type in (CreateDSFrom.CHYDB_TABLE, CreateDSFrom.CHYDB_SUBSELECT)


@attr.s
class CHYDBTableDataSource(CHYDBDataSourceMixin, StandardSQLDataSource):
    @property
    def spec(self) -> CHYDBTableDataSourceSpec:
        assert isinstance(self._spec, CHYDBTableDataSourceSpec)
        return self._spec

    @property
    def ydb_cluster(self) -> Optional[str]:
        return self.spec.ydb_cluster

    @property
    def ydb_database(self) -> Optional[str]:
        return self.spec.ydb_database

    def get_parameters(self) -> dict:
        return dict(
            super().get_parameters(),
            ydb_cluster=self.ydb_cluster,
            ydb_database=self.ydb_database,
        )

    default_server_version = None

    compiler_cls = ClickHouseQueryCompiler

    @property
    def default_title(self) -> str:
        return self.table_name.split('/')[-1]  # type: ignore  # TODO: fix

    @staticmethod
    def quote_path(value: str, part: str = 'path') -> str:
        # TODO: use CH quoter if either of these become useful.
        if "'" in value:
            raise exc.TableNameInvalidError(f"Invalid CHYDB {part}: must not contain single quotes.")
        if '"' in value:
            raise exc.TableNameInvalidError(f"Invalid CHYDB {part}: must not contain double quotes.")
        if '\n' in value:
            raise exc.TableNameInvalidError(f"Invalid CHYDB {part}: must not contain newlines.")
        if '\\' in value:
            raise exc.TableNameInvalidError(f"Invalid CHYDB {part}: must not contain backslashes.")
        return value

    @require_table_name
    def get_sql_source(self, alias: str = None) -> Any:
        table_name = self.table_name
        assert table_name
        ydb_cluster = self.ydb_cluster
        ydb_database = self.ydb_database
        # No known useful 'empty' / 'null' distinction, so equating them.
        if ydb_cluster and ydb_database:
            from_sql = "ydbTable('{}', '{}', '{}')".format(
                self.quote_path(ydb_cluster, part='cluster'),
                self.quote_path(ydb_database, part='database'),
                self.quote_path(table_name, part='table'))
        elif ydb_cluster or ydb_database:  # one missing
            raise ValueError("`ydb_cluster` and `ydb_database` should only be specified together")
        else:
            assert not ydb_cluster
            assert not ydb_database
            from_sql = "ydbTable('{}')".format(
                self.quote_path(table_name, part='table'))  # type: ignore  # TODO: fix
        if alias:
            from_sql = '{} as "{}"'.format(from_sql, self.quote_path(alias, part='alias'))
        return sa_plain_text(from_sql)

    @require_table_name
    def get_table_definition(self) -> TableDefinition:
        return SATextTableDefinition(text=self.get_sql_source())

    def _check_existence(self, conn_executor_factory: Callable[[], SyncConnExecutorBase]) -> bool:
        return True

    async def _check_existence_async(self, conn_executor_factory: Callable[[], AsyncConnExecutorBase]) -> bool:
        return True


class CHYDBSubselectDataSource(CHYDBDataSourceMixin, CommonClickHouseSubselectDataSource):
    """ CHYDB Subselect """
