from __future__ import annotations

import abc
import collections
import logging
from typing import (
    TYPE_CHECKING,
    Callable,
    Optional,
)
import urllib.parse

import attr
import sqlalchemy as sa
from sqlalchemy.sql.elements import ClauseElement

from dl_constants.enums import (
    DataSourceType,
    UserDataType,
)
from dl_core import exc
from dl_core.connection_models.common_models import (
    SATextTableDefinition,
    TableDefinition,
    TableIdent,
)
from dl_core.data_source.sql import (
    BaseSQLDataSource,
    TableSQLDataSourceMixin,
    require_table_name,
)
from dl_core.db import (
    SchemaColumn,
    SchemaInfo,
)
from dl_core.db.native_type import ClickHouseNativeType
from dl_sqlalchemy_chyt import (
    CHYTTablesConcat,
    CHYTTablesRange,
    CHYTTableSubselect,
)

from dl_connector_chyt.core.constants import (
    CONNECTION_TYPE_CHYT,
    SOURCE_TYPE_CHYT_YTSAURUS_SUBSELECT,
    SOURCE_TYPE_CHYT_YTSAURUS_TABLE,
    SOURCE_TYPE_CHYT_YTSAURUS_TABLE_LIST,
    SOURCE_TYPE_CHYT_YTSAURUS_TABLE_RANGE,
)
from dl_connector_chyt.core.data_source_spec import (
    CHYTTableDataSourceSpec,
    CHYTTableListDataSourceSpec,
    CHYTTableRangeDataSourceSpec,
)
from dl_connector_clickhouse.core.clickhouse_base.data_source import (
    ClickHouseBaseMixin,
    CommonClickHouseSubselectDataSource,
)


if TYPE_CHECKING:
    from dl_core.connection_executors.sync_base import SyncConnExecutorBase


LOGGER = logging.getLogger(__name__)


class CHYTDataSourceBaseMixin(ClickHouseBaseMixin):
    @staticmethod
    def normalize_path(value: str) -> str:
        """
        ...


        >>> CHYTDataSourceBaseMixin.normalize_path('\\n //a / b/c ')
        '//a / b/c '
        >>> CHYTDataSourceBaseMixin.normalize_path('https://host.net/cluster/navigation?path=//a%20/ b/c+&offsetMode=row')
        '//a / b/c '
        """
        raw_value = value.lstrip()
        if raw_value.startswith("//"):
            return raw_value.rstrip("/")
        parsed_url = urllib.parse.urlparse(raw_value)
        parsed_query = urllib.parse.parse_qs(parsed_url.query)
        res = parsed_query.get("path", [None])[0]  # type: ignore  # 2024-01-30 # TODO: List item 0 has incompatible type "None"; expected "str"  [list-item]
        if res:
            return res  # No extra normalization
        raise exc.TableNameInvalidError("Invalid CHYT table path")


class BaseCHYTTableDataSource(CHYTDataSourceBaseMixin, TableSQLDataSourceMixin, abc.ABC):
    @property
    def spec(self) -> CHYTTableDataSourceSpec:
        assert isinstance(self._spec, CHYTTableDataSourceSpec)
        return self._spec

    @property
    def default_title(self) -> str:
        assert self.spec.table_name is not None
        return self.spec.table_name.split("/")[-1]

    @require_table_name
    def get_sql_source(self, alias: Optional[str] = None) -> ClauseElement:
        if alias:
            return sa.alias(self.get_sql_source(), name=alias)
        path = self.spec.table_name
        assert path is not None
        path = self.normalize_path(path)
        return sa.table(path)

    @require_table_name
    def get_table_definition(self) -> TableDefinition:
        table_name = self.table_name
        assert table_name is not None
        return TableIdent(db_name=None, schema_name=None, table_name=table_name)


class BaseCHYTSpecialDataSource(CHYTDataSourceBaseMixin, BaseSQLDataSource, abc.ABC):
    """A common base class for CHYT custom sources (like table funcs or subselects)"""

    @property
    def default_title(self) -> str:
        raise NotImplementedError

    def get_sql_source(self, alias: Optional[str] = None) -> ClauseElement:
        raise NotImplementedError

    def get_table_definition(self) -> TableDefinition:
        return SATextTableDefinition(self.get_sql_source())  # type: ignore  # 2024-01-24 # TODO: Argument 1 to "SATextTableDefinition" has incompatible type "ClauseElement"; expected "TextClause"  [arg-type]


class BaseCHYTTableFuncDataSource(BaseCHYTSpecialDataSource, abc.ABC):
    """A common base class for CHYT Table-Function-based data source (concat / concat range)."""

    def get_schema_info(self, conn_executor_factory: Callable[[], SyncConnExecutorBase]) -> SchemaInfo:
        assert self.conn_type is not None
        schema_info = super().get_schema_info(conn_executor_factory=conn_executor_factory)
        return schema_info.clone(
            schema=tuple(
                [
                    *schema_info.schema,
                    *[
                        SchemaColumn(
                            name=key,
                            title=key,
                            user_type=UserDataType.string,
                            native_type=ClickHouseNativeType.normalize_name_and_create(name="string"),
                            nullable=False,
                        )
                        for key in ("$table_path", "$table_name", "$table_index")
                    ],
                ]
            )
        )


@attr.s
class BaseCHYTTableListDataSource(BaseCHYTTableFuncDataSource, abc.ABC):
    table_names: str = attr.ib(kw_only=True, default=None)  # stored as single `\n` separated string

    @property
    def spec(self) -> CHYTTableListDataSourceSpec:
        assert isinstance(self._spec, CHYTTableListDataSourceSpec)
        return self._spec

    @classmethod
    def normalize_tables_paths(cls, value: str) -> list[str]:
        table_paths = value.splitlines()
        table_paths = [cls.normalize_path(path) for path in table_paths if path.lstrip()]
        if not table_paths:
            raise exc.TableNameInvalidError("Table list is empty")
        if len(table_paths) != len(set(table_paths)):
            duplicates = [item for item, cnt in collections.Counter(table_paths).items() if cnt > 1]
            assert duplicates, ("should match the pre-check", table_paths)
            duplicates_s = "\n".join(duplicates)
            raise exc.TableNameInvalidError(f"Table list contains duplicates:\n{duplicates_s}")
        return table_paths

    def get_parameters(self) -> dict:
        return dict(
            super().get_parameters(),
            table_names=self.spec.table_names,
        )

    def get_sql_source(self, alias: Optional[str] = None) -> ClauseElement:
        if not self.spec.table_names:
            raise exc.TableNameNotConfiguredError
        table_names = self.normalize_tables_paths(self.spec.table_names)
        return CHYTTablesConcat(*table_names, alias=alias)

    @property
    def default_title(self) -> str:
        assert self.spec.table_names is not None
        return self.normalize_tables_paths(self.spec.table_names)[0].split("/")[-1]


def _value_or_none(value: Optional[str]) -> Optional[str]:
    return value or None


@attr.s
class BaseCHYTTableRangeDataSource(BaseCHYTTableFuncDataSource, abc.ABC):
    _directory_path: Optional[str] = attr.ib(default=None)
    _range_from: Optional[str] = attr.ib(default=None, converter=_value_or_none)
    _range_to: Optional[str] = attr.ib(default=None, converter=_value_or_none)

    @property
    def spec(self) -> CHYTTableRangeDataSourceSpec:
        assert isinstance(self._spec, CHYTTableRangeDataSourceSpec)
        return self._spec

    @property
    def directory_path(self) -> Optional[str]:
        return self.spec.directory_path

    @property
    def range_from(self) -> Optional[str]:
        return self.spec.range_from

    @property
    def range_to(self) -> Optional[str]:
        return self.spec.range_to

    def get_parameters(self) -> dict:
        return dict(
            super().get_parameters(),
            directory_path=self.directory_path,
            range_from=self.range_from or "",
            range_to=self.range_to or "",
        )

    def get_sql_source(self, alias: Optional[str] = None) -> ClauseElement:
        if not self.directory_path:
            raise exc.TableNameNotConfiguredError
        path = self.directory_path
        path = self.normalize_path(path)
        return CHYTTablesRange(directory=path, start=self.range_from, end=self.range_to, alias=alias)

    @property
    def default_title(self) -> str:
        assert self.directory_path is not None
        return self.normalize_path(self.directory_path).split("/")[-1]


# Expected MRO:
# CHYT-side: BaseCHYTTableSubselectDataSource, BaseCHYTTableFuncDataSource, CHYTBaseMixin, ClickHouseBaseMixin,
# Subselect-side: CommonClickHouseSubselectDataSource, SubselectDataSource,
# Base-side: BaseSQLDataSource, DataSource
# The Subselect-side isn't particularly necessary, but it marks the class as relevant.
class BaseCHYTTableSubselectDataSource(BaseCHYTSpecialDataSource, CommonClickHouseSubselectDataSource, abc.ABC):
    def get_parameters(self) -> dict:
        return dict(
            super().get_parameters(),
            subsql=self.subsql,
        )

    def get_sql_source(self, alias: Optional[str] = None) -> ClauseElement:
        if not self.connection.is_subselect_allowed:  # type: ignore  # 2024-01-24 # TODO: "ConnectionBase" has no attribute "is_subselect_allowed"  [attr-defined]
            raise exc.SubselectNotAllowed()

        subsql = self.subsql
        if not subsql:
            raise exc.TableNameNotConfiguredError
        return CHYTTableSubselect(subsql, alias=alias)

    @property
    def default_title(self) -> str:
        return "(select ...)"


class CHYTTokenAuthDataSourceMixin:
    conn_type = CONNECTION_TYPE_CHYT

    @classmethod
    def is_compatible_with_type(cls, source_type: DataSourceType) -> bool:
        return source_type in {
            SOURCE_TYPE_CHYT_YTSAURUS_TABLE,
            SOURCE_TYPE_CHYT_YTSAURUS_SUBSELECT,
            SOURCE_TYPE_CHYT_YTSAURUS_TABLE_LIST,
            SOURCE_TYPE_CHYT_YTSAURUS_TABLE_RANGE,
        }


class CHYTTableDataSource(CHYTTokenAuthDataSourceMixin, BaseCHYTTableDataSource):
    pass


class CHYTTableListDataSource(CHYTTokenAuthDataSourceMixin, BaseCHYTTableListDataSource):
    pass


class CHYTTableRangeDataSource(CHYTTokenAuthDataSourceMixin, BaseCHYTTableRangeDataSource):
    pass


class CHYTTableSubselectDataSource(CHYTTokenAuthDataSourceMixin, BaseCHYTTableSubselectDataSource):
    pass
