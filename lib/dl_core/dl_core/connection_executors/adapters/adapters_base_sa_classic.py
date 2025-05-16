from __future__ import annotations

import abc
import logging
from typing import (
    TYPE_CHECKING,
    Any,
    ClassVar,
    Generic,
    Optional,
    Type,
    TypeVar,
)
from urllib.parse import (
    quote_plus,
    urlencode,
)

import attr
import sqlalchemy as sa
from sqlalchemy.engine.base import Engine

from dl_core.connection_executors.adapters.adapters_base_sa import BaseSAAdapter
from dl_core.connection_executors.adapters.common_base import get_dialect_string
from dl_core.connection_executors.adapters.mixins import WithMinimalCursorInfo
from dl_core.connection_executors.models.connection_target_dto_base import (
    BaseSQLConnTargetDTO,
    ConnTargetDTO,
)
from dl_core.connection_executors.models.db_adapter_data import (
    DBAdapterQuery,
    ExecutionStepCursorInfo,
    RawColumnInfo,
    RawSchemaInfo,
)
from dl_type_transformer.native_type import CommonNativeType


if TYPE_CHECKING:
    from dl_core.connection_models import SATextTableDefinition

LOGGER = logging.getLogger(__name__)


_CONN_DTO_TV = TypeVar("_CONN_DTO_TV", bound=ConnTargetDTO)


@attr.s(cmp=False)
class BaseConnLineConstructor(abc.ABC, Generic[_CONN_DTO_TV]):
    _target_dto: _CONN_DTO_TV = attr.ib()
    _dsn_template: str = attr.ib()
    _dialect_name: str = attr.ib()

    @abc.abstractmethod
    def _get_dsn_params(
        self,
        safe_db_symbols: tuple[str, ...] = (),
        db_name: Optional[str] = None,
        standard_auth: Optional[bool] = True,
    ) -> dict:
        raise NotImplementedError

    def _get_dsn_query_params(self) -> dict:
        return {}

    def make_conn_line(self, db_name: Optional[str] = None, params: Optional[dict[str, Any]] = None) -> str:
        # TODO?: replace with `get_conn_line`, delete `dsn_template`.
        conn_line = self._dsn_template.format(**self._get_dsn_params(db_name=db_name))

        params = (params or {}).copy()
        params.update(self._get_dsn_query_params())

        if params:
            params = {key: val for key, val in params.items() if val is not None}
            conn_line += "?" + urlencode(params)

        return conn_line


_DBA_CLASSIC_SA_DTO_TV = TypeVar("_DBA_CLASSIC_SA_DTO_TV", bound="BaseSQLConnTargetDTO")


@attr.s(cmp=False)
class ClassicSQLConnLineConstructor(
    BaseConnLineConstructor[_DBA_CLASSIC_SA_DTO_TV],
    Generic[_DBA_CLASSIC_SA_DTO_TV],
):
    def _get_dsn_params(
        self,
        safe_db_symbols: tuple[str, ...] = (),
        db_name: Optional[str] = None,
        standard_auth: Optional[bool] = True,
    ) -> dict:
        return dict(
            dialect=self._dialect_name,
            user=quote_plus(self._target_dto.username) if standard_auth else None,
            passwd=quote_plus(self._target_dto.password) if standard_auth else None,
            host=quote_plus(self._target_dto.host),
            port=quote_plus(str(self._target_dto.port)),
            db_name=db_name or quote_plus(self._target_dto.db_name or "", safe="".join(safe_db_symbols)),
        )


@attr.s(cmp=False)
class BaseClassicAdapter(WithMinimalCursorInfo, BaseSAAdapter[_CONN_DTO_TV]):
    dsn_template: ClassVar[str] = "{dialect}://{user}:{passwd}@{host}:{port}/{db_name}"
    execution_options: ClassVar[dict[str, Any]] = {}
    conn_line_constructor_type: ClassVar[Type[BaseConnLineConstructor]] = ClassicSQLConnLineConstructor

    # Instance attributes
    _target_dto: _CONN_DTO_TV = attr.ib()

    def get_connect_args(self) -> dict:
        return {}

    def get_engine_kwargs(self) -> dict:
        return {}

    def get_default_db_name(self) -> Optional[str]:
        if isinstance(self._target_dto, BaseSQLConnTargetDTO):
            return self._target_dto.db_name
        raise NotImplementedError

    def _get_dsn_query_params(self) -> dict:
        return {}

    def get_conn_line(self, db_name: Optional[str] = None, params: Optional[dict[str, Any]] = None) -> str:
        return self.conn_line_constructor_type(
            dsn_template=self.dsn_template,
            dialect_name=get_dialect_string(self.conn_type),
            target_dto=self._target_dto,
        ).make_conn_line(db_name=db_name, params=params)

    def _get_db_engine(self, db_name: str, disable_streaming: bool = False) -> Engine:
        conn_line = self.get_conn_line(db_name=db_name)

        execution_options = self.execution_options
        if disable_streaming:
            execution_options = execution_options.copy()
            execution_options.pop("stream_results", None)

        print(f"@#~~!~~#@ connection line: {conn_line}")
        print(f"@#~~!~~#@ connection args: {self.get_connect_args()}")
        print(f"@#~~!~~#@ execution options: {execution_options}")
        print(f"@#~~!~~#@ engine kwargs: {self.get_engine_kwargs()}")

        engine = sa.create_engine(
            conn_line,
            connect_args=self.get_connect_args(),
            execution_options=execution_options,
            # Apparently, this is the only way to pass something to the dialect's `__init__`.
            **self.get_engine_kwargs(),
        )

        print(f"@#~~!~~#@ engine: {engine}")

        connection = engine.connect()
        print(f"@#~~!~~#@ conn: {connection}")

        res = connection.execute(sa.text("SELECT 1"))
        print(f"@#~~!~~#@ res: {res}")
        print(f"@#~~!~~#@ res: {res.fetchall()}")

        connection.close()


        engine = engine.execution_options(compiled_cache=None)
        return engine

    _subselect_cursor_info_where_false: ClassVar[bool] = True

    def _get_subselect_raw_cursor_info_and_data(
        self,
        subselect: sa.sql.elements.TextClause,
        limit: Optional[int] = 1,
        where_false: Optional[bool] = None,
    ) -> dict:
        """
        Run a `select * limit 1` query, return cursor info.

        Primarily meant for getting schema of subselects.

        NOTE: `subselect` must be already aliased (in most cases).
        """
        sa_query = sa.select([sa.literal_column("*")]).select_from(subselect)
        if where_false is None:
            where_false = self._subselect_cursor_info_where_false
        if where_false:
            sa_query = sa_query.where(False)
        if limit is not None:
            sa_query = sa_query.limit(limit)
        dba_query = DBAdapterQuery(query=sa_query)
        query_res = self.execute(dba_query)

        assert query_res.raw_cursor_info
        data = list(query_res.data_chunks)
        return query_res.raw_cursor_info, data  # type: ignore  # TODO: fix

    def _get_subselect_table_info(self, subquery: SATextTableDefinition) -> RawSchemaInfo:
        """Will not work without non-empty `self._type_code_to_sa`"""
        raw_cursor_info, _ = self._get_subselect_raw_cursor_info_and_data(subquery.text)
        return self._raw_cursor_info_to_schema(raw_cursor_info)

    def _raw_cursor_info_to_schema(self, raw_cursor_info: ExecutionStepCursorInfo) -> RawSchemaInfo:
        """
        See also: `_get_raw_columns_info`, `_get_sa_table_columns`,
        `create_schema_info_from_raw_schema_info`.
        """
        description = raw_cursor_info.raw_cursor_description
        dialect = None
        if raw_cursor_info.raw_engine is not None:
            dialect = raw_cursor_info.raw_engine.dialect

        columns = []
        for cursor_col in description:
            name = self._cursor_column_to_name(cursor_col, dialect=dialect)
            native_type = self._cursor_column_to_native_type(cursor_col, require=False)
            if native_type is None:
                native_type = CommonNativeType.normalize_name_and_create(
                    name=self.normalize_sa_col_type(sa.sql.sqltypes.NullType),  # type: ignore  # TODO: fix
                    nullable=True,
                )
            columns.append(
                RawColumnInfo(
                    name=name,
                    title=name,
                    nullable=native_type.nullable,
                    native_type=native_type,
                )
            )

        return RawSchemaInfo(columns=tuple(columns))
