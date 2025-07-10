from __future__ import annotations

import abc
import logging
from typing import (
    TYPE_CHECKING,
    Awaitable,
    Callable,
    ClassVar,
    Collection,
    Optional,
    Sequence,
    Union,
)

import attr
import sqlalchemy as sa
from sqlalchemy.sql.selectable import Select

from dl_cache_engine.primitives import LocalKeyRepresentation
from dl_constants.enums import JoinType
from dl_constants.types import TBIDataValue
from dl_core.connectors.base.query_compiler import QueryCompiler
from dl_core.data_processing.cache.utils import DatasetOptionsBuilder
from dl_core.data_processing.processing.context import OpExecutionContext
from dl_core.data_processing.types import TValuesChunkStream
from dl_core.query.bi_query import QueryAndResultInfo
from dl_core.utils import make_id
from dl_utils.streaming import AsyncChunkedBase


if TYPE_CHECKING:
    from dl_api_commons.reporting.records import ReportingRecord
    from dl_api_commons.reporting.registry import ReportingRegistry
    from dl_constants.enums import UserDataType
    from dl_core.base_models import ConnectionRef
    from dl_core.data_processing.prepared_components.primitives import PreparedFromInfo


LOGGER = logging.getLogger(__name__)
DEFAULT_CHUNK_SIZE = 1000


@attr.s
class ProcessorDbExecAdapterBase(abc.ABC):
    _default_chunk_size: ClassVar[int] = 1000
    _log: ClassVar[logging.Logger] = LOGGER.getChild("ProcessorDbExecAdapterBase")
    _cache_options_builder: DatasetOptionsBuilder = attr.ib(kw_only=True)
    _reporting_enabled: bool = attr.ib(kw_only=True, default=True)
    _reporting_registry: ReportingRegistry = attr.ib(kw_only=True)

    def add_reporting_record(self, record: ReportingRecord) -> None:
        if self._reporting_enabled:
            self._reporting_registry.save_reporting_record(record)

    @abc.abstractmethod
    async def _execute_and_fetch(
        self,
        *,
        query: Select | str,
        user_types: Sequence[UserDataType],
        chunk_size: int,
        joint_dsrc_info: Optional[PreparedFromInfo] = None,
        query_id: str,
        ctx: OpExecutionContext,
        data_key: LocalKeyRepresentation,
        preparation_callback: Optional[Callable[[], Awaitable[None]]],
    ) -> TValuesChunkStream:
        """
        Execute SELECT statement.
        Return data as ``AsyncChunked``.
        """
        raise NotImplementedError

    async def scalar(
        self,
        query: str | Select,
        user_type: UserDataType,
        ctx: OpExecutionContext,
        data_key: LocalKeyRepresentation = LocalKeyRepresentation(),  # noqa: B008
        preparation_callback: Optional[Callable[[], Awaitable[None]]] = None,
    ) -> TBIDataValue:
        """Execute a statement returning a scalar value."""
        data_stream = await self._execute_and_fetch(
            query_id=make_id(),
            query=query,
            user_types=[user_type],
            chunk_size=5,
            ctx=ctx,
            data_key=data_key,
            preparation_callback=preparation_callback,
        )
        data = await data_stream.all()
        assert len(data) == 1, f"Expected 1 entry, got {len(data)}"
        assert len(data[0]) == 1, f"Expected 1 column, got {len(data[0])}"
        return data[0][0]

    async def fetch_data_from_select(
        self,
        *,
        query: Union[str, sa.sql.selectable.Select],
        user_types: Sequence[UserDataType],
        chunk_size: Optional[int] = None,
        joint_dsrc_info: Optional[PreparedFromInfo] = None,
        query_id: str,
        ctx: OpExecutionContext,
        data_key: LocalKeyRepresentation = LocalKeyRepresentation(),  # noqa: B008
        preparation_callback: Optional[Callable[[], Awaitable[None]]] = None,
    ) -> TValuesChunkStream:
        """Fetch data from a table"""

        chunk_size = chunk_size or self._default_chunk_size
        self._log.info(f"Fetching data from query {query_id}")

        return await self._execute_and_fetch(
            query=query,
            user_types=user_types,
            chunk_size=chunk_size,
            joint_dsrc_info=joint_dsrc_info,
            query_id=query_id,
            ctx=ctx,
            data_key=data_key,
            preparation_callback=preparation_callback,
        )

    def _make_query_res_info(
        self,
        query: Union[str, Select],
        user_types: Sequence[UserDataType],
    ) -> QueryAndResultInfo:
        query_res_info = QueryAndResultInfo(
            query=query,  # type: ignore  # TODO: fix
            user_types=list(user_types),
            # This is basically legacy and will be removed.
            # col_names are not really used anywhere, just passed around a lot.
            # So we generate random ones here
            col_names=[f"col_{i}" for i in range(len(user_types))],
        )
        return query_res_info

    async def create_table(
        self,
        *,
        table_name: str,
        names: Sequence[str],
        user_types: Sequence[UserDataType],
    ) -> None:
        """Create table"""
        raise NotImplementedError  # By default DDL is not supported

    async def insert_data_into_table(
        self,
        *,
        table_name: str,
        names: Sequence[str],
        user_types: Sequence[UserDataType],
        data: AsyncChunkedBase,
    ) -> None:
        """,,,"""
        raise NotImplementedError  # By default DDL is not supported

    def get_supported_join_types(self) -> Collection[JoinType]:
        return frozenset(
            {
                JoinType.inner,
                JoinType.left,
                JoinType.right,
                JoinType.full,
            }
        )

    def get_query_compiler(self) -> QueryCompiler:
        raise NotImplementedError  # By default no specifc QueryCompiler is defined

    def pre_query_execute(
        self,
        query_id: str,
        compiled_query: str,
        target_connection_ref: Optional[ConnectionRef],
    ) -> None:
        return

    def post_query_execute(self, query_id: str, exec_exception: Optional[Exception]) -> None:
        return

    def post_cache_usage(self, query_id: str, cache_full_hit: bool | None) -> None:
        return
