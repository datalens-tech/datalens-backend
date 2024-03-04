from __future__ import annotations

import logging
import time
from typing import (
    TYPE_CHECKING,
    Awaitable,
    Callable,
    Optional,
    Sequence,
    Union,
)

import attr

from dl_api_commons.reporting.models import (
    QueryExecutionCacheInfoReportingRecord,
    QueryExecutionEndReportingRecord,
    QueryExecutionStartReportingRecord,
)
from dl_constants.enums import (
    ReportingQueryType,
    UserDataType,
)
from dl_core import utils
from dl_core.base_models import WorkbookEntryLocation
from dl_core.connection_executors import ConnExecutorQuery
from dl_core.data_processing.prepared_components.default_manager import DefaultPreparedComponentManager
from dl_core.data_processing.processing.context import OpExecutionContext
from dl_core.data_processing.processing.db_base.exec_adapter_base import ProcessorDbExecAdapterBase
import dl_core.exc as exc
from dl_core.query.bi_query import QueryAndResultInfo
from dl_core.us_connection_base import (
    ClassicConnectionSQL,
    ConnectionBase,
)
from dl_utils.streaming import (
    AsyncChunked,
    AsyncChunkedBase,
    LazyAsyncChunked,
)


if TYPE_CHECKING:
    from sqlalchemy.sql.selectable import Select

    from dl_api_commons.base_models import RequestContextInfo
    from dl_cache_engine.primitives import LocalKeyRepresentation
    from dl_constants.enums import DataSourceRole
    from dl_constants.types import TBIDataValue
    from dl_core.base_models import ConnectionRef
    from dl_core.connections_security.base import ConnectionSecurityManager
    from dl_core.data_processing.prepared_components.manager_base import PreparedComponentManagerBase
    from dl_core.data_processing.prepared_components.primitives import PreparedFromInfo
    from dl_core.data_processing.types import TValuesChunkStream
    from dl_core.services_registry.conn_executor_factory_base import ConnExecutorFactory
    from dl_core.us_dataset import Dataset
    from dl_core.us_manager.local_cache import USEntryBuffer


LOGGER = logging.getLogger(__name__)


def get_query_type(connection: ConnectionBase, conn_sec_mgr: ConnectionSecurityManager) -> ReportingQueryType:
    if connection.is_always_internal_source:
        return ReportingQueryType.internal

    if isinstance(connection, ClassicConnectionSQL):
        if conn_sec_mgr.is_internal_connection(connection.get_conn_dto()):
            return ReportingQueryType.internal

    return ReportingQueryType.external


@attr.s
class SourceDbExecAdapter(ProcessorDbExecAdapterBase):  # noqa
    _role: DataSourceRole = attr.ib(kw_only=True)
    _dataset: Dataset = attr.ib(kw_only=True)
    _prep_component_manager: Optional[PreparedComponentManagerBase] = attr.ib(kw_only=True, default=None)
    _row_count_hard_limit: Optional[int] = attr.ib(kw_only=True, default=None)
    _us_entry_buffer: USEntryBuffer = attr.ib(kw_only=True)
    _ce_factory: ConnExecutorFactory = attr.ib(kw_only=True)
    _rci: RequestContextInfo = attr.ib(kw_only=True)

    def __attrs_post_init__(self) -> None:
        if self._prep_component_manager is None:
            self._prep_component_manager = DefaultPreparedComponentManager(
                dataset=self._dataset,
                role=self._role,
                us_entry_buffer=self._us_entry_buffer,
            )

    def get_prep_component_manager(self) -> PreparedComponentManagerBase:
        assert self._prep_component_manager is not None
        return self._prep_component_manager

    async def _get_data_stream_from_source(
        self,
        *,
        query_res_info: QueryAndResultInfo,
        joint_dsrc_info: PreparedFromInfo,
        row_count_hard_limit: Optional[int] = None,
    ) -> TValuesChunkStream:
        """Generate data stream from a data source"""

        compiled_query = utils.compile_query_for_debug(query_res_info.query, joint_dsrc_info.query_compiler.dialect)
        LOGGER.info(f"SQL query for dataset: {compiled_query}")

        assert joint_dsrc_info.target_connection_ref is not None
        target_connection = self._us_entry_buffer.get_entry(joint_dsrc_info.target_connection_ref)
        assert isinstance(target_connection, ConnectionBase)

        ce = self._ce_factory.get_async_conn_executor(target_connection)

        exec_result = await ce.execute(
            ConnExecutorQuery(
                query=query_res_info.query,
                db_name=joint_dsrc_info.db_name,
                user_types=query_res_info.user_types,
                debug_compiled_query=compiled_query,
                chunk_size=None,
            )
        )
        wrapped_result_iter = AsyncChunked(chunked_data=exec_result.result)

        async def initialize_data_stream() -> AsyncChunkedBase[list[TBIDataValue]]:
            return wrapped_result_iter  # type: ignore  # TODO: fix

        async def finalize_data_stream() -> None:
            pass

        result_iter: AsyncChunkedBase = LazyAsyncChunked(
            initializer=initialize_data_stream,
            finalizer=finalize_data_stream,
        )

        if row_count_hard_limit is not None:
            result_iter = result_iter.limit(
                max_count=row_count_hard_limit,
                limit_exception=exc.ResultRowCountLimitExceeded,
            )
        return result_iter  # type: ignore  # 2024-01-24 # TODO: Incompatible return value type (got "LazyAsyncChunked[list[date | datetime | time | timedelta | Decimal | UUID | bytes | str | float | int | bool | None]]", expected "AsyncChunkedBase[Sequence[date | datetime | time | timedelta | Decimal | UUID | bytes | str | float | int | bool | None]]")  [return-value]

    async def _execute_and_fetch(
        self,
        *,
        query: Union[str, Select],
        user_types: Sequence[UserDataType],
        chunk_size: int,
        joint_dsrc_info: Optional[PreparedFromInfo] = None,
        query_id: str,
        ctx: OpExecutionContext,
        data_key: LocalKeyRepresentation,
        preparation_callback: Optional[Callable[[], Awaitable[None]]],
    ) -> TValuesChunkStream:
        assert not isinstance(query, str), "String queries are not supported by source DB processor"
        assert joint_dsrc_info is not None, "joint_dsrc_info is required for source DB processor"

        if preparation_callback is not None:
            await preparation_callback()

        query_res_info = self._make_query_res_info(query=query, user_types=user_types)
        data_stream_data = await self._get_data_stream_from_source(
            joint_dsrc_info=joint_dsrc_info,
            query_res_info=query_res_info,
            row_count_hard_limit=self._row_count_hard_limit,
        )
        return data_stream_data

    def _save_start_exec_reporting_record(
        self,
        query_id: str,
        compiled_query: str,
        target_connection_ref: Optional[ConnectionRef],
    ) -> None:
        assert target_connection_ref is not None
        target_connection = self._us_entry_buffer.get_entry(entry_id=target_connection_ref)
        assert isinstance(target_connection, ConnectionBase)

        workbook_id = self._rci.workbook_id or (
            target_connection.entry_key.workbook_id
            if isinstance(target_connection.entry_key, WorkbookEntryLocation)
            else None
        )
        dataset_id = self._dataset.uuid
        record = QueryExecutionStartReportingRecord(
            timestamp=time.time(),
            query_id=query_id,
            dataset_id=dataset_id,
            query_type=get_query_type(
                connection=target_connection,
                conn_sec_mgr=self._ce_factory.conn_security_manager,
            ),
            connection_type=target_connection.conn_type,
            conn_reporting_data=target_connection.get_conn_dto().conn_reporting_data(),
            query=compiled_query,
            workbook_id=workbook_id,
        )
        self.add_reporting_record(record)

    def _save_end_exec_reporting_record(self, query_id: str, exec_exception: Optional[Exception]) -> None:
        record = QueryExecutionEndReportingRecord(
            timestamp=time.time(),
            query_id=query_id,
            exception=exec_exception,
        )
        self.add_reporting_record(record)

    def _save_query_exec_cache_info_reporting_record(self, query_id: str, cache_full_hit: bool) -> None:
        query_exec_cache_record = QueryExecutionCacheInfoReportingRecord(
            query_id=query_id,
            cache_full_hit=cache_full_hit,
            timestamp=time.time(),
        )
        self.add_reporting_record(query_exec_cache_record)

    def pre_query_execute(
        self,
        query_id: str,
        compiled_query: str,
        target_connection_ref: Optional[ConnectionRef],
    ) -> None:
        self._save_start_exec_reporting_record(
            query_id=query_id,
            compiled_query=compiled_query,
            target_connection_ref=target_connection_ref,
        )

    def post_query_execute(self, query_id: str, exec_exception: Optional[Exception]) -> None:
        self._save_end_exec_reporting_record(query_id=query_id, exec_exception=exec_exception)

    def post_cache_usage(self, query_id: str, cache_full_hit: bool) -> None:
        self._save_query_exec_cache_info_reporting_record(query_id=query_id, cache_full_hit=cache_full_hit)
