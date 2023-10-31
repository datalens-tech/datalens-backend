from __future__ import annotations

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
from dl_constants.enums import UserDataType
from dl_core.base_models import WorkbookEntryLocation
from dl_core.data_processing.prepared_components.default_manager import DefaultPreparedComponentManager
from dl_core.data_processing.processing.context import OpExecutionContext
from dl_core.data_processing.processing.db_base.exec_adapter_base import ProcessorDbExecAdapterBase
from dl_core.data_processing.selectors.utils import get_query_type
from dl_core.us_connection_base import ExecutorBasedMixin


if TYPE_CHECKING:
    from sqlalchemy.sql.selectable import Select

    from dl_constants.enums import DataSourceRole
    from dl_core.base_models import ConnectionRef
    from dl_core.data_processing.cache.primitives import LocalKeyRepresentation
    from dl_core.data_processing.prepared_components.manager_base import PreparedComponentManagerBase
    from dl_core.data_processing.prepared_components.primitives import PreparedFromInfo
    from dl_core.data_processing.selectors.base import DataSelectorAsyncBase
    from dl_core.data_processing.types import TValuesChunkStream
    from dl_core.us_dataset import Dataset
    from dl_core.us_manager.local_cache import USEntryBuffer


@attr.s
class SourceDbExecAdapter(ProcessorDbExecAdapterBase):  # noqa
    _role: DataSourceRole = attr.ib(kw_only=True)
    _dataset: Dataset = attr.ib(kw_only=True)
    _selector: DataSelectorAsyncBase = attr.ib(kw_only=True)
    _prep_component_manager: Optional[PreparedComponentManagerBase] = attr.ib(kw_only=True, default=None)
    _row_count_hard_limit: Optional[int] = attr.ib(kw_only=True, default=None)
    _us_entry_buffer: USEntryBuffer = attr.ib(kw_only=True)

    def __attrs_post_init__(self):  # type: ignore  # TODO: fix
        if self._prep_component_manager is None:
            self._prep_component_manager = DefaultPreparedComponentManager(
                dataset=self._dataset,
                role=self._role,
                us_entry_buffer=self._us_entry_buffer,
            )

    def get_prep_component_manager(self) -> PreparedComponentManagerBase:
        assert self._prep_component_manager is not None
        return self._prep_component_manager

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
        data_stream = await self._selector.get_data_stream(
            query_id=query_id,
            role=self._role,
            joint_dsrc_info=joint_dsrc_info,
            query_res_info=query_res_info,
            row_count_hard_limit=self._row_count_hard_limit,
        )
        return data_stream.data

    def _save_start_exec_reporting_record(
        self,
        query_id: str,
        compiled_query: str,
        target_connection_ref: Optional[ConnectionRef],
    ) -> None:
        assert target_connection_ref is not None
        target_connection = self._us_entry_buffer.get_entry(entry_id=target_connection_ref)
        assert isinstance(target_connection, ExecutorBasedMixin)

        workbook_id = (
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
                conn_sec_mgr=self._service_registry.get_conn_executor_factory().conn_security_manager,
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
