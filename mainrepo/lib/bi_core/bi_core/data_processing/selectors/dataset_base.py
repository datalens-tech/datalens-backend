from __future__ import annotations

import abc
import logging
import time
from contextlib import contextmanager
from typing import TYPE_CHECKING, Generator, Optional

import attr

from bi_constants.enums import DataSourceRole

from bi_api_commons.reporting.models import (
    QueryExecutionEndReportingRecord, QueryExecutionStartReportingRecord,
)

import bi_core.exc as exc
from bi_core import utils
from bi_core.data_processing.selectors.base import (
    BIQueryExecutionContext, DataSelectorAsyncBase, NoData,
)
from bi_core.data_processing.selectors.utils import get_query_type
from bi_core.data_processing.stream_base import DataRequestMetaInfo, DataStreamAsync
from bi_core.query.bi_query import QueryAndResultInfo
from bi_core.us_connection_base import ExecutorBasedMixin
from bi_core.utils import make_id

if TYPE_CHECKING:
    from bi_core.data_processing.prepared_components.primitives import PreparedMultiFromInfo
    from bi_core.data_processing.cache.primitives import LocalKeyRepresentation
    from bi_core.data_processing.types import TValuesChunkStream
    from bi_api_commons.reporting.registry import ReportingRegistry
    from bi_core.services_registry import ServicesRegistry
    from bi_core.us_dataset import Dataset
    from bi_core.us_manager.local_cache import USEntryBuffer


LOGGER = logging.getLogger(__name__)


# noinspection PyDataclass
@attr.s
class DatasetDataSelectorAsyncBase(DataSelectorAsyncBase, metaclass=abc.ABCMeta):
    dataset: Dataset = attr.ib(kw_only=True)
    _us_entry_buffer: USEntryBuffer = attr.ib(kw_only=True)
    _service_registry: ServicesRegistry = attr.ib(kw_only=True)  # Service registry override

    """
    Base class for dataset-dependent asynchronous data selectors
    :param dataset: the dataset to operate on
    """

    def _save_start_exec_reporting_record(
            self,
            query_execution_ctx: BIQueryExecutionContext,
    ) -> None:
        connection = query_execution_ctx.target_connection
        report = QueryExecutionStartReportingRecord(
            timestamp=time.time(),
            query_id=query_execution_ctx.query_id,
            dataset_id=self.dataset.uuid,  # type: ignore  # TODO: fix
            query_type=get_query_type(
                connection=connection,
                conn_sec_mgr=self.service_registry.get_conn_executor_factory().conn_security_manager,
            ),
            connection_type=connection.conn_type,
            conn_reporting_data=connection.get_conn_dto().conn_reporting_data(),
            query=query_execution_ctx.compiled_query,
        )
        self.reporting_registry.save_reporting_record(report=report)

    def _save_end_exec_reporting_record(
            self,
            query_execution_ctx: BIQueryExecutionContext,
            exec_exception: Optional[Exception],
    ) -> None:
        report = QueryExecutionEndReportingRecord(
            timestamp=time.time(),
            query_id=query_execution_ctx.query_id,
            exception=exec_exception,
        )
        self.reporting_registry.save_reporting_record(report=report)

    def pre_exec(self, query_execution_ctx: BIQueryExecutionContext) -> None:
        self._save_start_exec_reporting_record(query_execution_ctx)

    def post_exec(
            self,
            query_execution_ctx: BIQueryExecutionContext,
            exec_exception: Optional[Exception],
    ) -> None:
        self._save_end_exec_reporting_record(query_execution_ctx, exec_exception)

    @contextmanager
    def _execution_cm(
            self,
            query_execution_ctx: BIQueryExecutionContext,
    ) -> Generator[None, None, None]:
        self.pre_exec(query_execution_ctx=query_execution_ctx)
        exec_exception: Optional[Exception] = None
        try:
            yield
        except Exception as fired_exception:
            exec_exception = fired_exception
            raise
        finally:
            self.post_exec(query_execution_ctx=query_execution_ctx, exec_exception=exec_exception)

    def get_data_key(
            self,
            query_execution_ctx: BIQueryExecutionContext,
    ) -> Optional[LocalKeyRepresentation]:
        return None

    async def get_data_stream(
            self, *,
            query_id: Optional[str] = None,
            role: DataSourceRole,
            query_res_info: QueryAndResultInfo,
            joint_dsrc_info: PreparedMultiFromInfo,
            row_count_hard_limit: Optional[int] = None,
            stream_id: Optional[str] = None,
    ) -> DataStreamAsync:
        """Generate data stream from a data source"""

        query_id = query_id or make_id()
        query_execution_ctx = self.build_query_execution_ctx(
            query_id=query_id,
            query_res_info=query_res_info,
            role=role,
            joint_dsrc_info=joint_dsrc_info,
        )

        if not isinstance(query_execution_ctx.target_connection, ExecutorBasedMixin):
            raise exc.NotAvailableError(
                f"Connection {type(query_execution_ctx.target_connection).__qualname__}"
                f" does not support async data selection"
            )

        with self._execution_cm(query_execution_ctx):
            result_iter = await self.execute_query_context(
                role=role,
                query_execution_ctx=query_execution_ctx,
                row_count_hard_limit=row_count_hard_limit,
            )
            if result_iter is None:
                raise NoData('Got no data from selector')

            if row_count_hard_limit is not None:
                result_iter = result_iter.limit(max_count=row_count_hard_limit)

            data_key = self.get_data_key(query_execution_ctx=query_execution_ctx)
            stream_id = stream_id or make_id()
            LOGGER.info('Making data stream %s,', stream_id, extra=dict(data_key=str(data_key)))
            assert stream_id is not None

            data_source_list = joint_dsrc_info.data_source_list
            assert data_source_list is not None

            return DataStreamAsync(
                id=stream_id,
                user_types=query_execution_ctx.requested_bi_types,
                names=query_execution_ctx.result_col_names,
                data=result_iter,
                meta=DataRequestMetaInfo(
                    query_id=query_id,
                    query=query_execution_ctx.compiled_query,
                    is_materialized=role != DataSourceRole.origin,
                    data_source_list=data_source_list,
                ),
                data_key=data_key
            )

    @property
    def service_registry(self) -> ServicesRegistry:
        return self._service_registry

    @property
    def reporting_registry(self) -> ReportingRegistry:
        return self.service_registry.get_reporting_registry()

    def build_query_execution_ctx(
            self, *,
            query_id: str,
            query_res_info: QueryAndResultInfo,
            role: DataSourceRole,
            joint_dsrc_info: PreparedMultiFromInfo,
    ) -> BIQueryExecutionContext:

        compiled_query = utils.compile_query_for_debug(query_res_info.query, joint_dsrc_info.query_compiler.dialect)
        LOGGER.info(f'SQL query for dataset: {compiled_query}')

        assert joint_dsrc_info.target_connection_ref is not None
        target_connection = self._us_entry_buffer.get_entry(joint_dsrc_info.target_connection_ref)
        assert isinstance(target_connection, ExecutorBasedMixin)

        return BIQueryExecutionContext(
            query_id=query_id,
            query=query_res_info.query,
            compiled_query=compiled_query,
            target_connection=target_connection,
            target_db_name=joint_dsrc_info.db_name,
            requested_bi_types=query_res_info.user_types,
            result_col_names=query_res_info.col_names,
            cache_options=None,
            connect_args=joint_dsrc_info.connect_args,
        )

    @abc.abstractmethod
    async def execute_query_context(
            self,
            role: DataSourceRole,
            query_execution_ctx: BIQueryExecutionContext,
            row_count_hard_limit: Optional[int] = None,
    ) -> Optional[TValuesChunkStream]:

        """Get data using info in ``query_execution_ctx``"""
        raise NotImplementedError
