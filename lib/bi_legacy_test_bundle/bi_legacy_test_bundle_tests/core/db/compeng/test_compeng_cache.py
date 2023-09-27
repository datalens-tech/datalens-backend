from __future__ import annotations

import logging
import time
from typing import List
import uuid

import pytest
import sqlalchemy as sa

from bi_legacy_test_bundle_tests.core.utils import get_dump_request_profile_records
from dl_api_commons.reporting.models import QueryExecutionStartReportingRecord
from dl_api_commons.reporting.profiler import (
    PROFILING_LOG_NAME,
    DefaultReportingProfiler,
)
from dl_connector_clickhouse.core.clickhouse_base.constants import CONNECTION_TYPE_CLICKHOUSE
from dl_connector_clickhouse.core.clickhouse_base.dto import ClickHouseConnDTO
from dl_constants.enums import (
    DataSourceRole,
    ProcessorType,
    QueryType,
    UserDataType,
)
from dl_core.base_models import WorkbookEntryLocation
from dl_core.data_processing.cache.primitives import (
    DataKeyPart,
    LocalKeyRepresentation,
)
from dl_core.data_processing.processing.operation import (
    BaseOp,
    CalcOp,
    DownloadOp,
    UploadOp,
)
from dl_core.data_processing.stream_base import (
    DataRequestMetaInfo,
    DataStreamAsync,
)
from dl_core.data_processing.streaming import AsyncChunked
from dl_core.query.bi_query import BIQuery
from dl_core.query.expression import ExpressionCtx
from dl_core.services_registry import ServicesRegistry
from dl_core_testing.dataset_wrappers import DatasetTestWrapper
from dl_core_testing.utils import SROptions


class TestCompengCache:
    @pytest.mark.asyncio
    async def test_compeng_cache(
        self,
        saved_ch_dataset,
        bi_context,
        async_service_registry_factory,
        caplog,
        default_sync_usm,
    ):
        dataset = saved_ch_dataset
        us_manager = default_sync_usm
        ds_wrapper = DatasetTestWrapper(dataset=dataset, us_manager=us_manager)

        names = ["int_value", "str_value"]
        user_types = [UserDataType.integer, UserDataType.string]

        def get_operations(coeff: int) -> List[BaseOp]:
            """
            Instructions for compeng.
            ``int_value`` is multiplied by ``coeff``
            """
            return [
                UploadOp(
                    result_id="1",
                    source_stream_id="1",
                    dest_stream_id="2",
                    alias=str(uuid.uuid4()),
                ),
                CalcOp(
                    result_id="2",
                    source_stream_id="2",
                    dest_stream_id="3",
                    alias=str(uuid.uuid4()),
                    bi_query=BIQuery(
                        select_expressions=[
                            ExpressionCtx(
                                expression=sa.literal_column(names[0]) * sa.literal(coeff),
                                alias="value_1",
                                user_type=UserDataType.integer,
                            ),
                            ExpressionCtx(
                                expression=sa.literal_column(names[1]),
                                alias="value_2",
                                user_type=UserDataType.string,
                            ),
                        ],
                    ),
                ),
                DownloadOp(
                    source_stream_id="3",
                    dest_stream_id="4",
                ),
            ]

        def get_expected_data(length: int, coeff: int):
            """
            What we expect to be returned from compeng.
            ``length`` entries are generated
            ``int_value`` is multiplied by ``coeff``
            """
            return [[i * coeff, f"str_{i}"] for i in range(length)]

        async def get_data_from_processor(input_data, data_key, operations):
            query_id = str(uuid.uuid4())
            sr: ServicesRegistry = async_service_registry_factory(
                SROptions(
                    rci=bi_context,
                    with_caches=True,
                    with_compeng_pg=True,
                )
            )
            dto = ClickHouseConnDTO(
                conn_id="123",
                host="localhost",
                cluster_name="cluster_name",
                db_name="",
                endpoint="",
                multihosts=[""],
                password="",
                port=0,
                protocol="",
                username="",
            )
            reporting = sr.get_reporting_registry()
            workbook_id = (
                dataset.entry_key.workbook_id if isinstance(dataset.entry_key, WorkbookEntryLocation) else None
            )
            reporting.save_reporting_record(
                QueryExecutionStartReportingRecord(
                    timestamp=time.time(),
                    query_id=query_id,
                    query_type=QueryType.internal,
                    query="1",  # doesn't matter...
                    connection_type=CONNECTION_TYPE_CLICKHOUSE,
                    dataset_id=dataset.uuid,
                    conn_reporting_data=dto.conn_reporting_data(),
                    workbook_id=workbook_id,
                )
            )
            input_stream = DataStreamAsync(
                id="1",
                data=AsyncChunked.from_chunked_iterable([input_data]),
                names=names,
                user_types=user_types,
                meta=DataRequestMetaInfo(
                    is_materialized=False,
                    data_source_list=ds_wrapper.get_data_source_list(role=DataSourceRole.origin),
                    query_id=query_id,
                ),
                data_key=data_key,
            )
            processor_factory = sr.get_data_processor_factory()
            processor = await processor_factory.get_data_processor(
                dataset=dataset,
                us_entry_buffer=us_manager.get_entry_buffer(),
                allow_cache_usage=True,
                processor_type=ProcessorType.ASYNCPG,
            )
            try:
                output_streams = await processor.run(
                    streams=[input_stream],
                    operations=operations,
                    output_stream_ids=[operations[-1].dest_stream_id],
                )
                assert len(output_streams) == 1
                output_stream = output_streams[0]
                assert output_stream.id == "4"
                return await output_stream.data.all()
            finally:
                await sr.close_async()
                reporting_profiler = DefaultReportingProfiler(reporting_registry=sr.get_reporting_registry())
                reporting_profiler.on_request_end()

        caplog.set_level(logging.INFO, logger=PROFILING_LOG_NAME)

        # Validate the first run
        caplog.clear()
        input_data_l10 = [[i, f"str_{i}"] for i in range(10)]
        operations_c10 = get_operations(coeff=10)
        data_key_l10 = LocalKeyRepresentation(key_parts=(DataKeyPart(part_type="part", part_content="value_l10"),))
        output_data = await get_data_from_processor(
            input_data=input_data_l10, data_key=data_key_l10, operations=operations_c10
        )
        expected_data_l10_c10 = get_expected_data(length=10, coeff=10)
        assert output_data == expected_data_l10_c10
        # Check cache flags in reporting
        req_profiling_log_rec = get_dump_request_profile_records(caplog, single=True)
        assert req_profiling_log_rec.cache_used is True
        assert req_profiling_log_rec.cache_full_hit is False

        # Now use updated input data, but with the old cache key and same operations
        # -> same old data from cache
        caplog.clear()
        input_data_l5 = [[i, f"str_{i}"] for i in range(5)]  # up to 5 instead of 10
        output_data = await get_data_from_processor(
            input_data=input_data_l5, data_key=data_key_l10, operations=operations_c10
        )
        assert output_data == expected_data_l10_c10
        # Check cache flags in reporting
        req_profiling_log_rec = get_dump_request_profile_records(caplog, single=True)
        assert req_profiling_log_rec.cache_used is True
        assert req_profiling_log_rec.cache_full_hit is True

        # Now use updated input data with updated operations -> should result in a new full key
        # -> fresh data, not from cache
        caplog.clear()
        data_key_l5 = LocalKeyRepresentation(key_parts=(DataKeyPart(part_type="part", part_content="value_l5"),))
        operations_c5 = get_operations(coeff=5)
        output_data = await get_data_from_processor(
            input_data=input_data_l5, data_key=data_key_l5, operations=operations_c5
        )
        expected_data_l5_c5 = get_expected_data(length=5, coeff=5)
        assert output_data == expected_data_l5_c5
        # Check cache flags in reporting
        req_profiling_log_rec = get_dump_request_profile_records(caplog, single=True)
        assert req_profiling_log_rec.cache_used is True
        assert req_profiling_log_rec.cache_full_hit is False
