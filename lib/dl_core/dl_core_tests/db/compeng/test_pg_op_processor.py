from __future__ import annotations

from datetime import (
    datetime,
    timedelta,
    timezone,
)
from typing import (
    ClassVar,
    Type,
)
import uuid

import pytest
import sqlalchemy as sa

from dl_compeng_pg.compeng_aiopg.pool_aiopg import AiopgPoolWrapper
from dl_compeng_pg.compeng_aiopg.processor_aiopg import AiopgOperationProcessor
from dl_compeng_pg.compeng_asyncpg.pool_asyncpg import AsyncpgPoolWrapper
from dl_compeng_pg.compeng_asyncpg.processor_asyncpg import AsyncpgOperationProcessor
from dl_compeng_pg.compeng_pg_base.pool_base import BasePgPoolWrapper
from dl_compeng_pg.compeng_pg_base.processor_base import PostgreSQLOperationProcessor
from dl_constants.enums import (
    JoinType,
    UserDataType,
)
from dl_core.data_processing.cache.primitives import LocalKeyRepresentation
from dl_core.data_processing.processing.operation import (
    CalcOp,
    DownloadOp,
    JoinOp,
    UploadOp,
)
from dl_core.data_processing.stream_base import (
    DataRequestMetaInfo,
    DataStreamAsync,
)
from dl_core.data_processing.streaming import AsyncChunked
from dl_core.query.bi_query import BIQuery
from dl_core.query.expression import (
    ExpressionCtx,
    JoinOnExpressionCtx,
    OrderByExpressionCtx,
)
from dl_core_tests.db.base import DefaultCoreTestClass


class PGOpRunnerTestBase(DefaultCoreTestClass):
    PG_POOL_WRAPPER_CLS: ClassVar[Type[BasePgPoolWrapper]]
    PG_PROCESSOR_CLS: ClassVar[Type[PostgreSQLOperationProcessor]]

    @pytest.fixture(scope="function")
    async def pg_op_processor(self, loop, conn_default_service_registry) -> PostgreSQLOperationProcessor:
        service_registry = conn_default_service_registry
        compeng_pg_dsn = self.core_test_config.get_compeng_url()
        async with self.PG_POOL_WRAPPER_CLS.context(compeng_pg_dsn) as pool_wrapper:
            async with self.PG_PROCESSOR_CLS(service_registry=service_registry, pg_pool=pool_wrapper) as processor:
                yield processor

    @pytest.fixture(scope="function")
    def input_stream(self) -> DataStreamAsync:
        names = ["int_value", "str_value", "dt_value", "gdt_value"]
        user_types = [UserDataType.integer, UserDataType.string, UserDataType.datetime, UserDataType.genericdatetime]
        now = datetime.now(tz=timezone.utc)
        input_data = [
            [
                i,
                f"str_{i}",
                now + timedelta(seconds=i),
                now + timedelta(seconds=i),
            ]
            for i in range(1000)
        ]
        return DataStreamAsync(
            id="1",
            data=AsyncChunked.from_chunked_iterable([input_data]),
            names=names,
            user_types=user_types,
            meta=DataRequestMetaInfo(),
            data_key=LocalKeyRepresentation(),
        )

    @pytest.mark.asyncio
    async def test_upload_calc_calc_download(self, pg_op_processor, input_stream):
        table_name = str(uuid.uuid4())

        output_streams = await pg_op_processor.run(
            streams=[input_stream],
            operations=[
                UploadOp(
                    result_id="1",
                    source_stream_id="1",
                    dest_stream_id="2",
                    alias=table_name,
                ),
                CalcOp(
                    result_id="2",
                    source_stream_id="2",
                    dest_stream_id="3",
                    alias=str(uuid.uuid4()),
                    bi_query=BIQuery(
                        select_expressions=[
                            ExpressionCtx(
                                expression=sa.func.MAX(sa.literal_column(input_stream.names[0])),
                                alias="value_1",
                                user_type=UserDataType.integer,
                            ),
                            ExpressionCtx(
                                expression=sa.func.MIN(sa.literal_column(input_stream.names[1])),
                                alias="value_2",
                                user_type=UserDataType.string,
                            ),
                        ],
                        group_by_expressions=[
                            ExpressionCtx(
                                expression=(sa.literal_column(input_stream.names[0]) / 100) * 100,
                                user_type=UserDataType.integer,
                            ),
                        ],
                    ),
                    data_key_data="__qwerty",  # just a random hashable
                ),
                CalcOp(
                    result_id="2",
                    source_stream_id="3",
                    dest_stream_id="4",
                    alias=str(uuid.uuid4()),
                    bi_query=BIQuery(
                        select_expressions=[
                            ExpressionCtx(
                                expression=(
                                    sa.literal_column("value_2").concat("_").concat(sa.literal_column("value_1"))
                                ),
                                alias="combined_value",
                                user_type=UserDataType.string,
                            ),
                        ],
                        order_by_expressions=[
                            OrderByExpressionCtx(
                                expression=sa.literal_column("value_1"),
                                user_type=UserDataType.string,
                            ),
                        ],
                    ),
                    data_key_data="__uiop",  # just a random hashable
                ),
                DownloadOp(
                    source_stream_id="4",
                    dest_stream_id="5",
                ),
            ],
            output_stream_ids=["5"],
        )

        assert len(output_streams) == 1
        output_stream = output_streams[0]
        assert output_stream.id == "5"

        output_data = await output_stream.data.all()
        assert output_data == [
            ["str_0_99"],
            ["str_100_199"],
            ["str_200_299"],
            ["str_300_399"],
            ["str_400_499"],
            ["str_500_599"],
            ["str_600_699"],
            ["str_700_799"],
            ["str_800_899"],
            ["str_900_999"],
        ]

    @pytest.mark.asyncio
    async def test_upload_deduplication(self, pg_op_processor, input_stream):
        table_name = str(uuid.uuid4())

        output_streams = await pg_op_processor.run(
            streams=[input_stream],
            operations=[
                UploadOp(
                    result_id="1",
                    source_stream_id="1",
                    dest_stream_id="2",
                    alias=table_name,
                ),
                CalcOp(
                    result_id="2",
                    source_stream_id="2",
                    dest_stream_id="3",
                    alias=str(uuid.uuid4()),
                    bi_query=BIQuery(
                        select_expressions=[
                            ExpressionCtx(
                                expression=sa.literal_column(input_stream.names[0]),
                                alias="value_1",
                                user_type=UserDataType.integer,
                            ),
                        ],
                    ),
                    data_key_data="__qwerty",  # just a random hashable
                ),
                JoinOp(
                    source_stream_ids={"2", "3"},
                    dest_stream_id="4",
                    join_on_expressions=[
                        JoinOnExpressionCtx(
                            expression=sa.literal_column(input_stream.names[0]),
                            user_type=UserDataType.integer,
                            join_type=JoinType.inner,
                            left_id="int_value",
                            right_id="value_1",
                        ),
                    ],
                    root_avatar_id="1",
                ),
                CalcOp(
                    result_id="4",
                    source_stream_id="4",
                    dest_stream_id="5",
                    alias=str(uuid.uuid4()),
                    bi_query=BIQuery(
                        select_expressions=[
                            ExpressionCtx(
                                expression=sa.literal_column(input_stream.names[0]),
                                alias="value_1",
                                user_type=UserDataType.integer,
                            ),
                        ],
                    ),
                    data_key_data="__qwerty",  # just a random hashable
                ),
                DownloadOp(
                    source_stream_id="5",
                    dest_stream_id="6",
                ),
            ],
            output_stream_ids=["6"],
        )

        assert len(output_streams) == 1
        output_stream = output_streams[0]
        assert output_stream.id == "6"

        output_data = await output_stream.data.all()
        assert output_data == [[i] for i in range(1000)]


class TestAiopgOpRunner(PGOpRunnerTestBase):
    PG_POOL_WRAPPER_CLS = AiopgPoolWrapper
    PG_PROCESSOR_CLS = AiopgOperationProcessor


class TestAsyncpgOpRunner(PGOpRunnerTestBase):
    PG_POOL_WRAPPER_CLS = AsyncpgPoolWrapper
    PG_PROCESSOR_CLS = AsyncpgOperationProcessor
