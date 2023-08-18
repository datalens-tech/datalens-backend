from __future__ import annotations

import uuid
from datetime import datetime, timedelta, timezone
from typing import ClassVar, Type

import pytest
import sqlalchemy as sa

from bi_constants.enums import BIType

from bi_core.data_processing.processing.operation import (
    UploadOp, DownloadOp, CalcOp
)
from bi_compeng_pg.compeng_pg_base.processor_base import PostgreSQLOperationProcessor
from bi_compeng_pg.compeng_aiopg.processor_aiopg import AiopgOperationProcessor
from bi_compeng_pg.compeng_asyncpg.processor_asyncpg import AsyncpgOperationProcessor
from bi_compeng_pg.compeng_pg_base.pool_base import BasePgPoolWrapper
from bi_compeng_pg.compeng_aiopg.pool_aiopg import AiopgPoolWrapper
from bi_compeng_pg.compeng_asyncpg.pool_asyncpg import AsyncpgPoolWrapper
from bi_core.data_processing.streaming import AsyncChunked
from bi_core.data_processing.stream_base import DataStreamAsync, DataRequestMetaInfo
from bi_core.query.expression import ExpressionCtx, OrderByExpressionCtx
from bi_core.query.bi_query import BIQuery


class PGOpRunnerTestBase:
    PG_POOL_WRAPPER_CLS: ClassVar[Type[BasePgPoolWrapper]]
    PG_PROCESSOR_CLS: ClassVar[Type[PostgreSQLOperationProcessor]]

    @pytest.mark.asyncio
    async def test_upload_calc_calc_download(self, pg_op_processor):
        names = ['int_value', 'str_value', 'dt_value', 'gdt_value']
        user_types = [BIType.integer, BIType.string, BIType.datetime, BIType.genericdatetime]
        now = datetime.now(tz=timezone.utc)
        input_data = [
            [i, f'str_{i}', now + timedelta(seconds=i), now + timedelta(seconds=i), ]
            for i in range(1000)
        ]
        input_stream = DataStreamAsync(
            id='1',
            data=AsyncChunked.from_chunked_iterable([input_data]),
            names=names,
            user_types=user_types,
            meta=DataRequestMetaInfo(),
            data_key=None
        )

        table_name = str(uuid.uuid4())

        output_streams = await pg_op_processor.run(
            streams=[input_stream],
            operations=[
                UploadOp(
                    result_id='1',
                    source_stream_id='1',
                    dest_stream_id='2',
                    alias=table_name,
                ),
                CalcOp(
                    result_id='2',
                    source_stream_id='2',
                    dest_stream_id='3',
                    alias=str(uuid.uuid4()),
                    bi_query=BIQuery(
                        select_expressions=[
                            ExpressionCtx(
                                expression=sa.func.MAX(sa.literal_column(names[0])),
                                alias='value_1', user_type=BIType.integer,
                            ),
                            ExpressionCtx(
                                expression=sa.func.MIN(sa.literal_column(names[1])),
                                alias='value_2', user_type=BIType.string,
                            ),
                        ],
                        group_by_expressions=[
                            ExpressionCtx(
                                expression=(sa.literal_column(names[0]) / 100) * 100,
                                user_type=BIType.integer,
                            ),
                        ]
                    )
                ),
                CalcOp(
                    result_id='2',
                    source_stream_id='3',
                    dest_stream_id='4',
                    alias=str(uuid.uuid4()),
                    bi_query=BIQuery(
                        select_expressions=[
                            ExpressionCtx(
                                expression=(
                                    sa.literal_column('value_2')
                                    .concat('_')
                                    .concat(sa.literal_column('value_1'))
                                ),
                                alias='combined_value', user_type=BIType.string,
                            ),
                        ],
                        order_by_expressions=[
                            OrderByExpressionCtx(
                                expression=sa.literal_column('value_1'),
                                user_type=BIType.string,
                            ),
                        ]
                    )
                ),
                DownloadOp(
                    source_stream_id='4',
                    dest_stream_id='5',
                )
            ],
            output_stream_ids=['5'],
        )

        assert len(output_streams) == 1
        output_stream = output_streams[0]
        assert output_stream.id == '5'

        output_data = await output_stream.data.all()
        assert output_data == [
            ['str_0_99'],
            ['str_100_199'],
            ['str_200_299'],
            ['str_300_399'],
            ['str_400_499'],
            ['str_500_599'],
            ['str_600_699'],
            ['str_700_799'],
            ['str_800_899'],
            ['str_900_999'],
        ]

    @pytest.fixture(scope='function')
    async def pg_op_processor(self, loop, compeng_pg_dsn) -> PostgreSQLOperationProcessor:
        async with self.PG_POOL_WRAPPER_CLS.context(compeng_pg_dsn) as pool_wrapper:
            async with self.PG_PROCESSOR_CLS(pg_pool=pool_wrapper) as processor:
                yield processor


class TestAiopgOpRunner(PGOpRunnerTestBase):
    PG_POOL_WRAPPER_CLS = AiopgPoolWrapper
    PG_PROCESSOR_CLS = AiopgOperationProcessor


class TestAsyncpgOpRunner(PGOpRunnerTestBase):
    PG_POOL_WRAPPER_CLS = AsyncpgPoolWrapper
    PG_PROCESSOR_CLS = AsyncpgOperationProcessor
