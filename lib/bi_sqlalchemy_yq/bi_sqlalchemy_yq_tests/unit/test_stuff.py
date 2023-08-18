from __future__ import annotations

import os
import datetime

import pytest
import sqlalchemy as sa

import ydb.public.api.protos.draft.fq_pb2 as yqpb

from bi_sqlalchemy_yq.cli import DLYQClient


def test_engine(sa_engine):
    conn = sa_engine.connect()
    assert conn


CALL_NAME_TO_RESP = {
    'CreateQuery': yqpb.CreateQueryResponse(),
    'DescribeQuery': yqpb.DescribeQueryResponse(),
    'GetResultData': yqpb.GetResultDataResponse(),
    'DeleteQuery': yqpb.DeleteQueryResponse(),
}

RESPS = []


class MockupYQClient(DLYQClient):

    def _wrap_call(self, func, service_name, call_name, *args, **kwargs):

        sup = super()._wrap_call(func, service_name, call_name, *args, **kwargs)

        async def wrapped_call(*call_args, **call_kwargs):
            result = CALL_NAME_TO_RESP.get(call_name)
            if result is not None:
                return result
            if not os.environ.get('BISAYQ_ENGINE_URL'):
                raise Exception("Should not call network here")
            # Convenience code for updating the mockup values:
            result = await sup.aio(*call_args, **call_kwargs)
            print(
                f'\n======='
                f'\n    {call_name!r}: yqpb.{result.__class__.__name__}.FromString('
                f'\n        {result.SerializeToString()!r}  # noqa: E501'
                f'\n    ),'
                f'\n======='
            )
            return result

        wrapped_call.aio = wrapped_call

        return wrapped_call
        # raise Exception("Should not reach here in unit tests")


@pytest.mark.skip('Broken (endlesslooping)')
def test_result(engine_url):
    sa_engine = sa.create_engine(
        engine_url,
        connect_args=dict(
            cli_cls=MockupYQClient,
        ),
    )
    res = sa_engine.execute('''
    select
        cast(7 as UInt64) as id,
        cast('2021-06-07' as Date) as some_date,
        cast('2021-06-07T18:19:20Z' as Datetime) as some_datetime
    ''')
    description = res.cursor.description
    assert isinstance(description, tuple)
    cols = tuple((col[0], col[1]) for col in description)
    assert cols == (('id', 'Uint64?'), ('some_date', 'Date?'), ('some_datetime', 'Datetime?'))
    data = list(res)
    assert data == [(7, datetime.date(2021, 6, 7), datetime.datetime(2021, 6, 7, 18, 19, 20))]
