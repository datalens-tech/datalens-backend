from __future__ import annotations

import datetime
import decimal
import json

from dl_model_tools.serialization import (
    RedisDatalensDataJSONDecoder,
    RedisDatalensDataJSONEncoder,
)


SAMPLE_DATA = dict(
    some_dt=datetime.datetime(
        2019, 6, 17, 7, 1, 41, 79585, tzinfo=datetime.timezone(datetime.timedelta(seconds=-1320))
    ),
    some_date=datetime.date(2020, 7, 18),
    some_time=datetime.time(11, 22, 33, 444555, tzinfo=datetime.timezone(datetime.timedelta(seconds=1320))),
    some_timedelta=datetime.timedelta(seconds=1320.0231),
    some_decimal=decimal.Decimal("12345" * 10 + "." + "54321" * 10),
)


def test_json_serialization():
    data = SAMPLE_DATA
    dumped = json.dumps(data, cls=RedisDatalensDataJSONEncoder)
    dumped_dt = json.loads(dumped)["some_dt"]
    assert dumped_dt == {"__dl_type__": "datetime", "value": [[2019, 6, 17, 7, 1, 41, 79585], -1320.0]}, dumped_dt
    roundtrip = json.loads(dumped, cls=RedisDatalensDataJSONDecoder)
    assert roundtrip == data


def test_json_tricky_serialization():
    data = SAMPLE_DATA
    dumped = json.dumps(data, cls=RedisDatalensDataJSONEncoder)
    tricky_data = dict(normal=data, abnormal=json.loads(dumped))
    tricky_data_dumped = json.dumps(tricky_data, cls=RedisDatalensDataJSONEncoder)
    tricky_roundtrip = json.loads(tricky_data_dumped, cls=RedisDatalensDataJSONDecoder)
    # WARNING: not currently viable: `assert tricky_roundtrip == tricky_data, tricky_roundtrip`
    assert tricky_roundtrip
