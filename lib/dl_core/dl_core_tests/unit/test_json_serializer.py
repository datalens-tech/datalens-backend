from __future__ import annotations

import datetime
import decimal
import json
import uuid

from dl_model_tools.serialization import (
    RedisDatalensDataJSONDecoder,
    RedisDatalensDataJSONEncoder,
)


TZINFO = datetime.timezone(datetime.timedelta(seconds=-1320))
SAMPLE_DATA = dict(
    # Scalars
    some_int=42,
    some_float=19.89,
    some_str="Some say",
    some_bool=True,
    some_none=None,
    # BI data values
    some_dt=datetime.datetime(2019, 6, 17, 7, 1, 41, 79585, tzinfo=TZINFO),
    some_date=datetime.date(2020, 7, 18),
    some_time=datetime.time(11, 22, 33, 444555, tzinfo=TZINFO),
    some_timedelta=datetime.timedelta(seconds=1320.0231),
    some_decimal=decimal.Decimal("12345" * 9 + "." + "54321" * 9),
    some_uuid=uuid.UUID("12345678123456781234567812345678"),
    # some_bytes=b"Another one bites",  TODO: currently not serializable
)


EXPECTED_DUMP = dict(
    # Scalars are unchanged
    some_int=42,
    some_float=19.89,
    some_str="Some say",
    some_bool=True,
    some_none=None,
    # More complex types have custom dumpers
    some_dt={"__dl_type__": "datetime", "value": [[2019, 6, 17, 7, 1, 41, 79585], -1320.0]},
    some_date={"__dl_type__": "date", "value": "2020-07-18"},
    some_time={"__dl_type__": "time", "value": [[11, 22, 33, 444555], -1320.0]},
    some_timedelta={"__dl_type__": "timedelta", "value": 1320.0231},
    some_decimal={
        "__dl_type__": "decimal",
        "value": "123451234512345123451234512345123451234512345.543215432154321543215432154321543215432154321",
    },
    some_uuid={"__dl_type__": "uuid", "value": "12345678-1234-5678-1234-567812345678"},
    # some_bytes=b"Another one bites",
)


def test_json_serialization():
    data = SAMPLE_DATA
    dumped = json.dumps(data, cls=RedisDatalensDataJSONEncoder)
    dumped_dict = json.loads(dumped)
    assert dumped_dict == EXPECTED_DUMP
    roundtrip = json.loads(dumped, cls=RedisDatalensDataJSONDecoder)
    assert roundtrip == data


def test_json_tricky_serialization():
    data = SAMPLE_DATA
    dumped = json.dumps(data, cls=RedisDatalensDataJSONEncoder)
    tricky_data = dict(normal=data, abnormal=json.loads(dumped))
    tricky_data_dumped = json.dumps(tricky_data, cls=RedisDatalensDataJSONEncoder)
    tricky_roundtrip = json.loads(tricky_data_dumped, cls=RedisDatalensDataJSONDecoder)
    assert tricky_roundtrip["normal"] == tricky_data["normal"], tricky_roundtrip
    # scalar types are parsed with vanilla json, so dumped JSON string is parsed into proper object
    assert tricky_roundtrip["abnormal"] == tricky_data["normal"], tricky_roundtrip
