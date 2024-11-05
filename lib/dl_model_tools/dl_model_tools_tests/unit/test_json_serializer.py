import json

import pytest

from dl_model_tools.serialization import (
    common_dumps,
    common_loads,
    safe_dumps,
)
from dl_model_tools_tests.unit.config import (
    EXPECTED_DUMP,
    SAMPLE_DATA,
    CustomType,
)
from dl_testing.utils import get_log_record


def test_json_serialization():
    data = SAMPLE_DATA
    dumped = common_dumps(data)
    dumped_dict = json.loads(dumped)
    assert dumped_dict == EXPECTED_DUMP
    roundtrip = common_loads(dumped)
    assert roundtrip == data


def test_json_tricky_serialization():
    tricky_data = dict(normal=SAMPLE_DATA, abnormal=EXPECTED_DUMP)
    tricky_data_dumped = common_dumps(tricky_data)
    tricky_roundtrip = common_loads(tricky_data_dumped)
    assert tricky_roundtrip["normal"] == tricky_data["normal"], tricky_roundtrip
    # abnormal data contains __dl_type__ fields, so decoder considers them to be dumps of BI types and decodes them
    assert tricky_roundtrip["abnormal"] == tricky_data["normal"], tricky_roundtrip


def test_safe_json_serialization(caplog):
    unserializable_data = SAMPLE_DATA | dict(unserializable=CustomType())
    with pytest.raises(TypeError, match="Object of type CustomType is not JSON serializable"):
        common_dumps(unserializable_data)

    safe_dumped = safe_dumps(unserializable_data)
    roundtrip = common_loads(safe_dumped)
    unserializable_value = roundtrip.pop("unserializable")
    assert unserializable_value is None
    assert roundtrip == SAMPLE_DATA

    log_record = get_log_record(caplog, predicate=lambda r: r.funcName == "to_jsonable", single=True)
    assert log_record.levelname == "WARNING"
    assert log_record.msg == "Value of type CustomType is not serializable, skipping serialization"
