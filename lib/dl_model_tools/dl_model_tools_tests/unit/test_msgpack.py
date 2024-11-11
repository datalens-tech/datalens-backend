import timeit

import flaky
import pytest

from dl_model_tools.msgpack import (
    DLMessagePackSerializer,
    DLSafeMessagePackSerializer,
)
from dl_model_tools.serialization import (
    common_dumps,
    common_loads,
)
from dl_model_tools_tests.unit.config import (
    PERF_DATA,
    SAMPLE_DATA,
    CustomType,
)
from dl_testing.utils import get_log_record


def test_serialization():
    serializer = DLMessagePackSerializer()
    data = SAMPLE_DATA
    assert data == serializer.loads(serializer.dumps(data))


def test_safe_serialization(caplog):
    unserializable_data = SAMPLE_DATA | dict(unserializable=CustomType())
    with pytest.raises(TypeError, match="Object of type CustomType is not MessagePack serializable"):
        DLMessagePackSerializer().dumps(unserializable_data)

    serializer = DLSafeMessagePackSerializer()
    safe_dumped = serializer.dumps(unserializable_data)
    roundtrip = serializer.loads(safe_dumped)
    unserializable_value = roundtrip.pop("unserializable")
    assert unserializable_value is None
    assert roundtrip == SAMPLE_DATA

    log_record = get_log_record(caplog, predicate=lambda r: r.funcName == "to_jsonable", single=True)
    assert log_record.levelname == "WARNING"
    assert log_record.msg == "Value of type CustomType is not serializable, skipping serialization"


@flaky.flaky(max_runs=3)
def test_perfomance():
    """Ensure msgpack serialization is significantly faster than json one"""

    def gen(obj, n=10):
        d = {str(i): obj for i in range(n)}
        lst = [obj for _ in range(n)]
        return {"dict": d, "list": lst, "scalar": 42}

    obj = gen(gen(gen(PERF_DATA)))
    serializer = DLMessagePackSerializer()
    json_timeit = timeit.timeit(lambda: common_loads(common_dumps(obj)), number=10)
    msgpack_timeit = timeit.timeit(lambda: serializer.loads(serializer.dumps(obj)), number=10)
    assert msgpack_timeit * 1.3 < json_timeit  # multiplier is selected experimentally
