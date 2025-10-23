import pytest

import dl_json


@pytest.mark.parametrize(
    "value, string_json, bytes_json",
    [
        ("foo", '"foo"', b'"foo"'),
        (1, "1", b"1"),
        (1.1, "1.1", b"1.1"),
        (True, "true", b"true"),
        (False, "false", b"false"),
        (None, "null", b"null"),
        ([1, 2, 3], "[1,2,3]", b"[1,2,3]"),
        ({"a": 1, "b": 2}, '{"a":1,"b":2}', b'{"a":1,"b":2}'),
        ({"a": [1, 2, 3], "b": {"c": 4}}, '{"a":[1,2,3],"b":{"c":4}}', b'{"a":[1,2,3],"b":{"c":4}}'),
    ],
    ids=[
        "str",
        "int",
        "float",
        "bool-true",
        "bool-false",
        "none",
        "list",
        "dict",
        "nested",
    ],
)
def test_json(
    value: dl_json.JsonSerializable,
    string_json: str,
    bytes_json: bytes,
) -> None:
    assert dl_json.dumps_bytes(value) == bytes_json
    assert dl_json.loads_bytes(bytes_json) == value
    assert dl_json.dumps_str(value) == string_json
    assert dl_json.loads_str(string_json) == value


def test_dump_float_nan() -> None:
    assert dl_json.dumps_bytes(float("nan")) == b"null"
    assert dl_json.dumps_str(float("nan")) == "null"


def test_dump_float_inf() -> None:
    assert dl_json.dumps_bytes(float("inf")) == b"null"
    assert dl_json.dumps_str(float("inf")) == "null"


def test_dump_float_neg_inf() -> None:
    assert dl_json.dumps_bytes(float("-inf")) == b"null"
    assert dl_json.dumps_str(float("-inf")) == "null"
