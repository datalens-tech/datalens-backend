import attrs
from typing_extensions import Self

import dl_httpx
import dl_httpx_tests.unit.error_transformers.conftest as conftest


@attrs.define(kw_only=True)
class FakeNotFound(Exception):
    code: str
    message: str | None = None
    details: list[dict] | None = None

    @classmethod
    def from_httpx_exception(cls, exception: dl_httpx.HttpStatusHttpxClientException) -> Self:
        body = exception.response.json()
        return cls(
            code=body["code"],
            message=body.get("message"),
            details=body.get("details"),
        )


@attrs.define(kw_only=True)
class FakeForbidden(Exception):
    code: str

    @classmethod
    def from_httpx_exception(cls, exception: dl_httpx.HttpStatusHttpxClientException) -> Self:
        return cls(code=exception.response.json()["code"])


def test_code_map_hit_calls_from_httpx_exception() -> None:
    transformer = dl_httpx.CodeMapTransformer(code_map={"NOT_FOUND": FakeNotFound.from_httpx_exception})
    exception = conftest.make_http_status_exception(
        status_code=404,
        json_body={"code": "NOT_FOUND", "message": "nope", "details": [{"field": "id"}]},
    )

    result = transformer.transform(exception)

    assert isinstance(result, FakeNotFound)
    assert result.code == "NOT_FOUND"
    assert result.message == "nope"
    assert result.details == [{"field": "id"}]


def test_code_map_miss_returns_none() -> None:
    transformer = dl_httpx.CodeMapTransformer(code_map={"NOT_FOUND": FakeNotFound.from_httpx_exception})
    exception = conftest.make_http_status_exception(
        status_code=403,
        json_body={"code": "SOMETHING_ELSE"},
    )
    assert transformer.transform(exception) is None


def test_code_map_no_code_key_returns_none() -> None:
    transformer = dl_httpx.CodeMapTransformer(code_map={"NOT_FOUND": FakeNotFound.from_httpx_exception})
    exception = conftest.make_http_status_exception(status_code=500, json_body={"message": "oops"})
    assert transformer.transform(exception) is None


def test_code_map_non_json_body_returns_none() -> None:
    transformer = dl_httpx.CodeMapTransformer(code_map={"NOT_FOUND": FakeNotFound.from_httpx_exception})
    exception = conftest.make_http_status_exception(status_code=502, text_body="not json")
    assert transformer.transform(exception) is None


def test_code_map_multiple_entries_picks_correct_class() -> None:
    transformer = dl_httpx.CodeMapTransformer(
        code_map={"NOT_FOUND": FakeNotFound.from_httpx_exception, "FORBIDDEN": FakeForbidden.from_httpx_exception},
    )
    exception = conftest.make_http_status_exception(status_code=403, json_body={"code": "FORBIDDEN"})
    result = transformer.transform(exception)
    assert isinstance(result, FakeForbidden)
    assert result.code == "FORBIDDEN"


@attrs.define(kw_only=True)
class FakeWrappedNotFound(Exception):
    code: str

    @classmethod
    def from_httpx_exception(cls, exception: dl_httpx.HttpStatusHttpxClientException) -> Self:
        return cls(code=exception.response.json()["error"]["code"])


def test_code_map_status_body_path_navigates_into_wrapper() -> None:
    transformer = dl_httpx.CodeMapTransformer(
        code_map={"NOT_FOUND": FakeWrappedNotFound.from_httpx_exception},
        status_body_path=("error", "code"),
    )
    exception = conftest.make_http_status_exception(
        status_code=404,
        json_body={"error": {"code": "NOT_FOUND"}},
    )

    result = transformer.transform(exception)

    assert isinstance(result, FakeWrappedNotFound)
    assert result.code == "NOT_FOUND"


def test_code_map_status_body_path_missing_key_returns_none() -> None:
    transformer = dl_httpx.CodeMapTransformer(
        code_map={"NOT_FOUND": FakeWrappedNotFound.from_httpx_exception},
        status_body_path=("error", "code"),
    )
    exception = conftest.make_http_status_exception(
        status_code=404,
        json_body={"code": "NOT_FOUND"},
    )
    assert transformer.transform(exception) is None


def test_code_map_status_body_path_non_dict_at_path_returns_none() -> None:
    transformer = dl_httpx.CodeMapTransformer(
        code_map={"NOT_FOUND": FakeWrappedNotFound.from_httpx_exception},
        status_body_path=("error", "code"),
    )
    exception = conftest.make_http_status_exception(
        status_code=404,
        json_body={"error": "not a dict"},
    )
    assert transformer.transform(exception) is None
