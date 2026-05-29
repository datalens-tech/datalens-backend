from typing import Self

import attrs

import dl_httpx
import dl_httpx_tests.unit.error_transformers.conftest as conftest


@attrs.define(kw_only=True)
class FakeNotFound(Exception):
    status_code: int

    @classmethod
    def from_httpx_exception(cls, exception: dl_httpx.HttpStatusHttpxClientException) -> Self:
        return cls(status_code=exception.response.status_code)


@attrs.define(kw_only=True)
class FakeForbidden(Exception):
    status_code: int

    @classmethod
    def from_httpx_exception(cls, exception: dl_httpx.HttpStatusHttpxClientException) -> Self:
        return cls(status_code=exception.response.status_code)


def test_status_map_hit_calls_from_httpx_exception() -> None:
    transformer = dl_httpx.StatusMapTransformer(status_map={404: FakeNotFound.from_httpx_exception})
    exception = conftest.make_http_status_exception(status_code=404)

    result = transformer.transform(exception)

    assert isinstance(result, FakeNotFound)
    assert result.status_code == 404


def test_status_map_picks_class_by_status_code() -> None:
    transformer = dl_httpx.StatusMapTransformer(
        status_map={404: FakeNotFound.from_httpx_exception, 403: FakeForbidden.from_httpx_exception},
    )
    exception = conftest.make_http_status_exception(status_code=403)

    result = transformer.transform(exception)

    assert isinstance(result, FakeForbidden)
    assert result.status_code == 403


def test_status_map_miss_returns_none() -> None:
    transformer = dl_httpx.StatusMapTransformer(status_map={404: FakeNotFound.from_httpx_exception})
    exception = conftest.make_http_status_exception(status_code=500)
    assert transformer.transform(exception) is None


def test_status_map_empty_returns_none() -> None:
    transformer = dl_httpx.StatusMapTransformer(status_map={})
    exception = conftest.make_http_status_exception(status_code=404)
    assert transformer.transform(exception) is None
