import attrs

import dl_httpx
import dl_httpx_tests.unit.error_transformers.conftest as conftest


class FakeFirstError(Exception):
    pass


class FakeSecondError(Exception):
    pass


@attrs.define(frozen=True)
class _AlwaysTransform:
    exception: Exception

    def transform(self, exception: dl_httpx.HttpStatusHttpxClientError) -> Exception | None:
        return self.exception


@attrs.define(frozen=True)
class _NeverTransform:
    def transform(self, exception: dl_httpx.HttpStatusHttpxClientError) -> Exception | None:
        return None


def test_chain_first_non_none_wins() -> None:
    first = _AlwaysTransform(exception=FakeFirstError())
    second = _AlwaysTransform(exception=FakeSecondError())
    chain = dl_httpx.ChainTransformer(transformers=[first, second])
    exception = conftest.make_http_status_exception(status_code=500)

    result = chain.transform(exception)

    assert isinstance(result, FakeFirstError)


def test_chain_skips_none_results() -> None:
    first = _NeverTransform()
    second = _AlwaysTransform(exception=FakeSecondError())
    chain = dl_httpx.ChainTransformer(transformers=[first, second])
    exception = conftest.make_http_status_exception(status_code=500)

    result = chain.transform(exception)

    assert isinstance(result, FakeSecondError)


def test_chain_all_none_returns_none() -> None:
    chain = dl_httpx.ChainTransformer(transformers=[_NeverTransform(), _NeverTransform()])
    exception = conftest.make_http_status_exception(status_code=500)
    assert chain.transform(exception) is None


def test_chain_empty_returns_none() -> None:
    chain = dl_httpx.ChainTransformer(transformers=[])
    exception = conftest.make_http_status_exception(status_code=500)
    assert chain.transform(exception) is None
