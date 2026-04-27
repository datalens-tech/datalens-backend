import dl_httpx
import dl_httpx_tests.unit.error_transformers.conftest as conftest


def test_null_transformer_returns_none() -> None:
    transformer = dl_httpx.NullErrorTransformer()
    exception = conftest.make_http_status_exception(status_code=500, json_body={"error": "boom"})
    assert transformer.transform(exception) is None


def test_null_transformer_singleton_is_null_transformer() -> None:
    assert isinstance(dl_httpx.NULL_ERROR_TRANSFORMER, dl_httpx.NullErrorTransformer)
