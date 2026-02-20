import httpx
import pytest

import dl_constants
from dl_httpx.retry_mutator import RequestIdRetryMutator
import dl_retrier


REQUEST_ID_HEADER = dl_constants.DLHeadersCommon.REQUEST_ID.value


@pytest.fixture(name="mutator")
def fixture_mutator() -> RequestIdRetryMutator:
    return RequestIdRetryMutator()


def _make_retry(attempt_number: int) -> dl_retrier.Retry:
    return dl_retrier.Retry(
        attempt_number=attempt_number,
        request_timeout=10,
        connect_timeout=30,
        sleep_before_seconds=0,
    )


def _make_request(request_id: str) -> httpx.Request:
    return httpx.Request(
        "GET",
        "https://example.com",
        headers={REQUEST_ID_HEADER: request_id},
    )


def test_first_attempt_unchanged(mutator: RequestIdRetryMutator) -> None:
    request = _make_request("abc123")
    mutator.on_retry(request, _make_retry(1))
    assert request.headers[REQUEST_ID_HEADER] == "abc123"


def test_second_attempt_suffix(mutator: RequestIdRetryMutator) -> None:
    request = _make_request("abc123")
    mutator.on_retry(request, _make_retry(1))
    mutator.on_retry(request, _make_retry(2))
    assert request.headers[REQUEST_ID_HEADER] == "abc123/2"


def test_third_attempt_suffix(mutator: RequestIdRetryMutator) -> None:
    request = _make_request("abc123")
    mutator.on_retry(request, _make_retry(1))
    mutator.on_retry(request, _make_retry(2))
    mutator.on_retry(request, _make_retry(3))
    assert request.headers[REQUEST_ID_HEADER] == "abc123/3"


def test_with_parent_chain(mutator: RequestIdRetryMutator) -> None:
    request = _make_request("parent--sp.abc123")
    mutator.on_retry(request, _make_retry(1))
    assert request.headers[REQUEST_ID_HEADER] == "parent--sp.abc123"
    mutator.on_retry(request, _make_retry(2))
    assert request.headers[REQUEST_ID_HEADER] == "parent--sp.abc123/2"


def test_no_request_id_header(mutator: RequestIdRetryMutator) -> None:
    request = httpx.Request("GET", "https://example.com")
    mutator.on_retry(request, _make_retry(2))
    assert REQUEST_ID_HEADER not in request.headers


def test_shared_across_requests(mutator: RequestIdRetryMutator) -> None:
    req1 = _make_request("req-1")
    mutator.on_retry(req1, _make_retry(1))
    mutator.on_retry(req1, _make_retry(2))
    assert req1.headers[REQUEST_ID_HEADER] == "req-1/2"

    req2 = _make_request("req-2")
    mutator.on_retry(req2, _make_retry(1))
    mutator.on_retry(req2, _make_retry(2))
    assert req2.headers[REQUEST_ID_HEADER] == "req-2/2"
