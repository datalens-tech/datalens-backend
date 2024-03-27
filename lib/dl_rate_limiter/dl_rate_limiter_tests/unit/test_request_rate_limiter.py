import re

import attrs
import pytest
import pytest_mock

import dl_rate_limiter


def test_check_request_limit_no_patterns(mocker: pytest_mock.MockFixture):
    mock_event_limiter = mocker.Mock(spec=dl_rate_limiter.SyncEventRateLimiterProtocol)
    rate_limiter = dl_rate_limiter.SyncRequestRateLimiter(event_limiter=mock_event_limiter)
    result = rate_limiter.check_limit(
        request=dl_rate_limiter.Request(
            url="http://example.com",
            method="GET",
        )
    )

    assert result is True
    mock_event_limiter.check_limit.assert_not_called()


@pytest.mark.asyncio
async def test_async_check_request_limit_no_patterns(mocker: pytest_mock.MockFixture):
    mock_event_limiter = mocker.AsyncMock(spec=dl_rate_limiter.AsyncEventRateLimiterProtocol)
    rate_limiter = dl_rate_limiter.AsyncRequestRateLimiter(event_limiter=mock_event_limiter)
    result = await rate_limiter.check_limit(
        request=dl_rate_limiter.Request(
            url="http://example.com",
            method="GET",
        )
    )

    assert result is True
    mock_event_limiter.check_limit.assert_not_awaited()


def test_check_request_limit(mocker: pytest_mock.MockFixture):
    mock_event_limiter = mocker.Mock(spec=dl_rate_limiter.SyncEventRateLimiterProtocol)
    mock_event_limiter.check_limit.return_value = True
    rate_limiter = dl_rate_limiter.SyncRequestRateLimiter(
        event_limiter=mock_event_limiter,
        patterns=[
            dl_rate_limiter.RequestPattern(
                url_regex=re.compile(r"http://.*.com"),
                methods=frozenset(["GET", "PATCH"]),
                event_key_template=dl_rate_limiter.RequestEventKeyTemplate(
                    key="key",
                    headers=frozenset(["header"]),
                ),
                limit=1,
                window_ms=1000,
            )
        ],
    )
    request = dl_rate_limiter.Request(url="http://example.com", method="GET", headers={"header": "value"})

    result = rate_limiter.check_limit(request=request)
    assert result is True
    mock_event_limiter.check_limit.assert_called_once_with(
        event_key="key:value",
        limit=1,
        window_ms=1000,
    )

    # no headers
    mock_event_limiter.reset_mock()
    result = rate_limiter.check_limit(request=attrs.evolve(request, headers={}))
    assert result is True
    mock_event_limiter.check_limit.assert_not_called()

    # excessive headers
    mock_event_limiter.reset_mock()
    result = rate_limiter.check_limit(request=attrs.evolve(request, headers={"header": "value", "header2": "value2"}))
    assert result is True
    mock_event_limiter.check_limit.assert_called_once_with(
        event_key="key:value",
        limit=1,
        window_ms=1000,
    )

    # limit reached
    mock_event_limiter.reset_mock()
    mock_event_limiter.check_limit.return_value = False
    result = rate_limiter.check_limit(request=request)
    assert result is False
    mock_event_limiter.check_limit.assert_called_once_with(
        event_key="key:value",
        limit=1,
        window_ms=1000,
    )

    # Wrong method
    mock_event_limiter.reset_mock()
    mock_event_limiter.check_limit.return_value = False
    result = rate_limiter.check_limit(request=attrs.evolve(request, method="POST"))
    assert result is True
    mock_event_limiter.check_limit.assert_not_called()

    # Wrong pattern
    mock_event_limiter.reset_mock()
    mock_event_limiter.check_limit.return_value = False
    result = rate_limiter.check_limit(request=attrs.evolve(request, url="http://example.org"))
    assert result is True
    mock_event_limiter.check_limit.assert_not_called()


@pytest.mark.asyncio
async def test_async_check_request_limit(mocker: pytest_mock.MockFixture):
    mock_event_limiter = mocker.AsyncMock(spec=dl_rate_limiter.AsyncEventRateLimiterProtocol)
    mock_event_limiter.check_limit.return_value = True
    rate_limiter = dl_rate_limiter.AsyncRequestRateLimiter(
        event_limiter=mock_event_limiter,
        patterns=[
            dl_rate_limiter.RequestPattern(
                url_regex=re.compile(r"http://.*.com"),
                methods=frozenset(["GET", "PATCH"]),
                event_key_template=dl_rate_limiter.RequestEventKeyTemplate(
                    key="key",
                    headers=frozenset(["header"]),
                ),
                limit=1,
                window_ms=1000,
            )
        ],
    )
    request = dl_rate_limiter.Request(url="http://example.com", method="GET", headers={"header": "value"})

    result = await rate_limiter.check_limit(request=request)
    assert result is True
    mock_event_limiter.check_limit.assert_awaited_once_with(
        event_key="key:value",
        limit=1,
        window_ms=1000,
    )

    # no headers
    mock_event_limiter.reset_mock()
    result = await rate_limiter.check_limit(request=attrs.evolve(request, headers={}))
    assert result is True
    mock_event_limiter.check_limit.assert_not_awaited()

    # excessive headers
    mock_event_limiter.reset_mock()
    result = await rate_limiter.check_limit(
        request=attrs.evolve(request, headers={"header": "value", "header2": "value2"})
    )
    assert result is True
    mock_event_limiter.check_limit.assert_awaited_once_with(
        event_key="key:value",
        limit=1,
        window_ms=1000,
    )

    # limit reached
    mock_event_limiter.reset_mock()
    mock_event_limiter.check_limit.return_value = False
    result = await rate_limiter.check_limit(request=request)
    assert result is False
    mock_event_limiter.check_limit.assert_awaited_once_with(
        event_key="key:value",
        limit=1,
        window_ms=1000,
    )

    # Wrong method
    mock_event_limiter.reset_mock()
    mock_event_limiter.check_limit.return_value = False
    result = await rate_limiter.check_limit(request=attrs.evolve(request, method="POST"))
    assert result is True
    mock_event_limiter.check_limit.assert_not_awaited()

    # Wrong pattern
    mock_event_limiter.reset_mock()
    mock_event_limiter.check_limit.return_value = False
    result = await rate_limiter.check_limit(request=attrs.evolve(request, url="http://example.org"))
    assert result is True
    mock_event_limiter.check_limit.assert_not_awaited()


def test_check_request_limit_multiple_patterns(mocker: pytest_mock.MockFixture):
    mock_event_limiter = mocker.Mock(spec=dl_rate_limiter.SyncEventRateLimiterProtocol)
    mock_event_limiter.check_limit.return_value = True
    rate_limiter = dl_rate_limiter.SyncRequestRateLimiter(
        event_limiter=mock_event_limiter,
        patterns=[
            dl_rate_limiter.RequestPattern(
                url_regex=re.compile(r".*"),
                methods=frozenset(["GET", "POST"]),
                event_key_template=dl_rate_limiter.RequestEventKeyTemplate(
                    key="all_requests",
                    headers=frozenset(["header"]),
                ),
                limit=1,
                window_ms=1000,
            ),
            dl_rate_limiter.RequestPattern(
                url_regex=re.compile(r"http://.*.com"),
                methods=frozenset(["GET", "POST"]),
                event_key_template=dl_rate_limiter.RequestEventKeyTemplate(
                    key="dot_com_requests",
                    headers=frozenset(["header"]),
                ),
                limit=1,
                window_ms=1000,
            ),
        ],
    )

    request = dl_rate_limiter.Request(url="http://example.com", method="GET", headers={"header": "value"})

    # all passes
    result = rate_limiter.check_limit(request=request)
    assert result is True
    mock_event_limiter.check_limit.assert_has_calls(
        [
            mocker.call(event_key="all_requests:value", limit=1, window_ms=1000),
            mocker.call(event_key="dot_com_requests:value", limit=1, window_ms=1000),
        ]
    )

    # exit on first false
    mock_event_limiter.reset_mock()
    mock_event_limiter.check_limit.return_value = False
    result = rate_limiter.check_limit(request=request)
    assert result is False
    mock_event_limiter.check_limit.assert_called_once_with(event_key="all_requests:value", limit=1, window_ms=1000)

    # exit on second false
    mock_event_limiter.reset_mock()
    mock_event_limiter.check_limit.side_effect = [True, False]
    result = rate_limiter.check_limit(request=request)
    assert result is False
    mock_event_limiter.check_limit.assert_has_calls(
        [
            mocker.call(event_key="all_requests:value", limit=1, window_ms=1000),
            mocker.call(event_key="dot_com_requests:value", limit=1, window_ms=1000),
        ]
    )


@pytest.mark.asyncio
async def test_async_check_request_limit_multiple_patterns(mocker: pytest_mock.MockFixture):
    mock_event_limiter = mocker.AsyncMock(spec=dl_rate_limiter.AsyncEventRateLimiterProtocol)
    mock_event_limiter.check_limit.return_value = True
    rate_limiter = dl_rate_limiter.AsyncRequestRateLimiter(
        event_limiter=mock_event_limiter,
        patterns=[
            dl_rate_limiter.RequestPattern(
                url_regex=re.compile(r".*"),
                methods=frozenset(["GET", "POST"]),
                event_key_template=dl_rate_limiter.RequestEventKeyTemplate(
                    key="all_requests",
                    headers=frozenset(["header"]),
                ),
                limit=1,
                window_ms=1000,
            ),
            dl_rate_limiter.RequestPattern(
                url_regex=re.compile(r"http://.*.com"),
                methods=frozenset(["GET", "POST"]),
                event_key_template=dl_rate_limiter.RequestEventKeyTemplate(
                    key="dot_com_requests",
                    headers=frozenset(["header"]),
                ),
                limit=1,
                window_ms=1000,
            ),
        ],
    )

    request = dl_rate_limiter.Request(url="http://example.com", method="GET", headers={"header": "value"})

    # all passes
    result = await rate_limiter.check_limit(request=request)
    assert result is True
    mock_event_limiter.check_limit.assert_has_awaits(
        [
            mocker.call(event_key="all_requests:value", limit=1, window_ms=1000),
            mocker.call(event_key="dot_com_requests:value", limit=1, window_ms=1000),
        ]
    )

    # exit on first false
    mock_event_limiter.reset_mock()
    mock_event_limiter.check_limit.return_value = False
    result = await rate_limiter.check_limit(request=request)
    assert result is False
    mock_event_limiter.check_limit.assert_awaited_once_with(event_key="all_requests:value", limit=1, window_ms=1000)

    # exit on second false
    mock_event_limiter.reset_mock()
    mock_event_limiter.check_limit.side_effect = [True, False]
    result = await rate_limiter.check_limit(request=request)
    assert result is False
    mock_event_limiter.check_limit.assert_has_awaits(
        [
            mocker.call(event_key="all_requests:value", limit=1, window_ms=1000),
            mocker.call(event_key="dot_com_requests:value", limit=1, window_ms=1000),
        ]
    )
