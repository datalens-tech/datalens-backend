from __future__ import annotations

import pytest

import dl_utils


def test_join_in_chunks():
    # TODO: `hypothesis` test over these parameters
    values = list(map(str, range(1000)))
    sep = "_"
    max_len = 10
    # ...
    tst_result = list(
        dl_utils.utils.join_in_chunks(
            values,
            sep,
            max_len=max_len,
        )
    )
    effective_max_len = max(max_len, max(len(val) + 1 for val in values))
    assert all(len(piece) < effective_max_len for piece in tst_result)
    assert [item for piece in tst_result for item in piece.split(sep)] == values


@pytest.mark.parametrize(
    "url_or_path,expected",
    [
        ("/api/v1/query?order_by=category&token=mytoken", "/api/v1/query?order_by=[hidden]&token=[hidden]"),
        ("", ""),
        (None, None),
        ("https://example.com", "https://example.com"),
        ("https://example.com/some/path", "https://example.com/some/path"),
        ("/some/path", "/some/path"),
        ("/some/path?", "/some/path?"),
        ("/some/path#fragment", "/some/path#fragment"),
        ("/some/path?#fragment", "/some/path?#fragment"),
        ("/some/path?param1=hoba", "/some/path?param1=[hidden]"),
        ("/some/path?param1=hoba&param2=another_param", "/some/path?param1=[hidden]&param2=[hidden]"),
        ("/some/path?param1=hoba&param2=another_param#f", "/some/path?param1=[hidden]&param2=[hidden]#f"),
    ],
)
def test_hide_url_args(url_or_path: str, expected: str):
    assert dl_utils.utils.hide_url_args(url_or_path) == expected


@pytest.mark.parametrize(
    "request_id,attempt,expected",
    [
        ("abc123", 1, "abc123"),
        ("abc123", 2, "abc123/2"),
        ("abc123", 3, "abc123/3"),
        ("parent--sp.abc123", 2, "parent--sp.abc123/2"),
    ],
)
def test_append_retry_suffix(request_id: str, attempt: int, expected: str) -> None:
    assert dl_utils.append_retry_suffix(request_id, attempt) == expected
