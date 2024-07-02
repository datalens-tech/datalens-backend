import pytest

from dl_rls.utils import (
    quote_by_quote,
    split_by_quoted_quote,
)


TEST_DATA = ["'abc'de", "'ab''c'''de", "'ab''c'''"]
EXPECTED = [("abc", "de"), ("ab'c'", "de"), ("ab'c'", "")]


@pytest.mark.parametrize(["string", "expected"], zip(TEST_DATA, EXPECTED))
def test_quoting(string, expected):
    split = split_by_quoted_quote(string)
    assert split == expected
    assert quote_by_quote(split[0]) + split[1] == string
