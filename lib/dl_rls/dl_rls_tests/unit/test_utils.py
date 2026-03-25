import pytest

from dl_constants.enums import RLSSubjectType
from dl_rls.models import (
    RLSEntry,
    RLSSubject,
)
from dl_rls.utils import (
    is_slug,
    quote_by_quote,
    rls_uses_real_group_ids,
    split_by_quoted_quote,
)


TEST_DATA = ["'abc'de", "'ab''c'''de", "'ab''c'''"]
EXPECTED = [("abc", "de"), ("ab'c'", "de"), ("ab'c'", "")]


@pytest.mark.parametrize(["string", "expected"], zip(TEST_DATA, EXPECTED))
def test_quoting(string: str, expected: tuple[str, str]) -> None:
    split = split_by_quoted_quote(string)
    assert split == expected
    assert quote_by_quote(split[0]) + split[1] == string


@pytest.mark.parametrize(
    "group_id, group_name, expected",
    [
        ("my-group", "@group:my-group", True),
        ("my-group", " @group:my-group  ", True),
        ("abc123def456", "@group:my-group", False),
        ("my-group", "my-group", True),
    ],
    ids=[
        "slug_matches",
        "slug_with_whitespaces",
        "real_id_differs",
        "no_prefix",
    ],
)
def test_is_slug(group_id: str, group_name: str, expected: bool) -> None:
    assert is_slug(group_id=group_id, group_name=group_name) is expected


def _make_group_entry(group_id: str, group_name: str) -> RLSEntry:
    return RLSEntry(
        field_guid="test-field",
        allowed_value="some-value",
        subject=RLSSubject(
            subject_type=RLSSubjectType.group,
            subject_id=group_id,
            subject_name=group_name,
        ),
    )


@pytest.mark.parametrize(
    "entries, expected",
    [
        ([], True),
        (
            [
                _make_group_entry("group-a", "@group:group-a"),
                _make_group_entry("group-b", "@group:group-b"),
            ],
            False,
        ),
        (
            [
                _make_group_entry("real-id-123", "@group:group-a"),
                _make_group_entry("real-id-456", "@group:group-b"),
            ],
            True,
        ),
        (
            [
                _make_group_entry("group-a", "@group:group-a"),
                _make_group_entry("real-id-456", "@group:group-b"),
            ],
            None,
        ),
    ],
    ids=[
        "empty_list",
        "all_slugs",
        "all_real_ids",
        "mixed_slugs_and_ids",
    ],
)
def test_rls_uses_real_group_ids(entries: list[RLSEntry], expected: bool | None) -> None:
    assert rls_uses_real_group_ids(entries) is expected
