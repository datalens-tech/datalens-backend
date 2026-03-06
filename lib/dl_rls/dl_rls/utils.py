from typing import (
    Iterable,
    TypeVar,
)

from dl_rls.models import RLSEntry


_T = TypeVar("_T")


# TODO: replace with itertools.batched after switching to Python 3.12
def chunks(lst: list[_T], size: int) -> Iterable[list[_T]]:
    """Yield successive chunks from lst. No padding."""
    for idx in range(0, len(lst), size):
        yield lst[idx : idx + size]


def split_by_quoted_quote(value: str, quote: str = "'") -> tuple[str, str]:
    """
    Parse out a quoted value at the beginning,
    where quotes are quoted by doubling (CSV-like).

    >>> split_by_quoted_quote("'abc'de")
    ('abc', 'de')
    >>> split_by_quoted_quote("'ab''c'''de")
    ("ab'c'", 'de')
    >>> split_by_quoted_quote("'ab''c'''")
    ("ab'c'", '')
    """
    ql = len(quote)
    if not value.startswith(quote):
        raise ValueError("Value does not start with quote")
    value = value[ql:]
    result = []
    while True:
        try:
            next_quote = value.index(quote)
        except ValueError as e:
            raise ValueError("Unclosed quote") from e
        value_piece = value[:next_quote]
        result.append(value_piece)
        value = value[next_quote + ql :]
        if value.startswith(quote):
            result.append(quote)
            value = value[ql:]
        else:  # some other text, or end-of-line.
            break

    return "".join(result), value


def quote_by_quote(value: str, quote: str = "'") -> str:
    """
    Inverse function for split_by_quoted_quote

    >>> quote_by_quote("a'b'")
    "'a''b'''"
    >>> split_by_quoted_quote(quote_by_quote("a'b'") + "and 'stuff'")
    ("a'b'", "and 'stuff'")
    """
    return "{}{}{}".format(quote, value.replace(quote, quote + quote), quote)


def is_slug(group_id: str, group_name: str) -> bool:
    return group_id == group_name.strip().removeprefix("@group:")


def rls_uses_real_group_ids(group_entries: list[RLSEntry]) -> bool | None:
    """
    This method is only needed during the transition period from group slugs to their actual IDs.
    Once the rls_v1 format is removed, it can be deleted - the result will be always True.
    """

    has_slugs = False
    has_ids = False
    for entry in group_entries:
        if is_slug(group_id=entry.subject.subject_id, group_name=entry.subject.subject_name):
            has_slugs = True
        else:
            has_ids = True
    if has_ids and has_slugs:
        # This is an unlikely situation that could only arise if one of the groups failed
        # to resolve to a real ID due to a resolver error unrelated to the group itself.
        # However, it is hypothetically possible, so to avoid breaking the dataset,
        # we will handle this case separatel.
        return None
    if has_slugs:
        return False
    if has_ids:
        return True
    # An empty list case. We'll return True, since that's the intended behavior.
    return True
