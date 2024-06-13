from typing import (
    Iterable,
    TypeVar,
)


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
