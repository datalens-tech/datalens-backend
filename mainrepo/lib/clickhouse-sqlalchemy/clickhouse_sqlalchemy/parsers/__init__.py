
# from typing import Dict, Callable, Optional
from collections import namedtuple
from . import base
from .base import Parts, ParseError

__all__ = base.__all__ + (
    'FormatInfo', 'FORMATS',
    'Parts', 'ParseError',
)

# imported for namespace copy:
assert Parts
assert ParseError

_FormatInfo = namedtuple(
    '_FormatInfo',
    [
        'format_string', 'generator', 'generator_from_lines', 'sans_io',
        'chunk_size', 'parts',
    ])


class FormatInfo(_FormatInfo):

    def __new__(
            cls, format_string,
            generator=None, generator_from_lines=None, sans_io=None,
            chunk_size=64 * 1024, parts=base.Parts):
        """

        format_string: str: 'FORMAT {format_string}' part of the query text.

        generator:
        Callable[[Iterable[str]], Iterable[List[Any]]]
        (chunks to rows)

        generator_from_lines:
        Callable[[Iterable[str]], Iterable[List[Any]]]
        (lines to rows)

        sans_io: SansIOBase:
        feed-and-read class
        (`receive_data(data)`, `next_event()`)

        chunk_size: int: recommended chunk size for sans_io / generator.
        """
        return super(FormatInfo, cls).__new__(
            cls,
            format_string=format_string,
            generator=generator,
            generator_from_lines=generator_from_lines,
            sans_io=sans_io,
            chunk_size=chunk_size,
            parts=parts,
        )


# Wrapped into functions for lazy imports


def info_tsv():
    from .tsv import (
        parse_TabSeparatedWithNamesAndTypes,
        parse_TabSeparatedWithNamesAndTypes_lines,
    )
    return FormatInfo(
        format_string='TabSeparatedWithNamesAndTypes',
        generator_from_lines=parse_TabSeparatedWithNamesAndTypes_lines,
        generator=parse_TabSeparatedWithNamesAndTypes,
    )


def info_tsv_with_totals():
    from .tsv import parse_TabSeparatedWithNamesAndTypes_with_totals
    return FormatInfo(
        # implicitly expected: 'group by ... with totals' in the query text.
        format_string='TabSeparatedWithNamesAndTypes',
        generator=parse_TabSeparatedWithNamesAndTypes_with_totals,
    )


def info_jsoncompact():
    from .jsoncompact import JSONCompactChunksParser
    return FormatInfo(
        format_string='JSONCompact',
        generator=JSONCompactChunksParser.as_generator,
        sans_io=JSONCompactChunksParser,
    )


FORMATS = {  # Dict[Optional[str], Callable[[], FormatInfo]
    'default': info_tsv,
    'TabSeparatedWithNamesAndTypes': info_tsv,
    'TabSeparatedWithNamesAndTypes with totals': info_tsv_with_totals,
    'JSONCompact': info_jsoncompact,
}
