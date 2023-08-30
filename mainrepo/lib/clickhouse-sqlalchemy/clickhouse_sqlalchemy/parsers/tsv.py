
import codecs

from .base import Parts, ParseError, to_str


__all__ = [
    'parse_tsv_line',
    'parse_TabSeparatedWithNamesAndTypes_general',
    'parse_TabSeparatedWithNamesAndTypes',
    'parse_TabSeparatedWithNamesAndTypes_with_totals',
]


def unescape(value):
    return codecs.escape_decode(value)[0].decode('utf-8', errors='replace')


def parse_tsv_line(line):
    if line and line[-1] == b'\n':
        line = line[:-1]
    return [
        unescape(value) if value != b'\\N' else None
        for value in line.split(b'\t')]


def chunks_to_lines(chunks, sep=b'\n'):
    r"""

    >>> data = b'value\nString\na\nb\nc\nd\n'
    >>> data.split(b'\n')
    [b'value', b'String', b'a', b'b', b'c', b'd', b'']
    >>> list(chunks_to_lines([data])) == data.split(b'\n')
    True
    """
    unflushed = None
    for chunk in chunks:
        pieces = chunk.split(sep)
        for piece in pieces[:-1]:  # all-but-last
            if unflushed is not None:
                piece = unflushed + piece
                unflushed = None
            yield piece
        if unflushed is None:
            unflushed = pieces[-1]
        else:
            unflushed += pieces[-1]
    if unflushed is not None:
        yield unflushed


def chunks_with_terminating_newline_to_lines(chunks, sep=b'\n'):
    r"""
    Wrapper over `chunks_to_lines` that requires and strips the last newline.

    >>> data = b'value\nString\na\nb\nc\nd\n'
    >>> list(chunks_with_terminating_newline_to_lines([data]))
    [b'value', b'String', b'a', b'b', b'c', b'd', b'']
    >>> list(chunks_with_terminating_newline_to_lines([data.rstrip()]))
    Traceback (most recent call last):
      ...
    ValueError: ('Missing the terminating newline', b'd')
    >>> list(chunks_with_terminating_newline_to_lines([b'xx']))
    Traceback (most recent call last):
      ...
    ValueError: ('Missing the terminating newline', b'xx')
    """
    items = iter(chunks_to_lines(chunks, sep=sep))
    try:
        prev_item = next(items)
    except StopIteration:
        return
    item = prev_item
    for item in items:
        yield prev_item
        prev_item = item
    if item != b'':
        raise ValueError('Missing the terminating newline', item)


def parse_tsv_chunks(chunks):
    for line in chunks_with_terminating_newline_to_lines(chunks):
        yield parse_tsv_line(line)


UNSPECIFIED = object()


def parse_TabSeparatedWithNamesAndTypes_general(
        chunks, with_totals=UNSPECIFIED, empty_string=''):
    """
    General / parametric case:
    required totals / no totals / ambiguous maybe-there-will-be-totals.
    """
    items = iter(parse_tsv_chunks(chunks))
    with_totals_ambiguous = with_totals is UNSPECIFIED
    with_totals = bool(with_totals)

    try:
        names = next(items)
        types = next(items)
    except StopIteration:
        raise ParseError('Unexpected stream end', 'STATE_START')

    if len(names) != len(types):
        raise ParseError(
            'Mismatch in amount of column names and column types',
            'STATE_META',
            etcetera=dict(names=names, types=types))

    metadata = [
        {'name': to_str(name), 'type': to_str(type_name)}
        for name, type_name in zip(names, types)]
    yield Parts.META, metadata

    def get_next_item():
        if lookahead:
            return lookahead.pop(0)
        return next(items)  # raises StopIteration

    def data_result(some_item):
        if len(some_item) != len(metadata):
            raise ParseError('Unexpected row width', 'STATE_DATA',
                             etcetera=dict(item=some_item, metadata=metadata))
        return Parts.DATA, some_item

    def totals_result(some_item):
        if len(some_item) != len(metadata):
            raise ParseError('Unexpected row width', 'STATE_TOTALS',
                             etcetera=dict(item=some_item, metadata=metadata))
        return Parts.TOTALS, some_item

    lookahead = []
    state = 'STATE_DATA'
    while True:
        try:
            item = get_next_item()
        except StopIteration:
            break

        if item == [empty_string]:
            # Cases:
            #
            #  * A single string column which happens to have an empty value,
            #  * An empty line before totals.

            while len(lookahead) < 2:
                try:
                    lookahead.append(next(items))
                except StopIteration:
                    break

            if lookahead == []:  # '…\n' – end of the stream.
                break
            assert lookahead
            if len(lookahead) == 1:  # end of the stream
                if with_totals:
                    totals_item = lookahead[0]
                    state = 'FINISHED'
                    yield totals_result(totals_item)
                    break
            yield data_result(item)
            continue
        else:
            yield data_result(item)

    if with_totals and not with_totals_ambiguous and state == 'STATE_DATA':
        raise ParseError(
            'Stream ended before totals', state,
            etcetera=dict(item=item))


def parse_TabSeparatedWithNamesAndTypes_lines(lines, **kwargs):
    items = (parse_tsv_line(line) for line in lines)

    try:
        names = next(items)
        types = next(items)
    except StopIteration:
        # Have to support parsing a completely empty stream
        # (for e.g. DDL requests).
        return

    if len(names) != len(types):
        raise ParseError(
            'Mismatch in amount of column names and column types',
            'STATE_META',
            etcetera=dict(names=names, types=types))

    metadata = [
        {'name': to_str(name), 'type': to_str(type_name)}
        for name, type_name in zip(names, types)]
    yield Parts.META, metadata

    for item in items:
        if len(item) != len(metadata):
            raise ParseError('Unexpected row width', 'STATE_DATA',
                             etcetera=dict(item=item, metadata=metadata))
        yield Parts.DATA, item


def parse_TabSeparatedWithNamesAndTypes(chunks, **kwargs):
    lines = chunks_with_terminating_newline_to_lines(chunks)
    return parse_TabSeparatedWithNamesAndTypes_lines(lines, **kwargs)


def parse_TabSeparatedWithNamesAndTypes_with_totals(chunks, **kwargs):
    return parse_TabSeparatedWithNamesAndTypes_general(
        chunks, with_totals=True, **kwargs)
