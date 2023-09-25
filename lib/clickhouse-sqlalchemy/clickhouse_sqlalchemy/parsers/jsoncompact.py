# coding: utf8
"""
...
"""

from __future__ import unicode_literals

import enum
import json
import logging
from typing import Optional

from .base import (
    Parts,
    ParseError,
    SansIOBase,
)
from ..exceptions import DatabaseException

logger = logging.getLogger(__name__)


@enum.unique
class ParserState(enum.Enum):
    START = "STATE_START"
    NEXT_BLOCK = "STATE_NEXT_BLOCK"
    META = "STATE_META"
    DATA = "STATE_DATA"
    FOOTER = "STATE_FOOTER"
    TOTALS = "STATE_TOTALS"  # deprecated
    STATS = "STATE_STATS"  # deprecated
    FINISHED = "STATE_FINISHED"
    ERROR = "STATE_ERROR"


class JSONCompactChunksParser(SansIOBase):
    """
    A parser class for JSONCompact chunks.

    Assumes a very particular positioning of newlines and whitespaces (and the
    data structure). Use a clickhouse version whitelist for reliability.

    Structured similarly to `h11.Connection`.
    """

    # minimal chunk size to ensure all the whitespaces for parsing actually fit
    # in and there's at least something in it.
    min_chunk_size = 1024
    # A bunch of things are expected to fit in this size:
    # metadata, each data line, footer.
    # On the other hand, garbage data will be read up to this size (+ chunk
    # size) into memory until raising an error.
    max_chunk_size = 8 * 1024 * 1024

    parts = Parts

    STATE_START = ParserState.START.value
    STATE_NEXT_BLOCK = ParserState.NEXT_BLOCK.value
    STATE_META = ParserState.META.value
    STATE_DATA = ParserState.DATA.value
    STATE_FOOTER = ParserState.FOOTER.value
    STATE_TOTALS = ParserState.TOTALS.value  # deprecated
    STATE_STATS = ParserState.STATS.value  # deprecated
    STATE_FINISHED = ParserState.FINISHED.value
    STATE_ERROR = ParserState.ERROR.value

    class RowTooLarge(ParseError):
        """ ... """

    def __init__(self, autoparse=True):
        self._unread = []
        self._results = []
        self.state = self.STATE_START
        self._autoparse = autoparse

    def receive_data(self, data: Optional[bytes]):
        """ Add data to the internal receive buffer """
        if not data:
            return  # allow and ignore empty chunks, just in case.
        self._unread.append(data)

    @staticmethod
    def _splitpair(chunk, splitters, start=0):
        assert not isinstance(splitters, (bytes, str))
        for idx, (splitter, rightsplit) in enumerate(splitters):
            if rightsplit:
                func = chunk.rindex
            else:
                func = chunk.index
            try:
                pos = func(splitter, start)
            except ValueError:  # `ValueError: subsection not found`
                continue

            return chunk[:pos], chunk[pos + len(splitter):], idx

        return chunk, None, None

    def _read_more(self, current_chunk):
        if not self._unread:
            return False, current_chunk
        extra_data = self._unread.pop(0)
        if current_chunk is not None:
            # max_chunk_size check
            l1 = len(current_chunk)
            l2 = len(extra_data)
            mcs = self.max_chunk_size
            if l1 <= mcs and l2 <= mcs and l1 + l2 > mcs:
                # Sample case: looking for metadata end, reading until it is
                # found, but it is not found and there's a lot of data.
                raise self.RowTooLarge(
                    "Tried to read more data than `max_chunk_size`",
                    dict(l1=l1, l2=l2, mcs=mcs))

            # Same as below, but only done when it is likely to help
            # (i.e. on large rows).
            if (l1 > self.min_chunk_size * 5 and not isinstance(current_chunk, bytearray)):
                current_chunk = bytearray(current_chunk)

            current_chunk += extra_data

            return True, current_chunk

        # # This should allow append (above) to happen inplace,
        # # but adds an extra copy.
        # extra_data = bytearray(extra_data)
        return True, extra_data

    def _read_until(self, current_chunk, end_markers):
        assert not isinstance(end_markers, (bytes, str))
        # Semantics note:
        # `right is None` means `not found`,
        # `right == b""` means `endswith`
        left, right, idx = self._splitpair(current_chunk, end_markers)
        max_marker_len = max(len(marker[0]) for marker in end_markers)
        while right is None:
            chunk_prev_len = len(current_chunk)
            # raises ValueError:
            more, current_chunk = self._read_more(current_chunk)
            if not more:
                break
            # Try again.
            start_pos = chunk_prev_len - max_marker_len
            if start_pos < 0:
                start_pos = 0
            left, right, idx = self._splitpair(
                current_chunk,
                end_markers,
                start=start_pos,
            )
        if right is not None:
            return left, right, idx
        return None, current_chunk, None

    def _put_back(self, chunk):
        if not chunk:
            return
        self._unread.insert(0, chunk)

    def _add_result(self, part, item):
        if self._autoparse:
            item = json.loads(item)
        self._results.append((part, item))

    def next_event(self):
        """ ... """
        if self.state == self.STATE_FINISHED:  # terminating state
            return self.parts.FINISHED, None
        if self.state == self.STATE_ERROR:  # terminating state
            raise ParseError(
                "next event requested in error-state",
                "STATE_ERROR", b"")

        if self._results:
            return self._results.pop(0)

        more, chunk = self._read_more(None)
        if not chunk:
            return self.parts.NEED_DATA, None

        while len(chunk) < self.min_chunk_size:
            more, chunk = self._read_more(chunk)
            if not more:
                break

        if self.state == self.STATE_START:
            prefix = b'{\n\t"meta":\n\t[\n'
            chunk = chunk.lstrip()  # allow extra whitespaces at start.

            if len(chunk) < len(prefix):
                self._put_back(chunk)
                return self.parts.NEED_DATA, None

            if not chunk.startswith(prefix):
                self.state = self.STATE_ERROR
                raise ParseError("unexpected data start", "STATE_START", chunk)

            chunk = chunk[len(prefix):]
            self.state = self.STATE_META

        if self.state == self.STATE_META:
            # (implicit "[") + data + (implicit "]")
            # maybe: `meta_end = b"\n\t\t}\n\t],\n\n"`
            meta_end = b"\n\t],\n"
            meta_chunk, chunk, _ = self._read_until(
                chunk,
                (
                    (meta_end, False),
                ))
            if meta_chunk is not None:
                self.state = self.STATE_NEXT_BLOCK
                self._add_result(self.parts.META, b"[" + meta_chunk + b"]")

        if self.state == self.STATE_NEXT_BLOCK:
            next_blocks = (
                (b'\n\t"data":\n\t[\n', self.STATE_DATA),  # data_start
                # Footer examples:
                # '\n\n\t"totals": ['
                # '\n\n\t"extremes":\n\t{'
                # '\n\n\t"rows": '
                (b'\n\t"', self.STATE_FOOTER),
            )
            for prefix, next_state in next_blocks:
                left, right, _ = self._splitpair(chunk, ((prefix, False),))
                if right is not None:
                    if left.strip():
                        self.state = self.STATE_ERROR
                        raise ParseError(
                            ("unexpected data between blocks "
                             "(to state {})").format(next_state),
                            "STATE_NEXT_BLOCK",
                            chunk)
                    chunk = right
                    self.state = next_state
                    break
            # `else:`
            if self.state == self.STATE_NEXT_BLOCK:
                if len(chunk) > self.min_chunk_size:
                    self.state = self.STATE_ERROR
                    raise ParseError(
                        "could not find the next block",
                        "STATE_NEXT_BLOCK", chunk)
                # Otherwise perhaps the chunk was too small.

        if self.state == self.STATE_DATA:
            # Up to multiple b"...\n\t\t[...],\n..." chunks
            data_end = b"\n\t],\n"
            data_line_end = b"],\n"
            data_chunk, chunk, match_idx = self._read_until(
                chunk,
                # Tricky ordering: most of the time this will result in going
                # over the data twice,
                # but it has to be done because `data_end` can be found after
                # the data block.
                (
                    (data_end, False),
                    (data_line_end, True),
                ))
            if data_chunk is not None:
                # maybe: `and b"[" in data_chunk`
                # maybe: `and data_chunk != b"\n\t"`
                self._add_result(
                    self.parts.DATACHUNK,
                    (b"[" +
                     data_chunk +
                     (b"]]" if match_idx == 1 else b"]")),
                )
            if match_idx == 0:  # `data_end`
                self.state = self.STATE_NEXT_BLOCK

        if self.state == self.STATE_FOOTER:
            suffix = b"\t}\n}"
            stats_chunk, chunk, _ = self._read_until(chunk, ((suffix, False),))
            if stats_chunk is not None:
                self.state = self.STATE_FINISHED
                self._add_result(
                    self.parts.STATS,
                    b'{"' + stats_chunk + suffix)
                chunk = chunk.strip()
                if chunk:
                    # Data in this state is not expected. Check if it's a timeout error and log the error.
                    # Raise ParseError otherwise.
                    db_exc = DatabaseException(chunk.decode())
                    if db_exc.code == '159':  # Timeout exceeded
                        logger.error("Got timeout error message after finished response processing.")
                    else:
                        self.state = self.STATE_ERROR
                        raise ParseError(
                            "Extra data after the end",
                            "STATE_STATS", chunk)

        if chunk:
            self._put_back(chunk)

        if self._results:
            return self._results.pop(0)
        return self.parts.NEED_DATA, None

    @classmethod
    def as_generator(cls, data_chunks, **kwargs):
        parser = cls(**kwargs)
        event = None
        chunk = None
        for chunk in data_chunks:
            parser.receive_data(chunk)
            event, data = parser.next_event()
            while (event != parser.parts.NEED_DATA and
                   event != parser.parts.FINISHED):
                yield event, data
                event, data = parser.next_event()
        if event is not None and event != parser.parts.FINISHED:
            raise ValueError(
                "Unexpected last event",
                dict(event=event, data=data, chunk=chunk))
