from __future__ import absolute_import

import itertools
import logging
from typing import (
    NamedTuple,
    Optional,
    Tuple,
)

LOGGER = logging.getLogger(__name__)


class DescriptionColumn(NamedTuple):
    # https://www.python.org/dev/peps/pep-0249/#description
    name: str
    type_code: str
    display_size: Optional[int] = None
    internal_size: Optional[int] = None
    precision: Optional[int] = None
    scale: Optional[int] = None
    null_ok: Optional[bool] = None


class Cursor:
    def __init__(self, connection):
        self.connection = connection
        self.description: Tuple[DescriptionColumn, ...] = ()
        self.arraysize = 1
        self.logger = LOGGER
        self.rows = None
        self._rows_prefetched = None

    def execute(self, query, parameters=None):
        self.logger.debug("execute promql: %s", query)
        chunks = self.connection.cli.query_range(
            query,
            parameters,
        )
        self.description = ()
        rows = self._rows_iterable(chunks)

        # Prefetch the description:
        try:
            first_row = next(rows)
        except StopIteration:
            pass
        else:
            rows = itertools.chain((first_row,), rows)

        if self.rows is not None:
            rows = itertools.chain(self.rows, rows)

        self.rows = rows

    def _rows_iterable(self, chunks_iterable):
        nmd = None
        description = None
        for chunk in chunks_iterable:
            schema, rows = chunk
            if description is None and rows:
                description = tuple(DescriptionColumn(name, type_name) for name, type_name in schema)
                self.description = description
            if nmd is None:
                assert description is not None
                colnames = tuple(col[0] for col in description)
                nmd = lambda row, colnames=colnames: tuple(row[colname] for colname in colnames)
            for row in rows:
                yield nmd(row)

    def _ensure_prefetched(self):
        if self.rows is not None and self._rows_prefetched is None:
            self._rows_prefetched = list(self.rows)
            self.rows = iter(self._rows_prefetched)
        return self._rows_prefetched

    def executemany(self, query, seq_of_parameters):
        for parameters in seq_of_parameters:
            self.execute(query, parameters)

    def fetchone(self):
        rows = self.rows
        if rows is None:
            return None
        try:
            return next(rows)
        except StopIteration:
            return None

    def fetchmany(self, size=None):
        if size is None:
            size = self.arraysize

        rows = self.rows
        if rows is None:
            return []
        return list(itertools.islice(rows, size))

    def fetchall(self):
        rows = self.rows
        if rows is None:
            return []
        return list(rows)

    def setinputsizes(self, sizes):
        pass

    def setoutputsize(self, column=None):
        pass

    def close(self):
        self.rows = None
        self._rows_prefetched = None

    @property
    def rowcount(self):
        return len(self._ensure_prefetched())
