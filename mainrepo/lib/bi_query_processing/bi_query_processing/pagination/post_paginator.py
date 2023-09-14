from typing import (
    Generator,
    Iterable,
    Iterator,
)

from bi_query_processing.merging.primitives import (
    MergedQueryDataRow,
    MergedQueryDataStream,
)


class QueryPostPaginator:
    """
    Implements offset/limit pagination for `MergedQueryDataStream`
    """

    def _skip_to_offset(
        self,
        row_it: Iterator[MergedQueryDataRow],
        offset: int,
    ) -> None:
        if offset <= 0:
            return
        try:
            while offset > 0:
                next(row_it)
                offset -= 1
        except StopIteration:
            pass

    def _iter_until_limit(
        self, row_it: Iterator[MergedQueryDataRow], limit: int
    ) -> Generator[MergedQueryDataRow, None, None]:
        try:
            while limit > 0:
                yield next(row_it)
                limit -= 1
        except StopIteration:
            pass

    def post_paginate(self, merged_stream: MergedQueryDataStream) -> MergedQueryDataStream:
        row_it = iter(merged_stream.rows)
        overridden = False

        # Apply offset
        if merged_stream.meta.offset is not None:
            self._skip_to_offset(row_it, offset=merged_stream.meta.offset)
            overridden = True

        # Apply limit
        rows: Iterable[MergedQueryDataRow] = row_it
        if merged_stream.meta.limit is not None:
            rows = self._iter_until_limit(row_it, limit=merged_stream.meta.limit)
            overridden = True

        if overridden:
            merged_stream = merged_stream.clone(rows=rows)
        return merged_stream
