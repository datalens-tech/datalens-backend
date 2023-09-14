from __future__ import annotations

import abc

import attr

from bi_core.query.bi_query import BIQuery
from bi_query_processing.execution.exec_info import QueryExecutionInfo
from bi_query_processing.execution.primitives import ExecutedQuery
from bi_query_processing.translation.primitives import TranslatedFlatQuery


@attr.s
class QueryExecutorBase(abc.ABC):
    def make_bi_query(
        self,
        *,
        translated_flat_query: TranslatedFlatQuery,
        is_top_level: bool = False,
        distinct: bool = False,
        row_count_hard_limit: int,
    ) -> BIQuery:
        limit = translated_flat_query.limit

        # Limit query size if hard limit is set to minimize the amount of downloaded data
        if is_top_level:  # only for queries that will be downloaded
            if limit is None or limit > row_count_hard_limit:
                limit = row_count_hard_limit + 1

        bi_query = BIQuery(
            select_expressions=translated_flat_query.select,
            group_by_expressions=translated_flat_query.group_by,
            order_by_expressions=translated_flat_query.order_by,
            dimension_filters=translated_flat_query.where,
            measure_filters=translated_flat_query.having,
            limit=limit,
            offset=translated_flat_query.offset,
            distinct=distinct if is_top_level else False,
        )

        return bi_query

    @abc.abstractmethod
    async def execute_query(self, exec_info: QueryExecutionInfo) -> ExecutedQuery:
        raise NotImplementedError
