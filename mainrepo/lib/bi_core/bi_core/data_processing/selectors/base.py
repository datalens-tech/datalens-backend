from __future__ import annotations

import abc
from typing import (
    TYPE_CHECKING,
    List,
    Optional,
    Sequence,
)

import attr

if TYPE_CHECKING:
    from sqlalchemy.sql.selectable import Select

    from bi_constants.enums import (
        BIType,
        DataSourceRole,
    )
    from bi_core.data_processing.cache.primitives import BIQueryCacheOptions
    from bi_core.data_processing.prepared_components.primitives import PreparedMultiFromInfo
    from bi_core.data_processing.stream_base import DataStreamAsync
    from bi_core.query.bi_query import QueryAndResultInfo
    from bi_core.us_connection_base import ExecutorBasedMixin


class NoData(Exception):
    pass


@attr.s(frozen=True, auto_attribs=True)
class BIQueryExecutionContext:
    query_id: str
    query: Select
    compiled_query: str  # for logs only
    target_connection: ExecutorBasedMixin
    requested_bi_types: List[BIType]
    result_col_names: Sequence[str]
    target_db_name: Optional[str] = attr.ib(default=None)
    cache_options: Optional[BIQueryCacheOptions] = attr.ib(default=None)
    connect_args: dict = attr.ib(default=None)


class DataSelectorAsyncBase(abc.ABC):
    @abc.abstractmethod
    async def get_data_stream(
        self,
        *,
        query_id: Optional[str] = None,
        role: DataSourceRole,
        query_res_info: QueryAndResultInfo,
        joint_dsrc_info: PreparedMultiFromInfo,
        row_count_hard_limit: Optional[int] = None,
        stream_id: Optional[str] = None,
    ) -> DataStreamAsync:
        """
        Fetch data from the database.
        Return SQL query compiled as a string and an iterable or result rows.

        :param role: data source role to use
        :param query_res_info: an object containing a compiled SA ``Select`` and info about result columns
        :param joint_dsrc_info: represents the combined SQL data source (joined tables)
        :param row_count_hard_limit: Result rows count limit. Will raise ResultRowCountLimitExceeded if receive more.
        :return: ``DataStreamAsync`` instance containing an async iterable of lists
            and some metadata such as compiled query, column names and data types
        """

        raise NotImplementedError

    async def close(self) -> None:
        pass
