import abc
from typing import (
    Generic,
    TypeVar,
)

import pytest

from dl_constants.enums import DashSQLQueryType
from dl_core.connection_executors import AsyncConnExecutorBase
from dl_core.us_connection_base import ConnectionBase
from dl_core_testing.testcases.connection_executor import BaseConnectionExecutorTestClass
from dl_dashsql.typed_query.primitives import (
    DataRowsTypedQueryResult,
    PlainTypedQuery,
    TypedQuery,
    TypedQueryResult,
)


_CONN_TV = TypeVar("_CONN_TV", bound=ConnectionBase)


class TypedQueryChecker(abc.ABC):
    @abc.abstractmethod
    def get_typed_query(self) -> TypedQuery:
        raise NotImplementedError

    @abc.abstractmethod
    def check_typed_query_result(self, typed_query_result: TypedQueryResult) -> None:
        raise NotImplementedError


class DefaultTypedQueryChecker(TypedQueryChecker):
    def get_typed_query(self) -> TypedQuery:
        return PlainTypedQuery(
            query_type=DashSQLQueryType.generic_query,
            query="select 1 as q, 2 as w, 'zxc' as e",
            parameters=(),
        )

    def check_typed_query_result(self, typed_query_result: TypedQueryResult) -> None:
        assert isinstance(typed_query_result, DataRowsTypedQueryResult)
        assert typed_query_result.data_rows[0] == (1, 2, "zxc")


class DefaultTypedQueryTestSuite(BaseConnectionExecutorTestClass[_CONN_TV], Generic[_CONN_TV]):
    @pytest.fixture(scope="class")
    def typed_query_checker(self) -> TypedQueryChecker:
        return DefaultTypedQueryChecker()

    async def test_typed_query(
        self,
        async_connection_executor: AsyncConnExecutorBase,
        typed_query_checker: TypedQueryChecker,
    ) -> None:
        typed_query = typed_query_checker.get_typed_query()
        typed_query_result = await async_connection_executor.execute_typed_query(typed_query=typed_query)
        typed_query_checker.check_typed_query_result(typed_query_result=typed_query_result)
