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
    TypedQueryRaw,
    TypedQueryRawParameters,
    TypedQueryRawResult,
)


_CONN_TV = TypeVar("_CONN_TV", bound=ConnectionBase)


class TypedQueryRawChecker(abc.ABC):
    @abc.abstractmethod
    def get_typed_query_raw(self) -> TypedQueryRaw:
        raise NotImplementedError

    @abc.abstractmethod
    def check_typed_query_raw_result(self, typed_query_raw_result: TypedQueryRawResult) -> None:
        raise NotImplementedError


class DefaultTypedQueryRawChecker(TypedQueryRawChecker):
    def get_typed_query_raw(self) -> TypedQueryRaw:
        return TypedQueryRaw(
            query_type=DashSQLQueryType.raw_query, parameters=TypedQueryRawParameters(path="/json", method="GET")
        )

    def check_typed_query_raw_result(self, typed_query_raw_result: TypedQueryRawResult) -> None:
        assert typed_query_raw_result.data.status == 200
        assert typed_query_raw_result.data.headers
        assert typed_query_raw_result.data.body


class DefaultTypedQueryRawTestSuite(BaseConnectionExecutorTestClass[_CONN_TV], Generic[_CONN_TV]):
    @pytest.fixture(scope="class")
    def typed_query_raw_checker(self) -> TypedQueryRawChecker:
        return DefaultTypedQueryRawChecker()

    async def test_typed_query_raw(
        self,
        async_connection_executor: AsyncConnExecutorBase,
        typed_query_raw_checker: TypedQueryRawChecker,
    ) -> None:
        typed_query_raw = typed_query_raw_checker.get_typed_query_raw()
        typed_query_raw_result = await async_connection_executor.execute_typed_query_raw(
            typed_query_raw=typed_query_raw
        )
        typed_query_raw_checker.check_typed_query_raw_result(typed_query_raw_result=typed_query_raw_result)
