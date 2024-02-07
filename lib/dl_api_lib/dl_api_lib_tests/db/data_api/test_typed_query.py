import pytest

from dl_api_lib_testing.connector.typed_query_suite import DefaultDashSQLTypedQueryTestSuite
from dl_api_lib_testing.typed_query_base import (
    TypedQueryInfo,
    TypedQueryParam,
)
from dl_api_lib_tests.db.base import DefaultApiTestBase
from dl_constants.enums import (
    DashSQLQueryType,
    RawSQLLevel,
    UserDataType,
)


class TestDashSQLTypedQuery(DefaultApiTestBase, DefaultDashSQLTypedQueryTestSuite):
    raw_sql_level = RawSQLLevel.dashsql


class TestDashSQLTypedQueryWithParameters(DefaultApiTestBase, DefaultDashSQLTypedQueryTestSuite):
    raw_sql_level = RawSQLLevel.dashsql

    @pytest.fixture(scope="class")
    def typed_query_info(self) -> TypedQueryInfo:
        return TypedQueryInfo(
            query_type=DashSQLQueryType.generic_query,
            query_content={"query": "select 1,{{my_param}}, 3"},
            params=[
                TypedQueryParam(
                    name="my_param",
                    user_type=UserDataType.integer,
                    value=8,
                ),
            ],
        )

    def check_result(self, result_data: dict) -> None:
        print(result_data)
        assert result_data["data"][0] == [1, 8, 3]
