import pytest

from dl_api_lib_testing.connector.typed_query_suite import DefaultDashSQLTypedQueryTestSuite
from dl_api_lib_testing.typed_query_base import (
    TypedQueryInfo,
    TypedQueryParam,
)
from dl_api_lib_testing.api_base import DefaultApiTestBase
from dl_constants.enums import (
    DashSQLQueryType,
    RawSQLLevel,
    UserDataType,
)


class TestDashSQLTypedQuery(DefaultApiTestBase, DefaultDashSQLTypedQueryTestSuite):
    raw_sql_level = RawSQLLevel.dashsql


class TestDashSQLTypedQueryWithParameters(DefaultApiTestBase, DefaultDashSQLTypedQueryTestSuite):
    raw_sql_level = RawSQLLevel.dashsql
    data_caches_enabled = True

    @pytest.fixture(scope="class")
    def typed_query_info(self) -> TypedQueryInfo:
        return TypedQueryInfo(
            query_type=DashSQLQueryType.generic_query,
            query_content={"query": "select 1 as one, {{my_param_1}} as two, {{my_param_2}} as three"},
            params=[
                TypedQueryParam(
                    name="my_param_2",
                    user_type=UserDataType.string,
                    value="lorem ipsum",
                ),
                TypedQueryParam(
                    name="my_param_1",
                    user_type=UserDataType.integer,
                    value=8,
                ),
            ],
        )

    def check_result(self, result_data: dict) -> None:
        assert result_data["data"]["rows"][0] == [1, 8, "lorem ipsum"]
        headers = result_data["data"]["headers"]
        assert len(headers) == 3
        assert [header["name"] for header in headers] == ["one", "two", "three"]
        assert [header["data_type"] for header in headers] == [
            UserDataType.integer.name,
            UserDataType.integer.name,
            UserDataType.string.name,
        ]
