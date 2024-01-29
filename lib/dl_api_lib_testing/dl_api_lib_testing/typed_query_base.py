import abc
from typing import (
    Any,
    Optional,
)

from aiohttp.test_utils import (
    ClientResponse,
    TestClient,
)
import attr

from dl_api_lib_testing.connection_base import ConnectionTestBase
from dl_api_lib_testing.data_api_base import DataApiTestBase
from dl_constants.enums import (
    DashSQLQueryType,
    UserDataType,
)
from dl_constants.types import TBIDataValue


@attr.s(frozen=True)
class TypedQueryParam:
    name: str = attr.ib(kw_only=True)
    user_type: UserDataType = attr.ib(kw_only=True)
    value: TBIDataValue = attr.ib(kw_only=True)


@attr.s(frozen=True)
class TypedQueryInfo:
    params: list[TypedQueryParam] = attr.ib(kw_only=True)
    query_content: dict = attr.ib(kw_only=True)
    query_type: DashSQLQueryType = attr.ib(kw_only=True)


class DashSQLTypedQueryTestBase(DataApiTestBase, ConnectionTestBase, metaclass=abc.ABCMeta):
    async def get_typed_query_response(
        self,
        data_api_aio: TestClient,
        conn_id: str,
        typed_query_info: TypedQueryInfo,
        fail_ok: bool = False,
        headers: Optional[dict[str, str]] = None,
    ) -> ClientResponse:
        request_body: dict[str, Any] = {
            "query_type": typed_query_info.query_type.name,
            "query_content": typed_query_info.query_content,
            "parameters": [
                {
                    "name": param.name,
                    "data_type": param.user_type.name,
                    "value": param.value,
                }
                for param in typed_query_info.params
            ],
        }

        resp = await data_api_aio.post(
            f"api/data/v1/connections/{conn_id}/typed_query",
            json=request_body,
            headers=headers,
        )
        resp_data = await resp.json()
        if not fail_ok:
            assert resp.status == 200, resp_data
        return resp
