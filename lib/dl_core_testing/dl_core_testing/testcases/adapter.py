import abc
from typing import (
    ClassVar,
    Generic,
    Type,
    TypeVar,
)

import pytest

from dl_api_commons.base_models import RequestContextInfo
from dl_core import exc
from dl_core.connection_executors import AsyncConnExecutorBase
from dl_core.connection_executors.adapters.async_adapters_base import AsyncDirectDBAdapter
from dl_core.connection_executors.models.connection_target_dto_base import BaseSQLConnTargetDTO
from dl_core.connection_executors.models.db_adapter_data import DBAdapterQuery
from dl_core.connection_executors.models.scoped_rci import DBAdapterScopedRCI
from dl_core_testing.testcases.connection_executor import BaseConnectionExecutorTestClass


_TARGET_DTO_TV = TypeVar("_TARGET_DTO_TV", bound=BaseSQLConnTargetDTO)


class BaseAsyncAdapterTestClass(BaseConnectionExecutorTestClass, Generic[_TARGET_DTO_TV], metaclass=abc.ABCMeta):
    """
    Most of the testable adapter behaviour is covered by conn executor tests
    Here are the tests, that are just easier to implement at the adapter level
    """

    ASYNC_ADAPTER_CLS: ClassVar[Type[AsyncDirectDBAdapter]]  # TODO add tests for other adapters

    @pytest.fixture
    async def target_conn_dto(self, async_connection_executor: AsyncConnExecutorBase) -> _TARGET_DTO_TV:
        return next(iter(await async_connection_executor._make_target_conn_dto_pool()))  # noqa

    def _make_dba(self, target_dto: _TARGET_DTO_TV, rci: RequestContextInfo) -> AsyncDirectDBAdapter:
        return self.ASYNC_ADAPTER_CLS.create(
            req_ctx_info=DBAdapterScopedRCI.from_full_rci(rci),
            target_dto=target_dto,
            default_chunk_size=10,
        )

    test_debug_query_with_default_value: ClassVar[bool] = False

    @pytest.mark.parametrize(
        "pass_db_query_to_user, expected_query",
        (
            (None, None),
            (False, None),
            (True, "select 1 from <hidden>"),
        ),
    )
    async def test_pass_db_query_to_user(
        self,
        pass_db_query_to_user: bool,
        expected_query: str,
        conn_bi_context: RequestContextInfo,
        target_conn_dto: _TARGET_DTO_TV,
    ) -> None:
        if pass_db_query_to_user is None and not self.test_debug_query_with_default_value:
            pytest.skip(f"Skipping the test with no value override ({self.test_debug_query_with_default_value=})")

        query_to_send = "select 1 from very_secret_table"
        debug_query = "select 1 from <hidden>"

        target_conn_dto = target_conn_dto.clone(
            port="65535",  # not the actual db port to raise connect error
            pass_db_query_to_user=pass_db_query_to_user,
        )

        dba = self._make_dba(target_conn_dto, conn_bi_context)

        with pytest.raises(exc.SourceConnectError) as exception_info:
            await dba.execute(DBAdapterQuery(query=query_to_send, debug_compiled_query=debug_query))

        assert exception_info.value.query == expected_query, exception_info.value.query

    async def test_timeout(
        self,
        conn_bi_context: RequestContextInfo,
        target_conn_dto: _TARGET_DTO_TV,
    ) -> None:
        target_conn_dto = target_conn_dto.clone(port="65535")
        dba = self._make_dba(target_conn_dto, conn_bi_context)

        with pytest.raises(exc.SourceConnectError):
            await dba.execute(DBAdapterQuery(query="select 1"))
