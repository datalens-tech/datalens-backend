from typing import Type

from bi_constants.enums import ConnectionType
from bi_core.connection_executors.adapters.common_base import CommonBaseDirectAdapter, _TARGET_DTO_TV, _DBA_TV
from bi_core.connection_executors.models.scoped_rci import DBAdapterScopedRCI
from bi_core.utils import sa_plain_text


class TestBaseDirectAdapter(CommonBaseDirectAdapter):

    conn_type = ConnectionType.solomon

    @classmethod
    def create(
            cls: Type[_DBA_TV],
            target_dto: _TARGET_DTO_TV,
            req_ctx_info: DBAdapterScopedRCI,
            default_chunk_size: int
    ) -> _DBA_TV:
        pass


def test_compile_query():
    adapter = TestBaseDirectAdapter()
    query = sa_plain_text('alias(constant_line(100), "100%")')
    compiled_query = adapter.compile_query_for_execution(query)
    assert compiled_query == 'alias(constant_line(100), "100%")'

