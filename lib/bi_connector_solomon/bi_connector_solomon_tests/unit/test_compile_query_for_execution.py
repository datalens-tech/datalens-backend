from typing import Type

import sqlalchemy as sa
from sqlalchemy.engine.default import DefaultDialect

from dl_core.connection_executors.adapters.common_base import (
    _DBA_TV,
    _TARGET_DTO_TV,
    CommonBaseDirectAdapter,
)
from dl_core.connection_executors.models.scoped_rci import DBAdapterScopedRCI
from dl_core.utils import sa_plain_text

from bi_connector_solomon.core.constants import CONNECTION_TYPE_SOLOMON


class TestBaseDirectAdapter(CommonBaseDirectAdapter):
    conn_type = CONNECTION_TYPE_SOLOMON

    @classmethod
    def create(
        cls: Type[_DBA_TV], target_dto: _TARGET_DTO_TV, req_ctx_info: DBAdapterScopedRCI, default_chunk_size: int
    ) -> _DBA_TV:
        pass


def get_dialect() -> DefaultDialect:
    engine = sa.create_engine("bi_solomon://", strategy="mock", executor=lambda *_, **__: None)
    engine = engine.execution_options(compiled_cache=None)
    return engine.dialect


def test_compile_query():
    adapter = TestBaseDirectAdapter()
    query = sa_plain_text('alias(constant_line(100), "100%")')
    compiled_query = adapter.compile_query_for_execution(query, dialect=get_dialect())
    assert compiled_query == 'alias(constant_line(100), "100%")'
