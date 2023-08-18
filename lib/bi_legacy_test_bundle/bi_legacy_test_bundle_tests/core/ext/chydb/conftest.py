from __future__ import annotations

import pytest

from bi_legacy_test_bundle_tests.core.fixtures_ce import (  # noqa: F401
    query_executor_app, query_executor_options, sync_remote_query_executor,
)


@pytest.fixture(scope='session')
def chydb_test_connection_params_base(env_param_getter):
    return dict(
        token=env_param_getter.get_str_value('YDB_OAUTH'),
        host='ydb-clickhouse.yandex.net',
        port=8123,
    )
