from __future__ import annotations

import pytest

from bi_api_lib_tests.utils import get_random_str


@pytest.fixture(scope='function')
def solomon_connection_params():
    return dict(
        type='solomon',
        dir_path='tests/connections',
        name='solomon_{}'.format(get_random_str()),
        host='solomon.yandex.net',
    )
