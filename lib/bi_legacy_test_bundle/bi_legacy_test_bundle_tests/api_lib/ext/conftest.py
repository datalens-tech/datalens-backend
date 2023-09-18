from __future__ import annotations

import json
import os

import pytest

from dl_core.connection_executors.adapters.async_adapters_remote import RemoteAsyncAdapter
from dl_core_testing.environment import common_pytest_configure, prepare_united_storage_from_config

from dl_testing.env_params.generic import GenericEnvParamGetter
from bi_testing_ya.tvm_info import TvmSecretReader

import bi_legacy_test_bundle_tests.api_lib.config as tests_config_mod
from bi_legacy_test_bundle_tests.api_lib.conftest import loaded_libraries  # noqa
from bi_legacy_test_bundle_tests.api_lib.utils import get_random_str


def pytest_configure(config):  # noqa
    bi_test_config = tests_config_mod.BI_TEST_CONFIG
    common_pytest_configure()  # make sure the CH dialect is available.
    prepare_united_storage_from_config(us_config=bi_test_config.core_test_config.get_us_config())


@pytest.fixture(scope='session')
def env_param_getter() -> GenericEnvParamGetter:
    filepath = os.path.join(os.path.dirname(__file__), 'params.yml')
    return GenericEnvParamGetter.from_yaml_file(filepath)


@pytest.fixture(scope='session')
def tvm_secret_reader(env_param_getter) -> TvmSecretReader:
    return TvmSecretReader(env_param_getter)


@pytest.fixture(scope='session')
def yt_token(env_param_getter):
    return env_param_getter.get_str_value('YT_OAUTH')


@pytest.fixture(scope='session')
def int_cookie(env_param_getter):
    import requests

    password = env_param_getter.get_str_value('ROBOT_STATBOX_KAPPA_PASSWORD')

    url = 'https://passport.yandex-team.ru/auth'
    payload = f'login=robot-statbox-kappa&passwd={password}'
    headers = {
        'Content-Type': 'application/x-www-form-urlencoded',
    }

    with requests.Session() as s:
        s.request('POST', url, headers=headers, data=payload)
        cookies = s.cookies.get_dict()

    return '; '.join(f'{k}={v}' for k, v in cookies.items())


@pytest.fixture
def tvm_info(tvm_secret_reader):
    tvm_info_str = tvm_secret_reader.get_tvm_info_str()
    os.environ['TVM_INFO'] = tvm_info_str
    return tvm_info_str


@pytest.fixture
def public_ch_over_yt_connection_params(app, yt_token):
    return {
        'name': 'chyt_test_{}'.format(get_random_str()),
        'type': 'ch_over_yt',
        'token': yt_token,
        'cluster': 'hahn',
        'alias': '*chyt_datalens_back',
        'raw_sql_level': 'dashsql',
    }


@pytest.fixture
def public_ch_over_yt_user_auth_connection_params(app):
    return {
        'name': 'chyt_test_{}'.format(get_random_str()),
        'type': 'ch_over_yt_user_auth',
        'cluster': 'hahn',
        'alias': '*chyt_datalens_back',
        'raw_sql_level': 'dashsql',
    }


@pytest.fixture
def public_ch_over_yt_user_auth_headers(app, yt_token):
    return {
        'Authorization': 'OAuth {}'.format(yt_token),
    }


@pytest.fixture
def solomon_connection_params(app):
    return dict(
        type='solomon',
        dir_path='tests/connections',
        name='solomon_{}'.format(get_random_str()),
        host='solomon.yandex.net',
    )


def _get_public_ch_over_yt_connection_id(app, client, connection_params, request):
    resp = client.post(
        '/api/v1/connections',
        data=json.dumps(connection_params),
        content_type='application/json'
    )
    assert resp.status_code == 200, resp.json
    conn_id = resp.json['id']

    def teardown():
        client.delete('/api/v1/connections/{}'.format(conn_id))

    request.addfinalizer(teardown)

    return conn_id


@pytest.fixture
def public_ch_over_yt_connection_id(app, client, public_ch_over_yt_connection_params, request):
    return _get_public_ch_over_yt_connection_id(app, client, public_ch_over_yt_connection_params, request)


@pytest.fixture
def public_ch_over_yt_user_auth_connection_id(app, client, public_ch_over_yt_user_auth_connection_params, request):
    return _get_public_ch_over_yt_connection_id(app, client, public_ch_over_yt_user_auth_connection_params, request)


def _get_public_ch_over_yt_second_connection_id(app, client, connection_params, request):
    params = dict(
        connection_params,
        name='chyt_test_the_second_{}'.format(get_random_str()),
        # # To reconsider:
        # alias='*ch_datalens',
        # token (some second token)
    )
    resp = client.post(
        '/api/v1/connections',
        data=json.dumps(params),
        content_type='application/json'
    )
    assert resp.status_code == 200, resp.json
    conn_id = resp.json['id']

    def teardown():
        client.delete('/api/v1/connections/{}'.format(conn_id))

    request.addfinalizer(teardown)

    return conn_id


@pytest.fixture(scope='function')
def solomon_subselectable_connection_id(app, client, request, solomon_connection_params):
    resp = client.post(
        '/api/v1/connections',
        data=json.dumps(solomon_connection_params),
        content_type='application/json'
    )

    assert resp.status_code == 200, resp.json
    conn_id = resp.json['id']

    def teardown():
        client.delete('/api/v1/connections/{}'.format(conn_id))

    request.addfinalizer(teardown)

    return conn_id


@pytest.fixture
def public_ch_over_yt_second_connection_id(app, client, public_ch_over_yt_connection_params, request):
    return _get_public_ch_over_yt_second_connection_id(app, client, public_ch_over_yt_connection_params, request)


@pytest.fixture
def public_ch_over_yt_user_auth_second_connection_id(
    app, client, public_ch_over_yt_user_auth_connection_params, request
):
    return _get_public_ch_over_yt_second_connection_id(
        app, client, public_ch_over_yt_user_auth_connection_params, request)


@pytest.fixture
def api_v1_with_token(api_v1, public_ch_over_yt_user_auth_headers):
    api_v1.headers = public_ch_over_yt_user_auth_headers
    return api_v1


@pytest.fixture
def remote_async_adapter_mock(monkeypatch) -> None:
    # TODO consider: use a special tests-services_registry that returns a mock
    # object (subclass) instead of the normal RemoteAsyncAdapter.

    async def test_mock(self) -> None:
        pass

    monkeypatch.setattr(RemoteAsyncAdapter, 'test', test_mock)
