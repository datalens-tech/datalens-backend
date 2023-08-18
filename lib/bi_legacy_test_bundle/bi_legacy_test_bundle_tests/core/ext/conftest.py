from __future__ import annotations

import os

import pytest

from bi_testing.env_params.generic import GenericEnvParamGetter
from bi_testing.files import get_file_loader
from bi_testing.tvm_info import TvmSecretReader

from bi_blackbox_client.testing import update_global_tvm_info
from bi_core_testing.environment import common_pytest_configure, prepare_united_storage_from_config

import bi_legacy_test_bundle_tests.core.config as tests_config_mod
from bi_legacy_test_bundle_tests.core.conftest import (  # noqa: F401
    make_sync_rqe_netloc_subprocess, clear_logging_context, loaded_libraries
)


def pytest_configure(config):  # noqa
    common_pytest_configure()
    prepare_united_storage_from_config(us_config=tests_config_mod.CORE_TEST_CONFIG.get_us_config())


@pytest.fixture(scope='session')
def env_param_getter() -> GenericEnvParamGetter:
    filepath = os.path.join(os.path.dirname(__file__), 'params.yml')
    filepath = get_file_loader().resolve_path(filepath)
    return GenericEnvParamGetter.from_yaml_file(filepath)


@pytest.fixture(scope='session')
def tvm_secret_reader(env_param_getter) -> TvmSecretReader:
    return TvmSecretReader(env_param_getter)


@pytest.fixture(scope='session')
def yt_token(env_param_getter: GenericEnvParamGetter) -> str:
    return env_param_getter.get_str_value('YT_OAUTH')


@pytest.fixture(scope='session', autouse=True)
def updated_global_tvm_info(tvm_secret_reader):
    return update_global_tvm_info(tvm_secret_reader.get_tvm_info())


@pytest.fixture(scope='session')
def tvm_info(updated_global_tvm_info, tvm_secret_reader):
    return updated_global_tvm_info or tvm_secret_reader.get_tvm_info()


@pytest.fixture(scope='session')
def sync_rqe_netloc_subprocess_exttests(tests_config, tvm_info):
    assert tvm_info
    with make_sync_rqe_netloc_subprocess(tests_config) as result:
        yield result


@pytest.fixture(scope='function')
def ext_sync_usm(default_sync_usm, sync_rqe_netloc_subprocess_exttests):
    """ USM with RQE that was started with ext tests' secrets """
    sync_rqe_netloc = sync_rqe_netloc_subprocess_exttests
    usm = default_sync_usm
    sr = usm._services_registry
    cef = sr._conn_exec_factory
    rqe_config = cef.rqe_config
    rqe_config = rqe_config.clone(
        ext_sync_rqe=sync_rqe_netloc,
        int_sync_rqe=sync_rqe_netloc,
    )
    cef = cef.clone(rqe_config=rqe_config)
    sr = sr.clone(conn_exec_factory=cef)
    usm = usm.clone(services_registry=sr)
    return usm
