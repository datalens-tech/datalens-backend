from __future__ import annotations

import json
import os
from typing import Any, Generator, Optional, Callable

import attr
import jaeger_client
import opentracing
import pytest
import yaml

from bi_configs import environments
from bi_configs.environments import IntegrationTestConfig, CommonInstallation, DataCloudExposedInstallationTesting
from bi_testing import shared_testing_constants
from bi_testing.cloud_tokens import AccountCredentials, ServiceAccountAndKeyData
from bi_testing.dlenv import DLEnv
from bi_testing.external_systems_helpers.top import (
    ExternalSystemsHelperBase, ExternalSystemsHelperCloud, ExternalSystemsHelperInternalInstallation,
)
from bi_testing.utils import skip_outside_devhost

from bi_testing_ya.secrets import get_secret


@pytest.fixture(scope='session')
def dl_env(request: Any) -> DLEnv:
    actual_param = request.param if hasattr(request, 'param') else DLEnv.cloud_preprod
    assert isinstance(actual_param, DLEnv), f'Unexpected DLEnv in parameters: {actual_param}'

    dl_env_filter_str = shared_testing_constants.DL_ENV_TESTS_FILTER
    if dl_env_filter_str is not None:
        dl_env_filter = {DLEnv[env_str.strip()] for env_str in dl_env_filter_str.split(",")}

        if actual_param not in dl_env_filter:
            pytest.skip("Env filtered by DL_ENV_TESTS_FILTER")

    return actual_param


# Supplementary resources

def get_cloud_tokens_caches_dir(dl_env: DLEnv) -> Optional[str]:
    caches_dir = shared_testing_constants.TESTS_CACHES_DIR
    if caches_dir and os.path.exists(caches_dir):
        os.makedirs(os.path.join(caches_dir, f"env_{dl_env.name}"), exist_ok=True)
        return caches_dir
    return None


def get_root_certs_file_content() -> bytes:
    cert_file_name = shared_testing_constants.CA_BUNDLE_FILE
    with open(cert_file_name, "rb") as fobj:
        certs = fobj.read()
    return certs


# Secrets

@pytest.fixture(scope='session')
@skip_outside_devhost
def secret_datalens_test_data(dl_env: DLEnv) -> dict[str, Any]:
    if dl_env == DLEnv.dynamic:
        with open('secrets/integration_tests.json') as secrets_file:
            return json.load(secrets_file)
    sec_id = environments.YAVSecretsMap.datalens_test_data.value
    return get_secret(sec_id, use_ssh_auth=True)


# CI-safe secrets

@pytest.fixture(scope='session')
def ci_safe_yav_token() -> Optional[str]:
    """
    YAV token to access CI safe secrets.
    Actually, this token will be available to any user in Arcadia as far as any secret that can be fetched with it.
    """
    return os.environ.get('YAV_TOKEN')


# External systems
EXT_SYS_REQUISITES_MAP = {
    DLEnv.cloud_preprod: environments.InstallationsMap.ext_testing,
    DLEnv.cloud_prod: environments.InstallationsMap.ext_prod,
    DLEnv.internal_preprod: environments.InstallationsMap.int_testing,
    DLEnv.internal_prod: environments.InstallationsMap.int_prod,
    DLEnv.dc_testing: environments.DataCloudExposedInstallationTesting(),
}


@pytest.fixture(scope='session')
def ext_sys_requisites(
        dl_env: DLEnv,
) -> IntegrationTestConfig | CommonInstallation | DataCloudExposedInstallationTesting:
    if dl_env == DLEnv.dynamic:
        return IntegrationTestConfig.from_env_vars(os.environ.items())  # type: ignore  # FIXME
    return EXT_SYS_REQUISITES_MAP[dl_env]  # type: ignore  # FIXME


@pytest.fixture(scope='session')
def ext_sys_helpers_factory(
        dl_env: DLEnv,
        ext_sys_requisites: IntegrationTestConfig | CommonInstallation | DataCloudExposedInstallationTesting,
) -> Callable[[], ExternalSystemsHelperBase]:
    cloud_tokens_caches_dir = get_cloud_tokens_caches_dir(dl_env)
    root_certs_file_content = get_root_certs_file_content()

    def factory() -> ExternalSystemsHelperBase:
        if dl_env.name == DLEnv.dynamic.name:
            assert isinstance(ext_sys_requisites, environments.IntegrationTestConfig)
            return ExternalSystemsHelperCloud(
                ext_sys_requisites=ext_sys_requisites,
                root_certs_file_content=root_certs_file_content,
                caches_dir=cloud_tokens_caches_dir,
            )
        if dl_env.name.startswith("cloud_") or dl_env.name.startswith("dc_"):
            assert isinstance(
                ext_sys_requisites,
                (environments.CommonExternalInstallation, environments.DataCloudExposedInstallation),
            )
            return ExternalSystemsHelperCloud(
                ext_sys_requisites=ext_sys_requisites,
                root_certs_file_content=root_certs_file_content,
                caches_dir=cloud_tokens_caches_dir,
            )
        elif dl_env.name.startswith("internal_"):
            assert isinstance(
                ext_sys_requisites,
                (environments.InternalTestingInstallation, environments.InternalProductionInstallation),
            )
            return ExternalSystemsHelperInternalInstallation(
                ext_sys_requisites=ext_sys_requisites,
                root_certs_file_content=root_certs_file_content,
                caches_dir=None,
            )
        else:
            raise ValueError(f"Unexpected DLEnv: {dl_env}")

    return factory


@pytest.fixture(scope='session')
def ext_sys_helpers_per_session(
        ext_sys_helpers_factory: Callable[[], ExternalSystemsHelperBase],
) -> Generator[ExternalSystemsHelperBase, None, None]:
    with ext_sys_helpers_factory() as ext_helpers:
        yield ext_helpers


@pytest.fixture(scope='function')
def ext_sys_helpers_per_test(
        ext_sys_helpers_factory: Callable[[], ExternalSystemsHelperBase],
) -> Generator[ExternalSystemsHelperBase, None, None]:
    with ext_sys_helpers_factory() as ext_helpers:
        assert isinstance(ext_helpers, ExternalSystemsHelperCloud)
        yield ext_helpers


# Test users

@pytest.fixture(scope='session')
def ext_passport_acct_01_creds(
        dl_env: DLEnv,
        secret_datalens_test_data: dict,
        ext_sys_helpers_per_session: ExternalSystemsHelperBase,
) -> Optional[AccountCredentials]:
    if dl_env in [DLEnv.internal_preprod, DLEnv.internal_prod, DLEnv.dynamic]:
        return None

    assert isinstance(ext_sys_helpers_per_session, ExternalSystemsHelperCloud)
    user_info = yaml.safe_load(secret_datalens_test_data['TEST_USER_01_INFO'])
    account_credentials = ext_sys_helpers_per_session.yc_credentials_converter.get_user_account_credentials(
        user_oauth_token=user_info['oauth_token'],
        cache_key="bi_testing_ya.pytest_plugin.test_user_01",
    )
    account_credentials = attr.evolve(account_credentials, user_name=user_info['username'])

    return account_credentials


@pytest.fixture(scope='session')
def ext_passport_acct_02_creds(
        dl_env: DLEnv,
        secret_datalens_test_data: dict,
        ext_sys_helpers_per_session: ExternalSystemsHelperBase,
) -> Optional[AccountCredentials]:
    if dl_env in [DLEnv.internal_preprod, DLEnv.internal_prod, DLEnv.dynamic]:
        return None

    assert isinstance(ext_sys_helpers_per_session, ExternalSystemsHelperCloud)
    user_info = yaml.safe_load(secret_datalens_test_data['TEST_USER_02_INFO'])
    account_credentials = ext_sys_helpers_per_session.yc_credentials_converter.get_user_account_credentials(
        user_oauth_token=user_info['oauth_token'],
        cache_key="bi_testing_ya.pytest_plugin.test_user_02",
    )
    account_credentials = attr.evolve(account_credentials, user_name=user_info['username'])

    return account_credentials


@pytest.fixture()
def intranet_user_1_creds(dl_env: DLEnv, secret_datalens_test_data: dict) -> Optional[AccountCredentials]:
    if dl_env == DLEnv.dynamic:
        return None
    user_info = yaml.safe_load(secret_datalens_test_data['TEST_INTRANET_USER_01_INFO'])
    account_credentials = AccountCredentials(
        user_id=user_info['user_id'],
        user_name=user_info['user_name'],
        token=user_info['oauth_token'],
        is_intranet_user=True,
    )

    return account_credentials


@pytest.fixture()
def intranet_user_2_creds(dl_env: DLEnv, secret_datalens_test_data: dict) -> Optional[AccountCredentials]:
    if dl_env == DLEnv.dynamic:
        return None
    user_info = yaml.safe_load(secret_datalens_test_data['TEST_INTRANET_USER_02_INFO'])
    account_credentials = AccountCredentials(
        user_id=user_info['user_id'],
        user_name=user_info['user_name'],
        token=user_info['oauth_token'],
        is_intranet_user=True,
    )

    return account_credentials


@pytest.fixture(scope='session')
def integration_tests_admin_sa_data(
        dl_env: DLEnv,
        secret_datalens_test_data: dict,
) -> Optional[ServiceAccountAndKeyData]:
    if dl_env in [DLEnv.internal_preprod, DLEnv.internal_prod, DLEnv.dynamic]:
        return None

    it_data_secret_entry_key = {
        DLEnv.cloud_preprod: "INTEGRATION_TESTS_PREPROD_DATA",
        DLEnv.cloud_prod: "INTEGRATION_TESTS_PROD_DATA",
        DLEnv.dc_testing: "INTEGRATION_TESTS_DC_TESTING_DATA",
        DLEnv.dc_prod: "INTEGRATION_TESTS_DC_PROD_DATA",
    }[dl_env]
    it_sa_key_secret_entry_key = {
        DLEnv.cloud_preprod: "INTEGRATION_TESTS_PREPROD_ADMIN_SA_PRIV_KEY",
        DLEnv.cloud_prod: "INTEGRATION_TESTS_PROD_ADMIN_SA_PRIV_KEY",
        DLEnv.dc_testing: "INTEGRATION_TESTS_DC_TESTING_ADMIN_SA_PRIV_KEY",
        DLEnv.dc_prod: "INTEGRATION_TESTS_DC_PROD_ADMIN_SA_PRIV_KEY",
    }[dl_env]

    integration_tests_data = yaml.safe_load(secret_datalens_test_data[it_data_secret_entry_key])
    admin_sa_priv_key = secret_datalens_test_data[it_sa_key_secret_entry_key]

    return ServiceAccountAndKeyData(
        sa_id=integration_tests_data['admin_sa_id'],
        key_id=integration_tests_data['admin_sa_key_id'],
        key_pem_data=admin_sa_priv_key,
    )


@pytest.fixture(scope='session')
def integration_tests_admin_sa(
        dl_env: DLEnv,
        secret_datalens_test_data: dict,
        ext_sys_helpers_per_session: ExternalSystemsHelperBase,
        integration_tests_admin_sa_data: Optional[ServiceAccountAndKeyData],
) -> Optional[AccountCredentials]:
    if integration_tests_admin_sa_data is None:
        return None

    assert isinstance(ext_sys_helpers_per_session, ExternalSystemsHelperCloud)
    iam_token = ext_sys_helpers_per_session.yc_credentials_converter.get_service_account_iam_token(
        service_account_id=integration_tests_admin_sa_data.sa_id,
        key_id=integration_tests_admin_sa_data.key_id,
        private_key=integration_tests_admin_sa_data.key_pem_data,
    )
    return AccountCredentials(
        user_id=integration_tests_admin_sa_data.sa_id,
        token=iam_token,
    )


@pytest.fixture(scope='session')
def integration_tests_folder_id(
        dl_env: DLEnv,
        secret_datalens_test_data: dict,
) -> str:
    if dl_env in [DLEnv.internal_prod, DLEnv.internal_preprod]:
        # internal installation has single folder
        return "common"

    secret_entry_key = {
        DLEnv.cloud_preprod: "INTEGRATION_TESTS_PREPROD_DATA",
        DLEnv.dynamic: "INTEGRATION_TESTS_DATA",
        DLEnv.cloud_prod: "INTEGRATION_TESTS_PROD_DATA",
        DLEnv.dc_testing: "INTEGRATION_TESTS_DC_TESTING_DATA",
        DLEnv.dc_prod: "INTEGRATION_TESTS_DC_PROD_DATA",
    }[dl_env]

    integration_tests_data = yaml.safe_load(secret_datalens_test_data[secret_entry_key])
    return integration_tests_data['folder_id']


# Tracing

@pytest.fixture(scope='function', autouse=True)
def bi_test_root_span(request: Any) -> Generator[opentracing.Span, None, None]:
    tracer = opentracing.global_tracer()
    if tracer.scope_manager.active is not None:
        tracer.scope_manager.active.close()

    with tracer.start_active_span(request.node.name) as scope:
        yield scope.span


@pytest.fixture(scope='session', autouse=True)
def bi_test_ensure_spans_sent() -> Generator[None, None, None]:
    yield
    tracer = opentracing.global_tracer()
    if isinstance(tracer, jaeger_client.Tracer):
        # TODO FIX: BI-1967 await
        tracer.close()
