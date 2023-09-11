from __future__ import annotations

import asyncio
import logging
import uuid
from typing import Optional, Callable

import attr
import pytest
import yaml
from opentracing import tracer

from bi_api_commons.base_models import RequestContextInfo, TenantDef
from bi_api_commons.tracing import get_current_tracing_headers
from bi_api_commons_ya_cloud.models import TenantYCOrganization
from bi_cloud_integration.iam_rm_client import IAMRMClient
from bi_cloud_integration.model import AccessBindingAction
from bi_defaults.environments import IntegrationTestConfig
from bi_dls_client.dls_client import DLSClient
from bi_integration_tests.account_credentials import TestCredentialsConverter
from bi_integration_tests.report_formatting import ReportFormatter
from bi_integration_tests.request_executors.workbook_mgmt import workbook_mgmt_hooks, NOT_REQUIRED
from bi_testing_ya.cloud_tokens import AccountCredentials
from bi_testing_ya.dlenv import DLEnv

pytest_plugins = (
    'aiohttp.pytest_plugin',
    'bi_testing_ya.pytest_plugin',
)


@attr.s(auto_attribs=True, frozen=True)
class BaseAuthCheckUserConfiguration:
    with_dl_inst_use_role: AccountCredentials
    without_dl_inst_use_role: AccountCredentials


@attr.s(auto_attribs=True, frozen=True)
class BaseTwoUserConfiguration:
    user_1: Optional[AccountCredentials]
    user_2: Optional[AccountCredentials]


@pytest.fixture(scope='session')
def ext_passport_public_api_key(
        dl_env,
        secret_datalens_test_data,
        ext_sys_helpers_per_session,
) -> str:
    secret_entry_key = {
        DLEnv.cloud_preprod: "PREPROD_PUBLIC_MASTER_TOKEN",
        DLEnv.cloud_prod: "PROD_PUBLIC_MASTER_TOKEN",
    }[dl_env]

    public_api_key = yaml.safe_load(secret_datalens_test_data[secret_entry_key])
    return public_api_key


@pytest.fixture(scope='function')
def integration_tests_reporter(
        request
):
    formatter = ReportFormatter()
    with formatter.section(f"TEST REPORT: {request.node.name}"):
        yield formatter


@pytest.fixture(scope='session')
def integration_tests_vault_data(
        dl_env,
        secret_datalens_test_data
):
    secret_entry_key = {
        DLEnv.cloud_preprod: "INTEGRATION_TESTS_PREPROD_DATA",
        DLEnv.cloud_prod: "INTEGRATION_TESTS_PROD_DATA",
    }[dl_env]

    return secret_datalens_test_data[secret_entry_key]


@pytest.fixture(scope='session')
def integration_tests_folder_id(
        dl_env,
        secret_datalens_test_data
):
    if dl_env in [DLEnv.internal_prod, DLEnv.internal_preprod]:
        # internal installation has single folder
        return "common"
    if dl_env == DLEnv.dynamic:
        return secret_datalens_test_data['folder_id']

    secret_entry_key = {
        DLEnv.cloud_preprod: "INTEGRATION_TESTS_PREPROD_DATA",
        DLEnv.cloud_prod: "INTEGRATION_TESTS_PROD_DATA",
    }[dl_env]

    integration_tests_data = yaml.safe_load(secret_datalens_test_data[secret_entry_key])
    return integration_tests_data['folder_id']


@pytest.fixture(scope='session')
def integration_tests_postgres_1(
        dl_env,
        secret_datalens_test_data
):
    pg_data = prepare_postgres_data(dl_env, secret_datalens_test_data)
    pg_data["connection_type"] = "postgres"
    return pg_data


def prepare_postgres_data(
        dl_env,
        secret_datalens_test_data
):
    if dl_env == DLEnv.dynamic:
        return secret_datalens_test_data['postgres']
    secret_entry_key = {
        DLEnv.cloud_preprod: "INTEGRATION_TESTS_PREPROD_PG_1",
        DLEnv.cloud_prod: "INTEGRATION_TESTS_PROD_PG_1",
        DLEnv.internal_preprod: "INTEGRATION_TESTS_INTERNAL_PG_1",
        DLEnv.internal_prod: "INTEGRATION_TESTS_INTERNAL_PG_1",
    }[dl_env]

    return yaml.safe_load(secret_datalens_test_data[secret_entry_key])


@pytest.fixture(scope='function')
def per_test_request_id() -> str:
    req_id = uuid.uuid4().hex
    tracing_headers = get_current_tracing_headers()
    with tracer.start_active_span(f'integration-tests-{req_id}'):
        logging.info(f'Starting test with req_id={req_id}, tracing_headers={tracing_headers}')
        yield req_id


def _assign_datalens_user_role(
        iam_rm_client,
        folder_id,
        cloud_user_id,
        role_id
):
    user_roles = iam_rm_client.list_svc_acct_role_ids_on_folder_sync(
        folder_id=folder_id,
        svc_acct_id=cloud_user_id,
        acct_type='userAccount',
    )

    if role_id not in user_roles:
        iam_rm_client.modify_folder_access_bindings_for_svc_acct_sync(
            svc_acct_id=cloud_user_id,
            acct_type='userAccount',
            folder_id=folder_id,
            role_ids=(role_id,),
            action=AccessBindingAction.ADD,
        )


@pytest.fixture(scope='function')
def iam_rm_client(
        dl_env,
        integration_tests_admin_sa,
        per_test_request_id,
        ext_sys_requisites,
) -> Optional[IAMRMClient]:
    if dl_env in [DLEnv.cloud_prod, DLEnv.cloud_preprod]:
        return IAMRMClient(
            iam_endpoint=ext_sys_requisites.YC_API_ENDPOINT_IAM,
            rm_endpoint=ext_sys_requisites.YC_API_ENDPOINT_RM,
            iam_token=integration_tests_admin_sa.token,
            request_id=per_test_request_id,
        )
    return None


@pytest.fixture(scope='session')
def dynamic_env_test_credentials_converter(
    dl_env,
    ext_sys_helpers_per_session
):
    if dl_env != DLEnv.dynamic:
        return None
    return TestCredentialsConverter(ext_sys_helpers_per_session.yc_credentials_converter)


@pytest.fixture(scope='session')
def dynamic_env_service_accounts(
        dl_env,
        dynamic_env_test_credentials_converter,
        secret_datalens_test_data: dict,
) -> Optional[dict[str, AccountCredentials]]:
    if dl_env != DLEnv.dynamic:
        return None
    short_name_sa_data = secret_datalens_test_data['service_accounts'].items()
    return {short_name: dynamic_env_test_credentials_converter.convert(d) for short_name, d in short_name_sa_data}


@pytest.fixture(scope='function')
def role_check_user_configuration(
        dl_env,
        integration_tests_folder_id,
        ext_passport_acct_01_creds,
        ext_passport_acct_02_creds,
        iam_rm_client,
        dynamic_env_service_accounts,
):
    if dl_env != DLEnv.dynamic:
        if dl_env in [DLEnv.cloud_prod, DLEnv.cloud_preprod]:
            role_id = 'datalens.instances.user'
            folder_id = integration_tests_folder_id

            _assign_datalens_user_role(iam_rm_client, folder_id, ext_passport_acct_01_creds.user_id, role_id)

            u2_roles = iam_rm_client.list_svc_acct_role_ids_on_folder_sync(
                folder_id=folder_id,
                svc_acct_id=ext_passport_acct_02_creds.user_id,
                acct_type='userAccount',
            )

            if role_id in u2_roles:
                iam_rm_client.modify_folder_access_bindings_for_svc_acct_sync(
                    svc_acct_id=ext_passport_acct_02_creds.user_id,
                    acct_type='userAccount',
                    folder_id=folder_id,
                    role_ids=(role_id,),
                    action=AccessBindingAction.REMOVE
                )

        yield BaseAuthCheckUserConfiguration(
            with_dl_inst_use_role=ext_passport_acct_01_creds,
            without_dl_inst_use_role=ext_passport_acct_02_creds,
        )
    else:
        yield BaseAuthCheckUserConfiguration(
            with_dl_inst_use_role=dynamic_env_service_accounts['viewer-1'],
            without_dl_inst_use_role=dynamic_env_service_accounts['nobody'],
        )


@pytest.fixture(scope='function')
def external_dl_users_configuration(
        dl_env,
        integration_tests_folder_id,
        ext_passport_acct_01_creds,
        ext_passport_acct_02_creds,
        iam_rm_client,
):
    if dl_env in [DLEnv.cloud_prod, DLEnv.cloud_preprod]:
        role_id = 'datalens.instances.user'
        folder_id = integration_tests_folder_id

        _assign_datalens_user_role(iam_rm_client, folder_id, ext_passport_acct_01_creds.user_id, role_id)
        _assign_datalens_user_role(iam_rm_client, folder_id, ext_passport_acct_02_creds.user_id, role_id)

    return BaseTwoUserConfiguration(
        user_1=ext_passport_acct_01_creds,
        user_2=ext_passport_acct_02_creds,
    )


@pytest.fixture(scope='function')
def internal_dl_users_configuration(
        intranet_user_1_creds,
        intranet_user_2_creds,
):
    return BaseTwoUserConfiguration(
        user_1=intranet_user_1_creds,
        user_2=intranet_user_2_creds,
    )


@pytest.fixture(scope='function')
def dls_client_factory(
        integration_tests_folder_id,
        per_test_request_id,
        secret_datalens_test_data,
        ext_sys_requisites,
        tenant
) -> Callable[[str], Optional[DLSClient]]:
    if not isinstance(ext_sys_requisites, IntegrationTestConfig):
        return lambda x: None
    if not ext_sys_requisites.DLS_ENABLED:
        return lambda x: None
    dls_url = ext_sys_requisites.DATALENS_DLS_LB_MAIN_BASE_URL
    dls_api_key = secret_datalens_test_data['dls_api_key']

    def _client(user_id):
        rce = RequestContextInfo.create(
            tenant=tenant,
            request_id=per_test_request_id,
            user_name=None,
            user_id=user_id,
            x_dl_debug_mode=None,
            secret_headers=None,
            auth_data=None,
            plain_headers=None,
            endpoint_code=None,
            x_dl_context=None,
        )
        return DLSClient(
            dls_url,
            dls_api_key,
            rce
        )
    return _client


@pytest.fixture(scope='session')
def tenant(
    ext_sys_requisites,
) -> Optional[TenantDef]:
    if not isinstance(ext_sys_requisites, IntegrationTestConfig) or ext_sys_requisites.TENANT_TYPE is None:
        return None
    # TODO: extract env-specific modules for integration tests.
    # Organization tenant is now being used in yc-preprod and israel installations
    return TenantYCOrganization(ext_sys_requisites.TENANT_ID)


def _get_workbook_mgmt_strategy_us_url(ext_sys_requisites):
    if not isinstance(ext_sys_requisites, IntegrationTestConfig):
        return NOT_REQUIRED, None
    return ext_sys_requisites.WORKBOOK_MGMT_STRATEGY, ext_sys_requisites.US_LB_MAIN_BASE_URL


@pytest.fixture(scope='function')
def workbook_id(
        ext_sys_requisites,
        dynamic_env_service_accounts,
        tenant,
) -> Optional[str]:
    strategy, url = _get_workbook_mgmt_strategy_us_url(ext_sys_requisites)
    create_hook, delete_hook = workbook_mgmt_hooks(
        strategy,
        url,
        dynamic_env_service_accounts,
        tenant
    )
    workbook_id = asyncio.run(create_hook())
    yield workbook_id

    asyncio.run(delete_hook(workbook_id))


@pytest.fixture(scope='function')
def two_users_configuration(
        dl_env,
        external_dl_users_configuration,
        internal_dl_users_configuration,
        dynamic_env_service_accounts
) -> BaseTwoUserConfiguration:
    if dl_env == DLEnv.dynamic:
        yield BaseTwoUserConfiguration(
            user_1=dynamic_env_service_accounts['viewer-1'],
            user_2=dynamic_env_service_accounts['viewer-2'],
        )
    elif dl_env in [DLEnv.cloud_preprod, DLEnv.cloud_prod]:
        yield external_dl_users_configuration
    elif dl_env in [DLEnv.internal_preprod, DLEnv.internal_prod]:
        yield internal_dl_users_configuration
    else:
        raise ValueError(f"Unknown DataLens env: {dl_env}.")
