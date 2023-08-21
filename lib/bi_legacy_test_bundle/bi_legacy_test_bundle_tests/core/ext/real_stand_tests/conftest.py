from __future__ import annotations

import pytest

from bi_testing_ya.cloud_tokens import AccountCredentials

# ##### #
# Creds #
# ##### #
from bi_testing_ya.dlenv import DLEnv


@pytest.fixture()
def test_intranet_user_creds_wrong_scope(env_param_getter) -> AccountCredentials:
    return AccountCredentials(
        user_id='1120000000086935',
        user_name='robot-statbox-kappa',
        is_intranet_user=True,
        token=env_param_getter.get_str_value('METRIKA_OAUTH'),
    )


# Cloud
@pytest.fixture(scope='session')
def bi_common_stand_folder_id(bi_tests_folder_id_default) -> str:
    return bi_tests_folder_id_default


@pytest.fixture(scope='session')
def bi_common_stand_ok_user_account_creds(dl_env, ext_passport_acct_01_creds) -> AccountCredentials:
    assert dl_env == DLEnv.cloud_preprod
    # TODO FIX: BI-2200 ensure that user has datalens.instance.user/datalens.instance.admin
    return ext_passport_acct_01_creds


@pytest.fixture(scope='session')
def bi_common_stand_access_service_endpoint(dl_env, ext_sys_requisites) -> str:
    assert dl_env == DLEnv.cloud_preprod
    return ext_sys_requisites.YC_AS_ENDPOINT
