from __future__ import annotations

from typing import Any

import pytest

from bi_cloud_integration.iam_mock import IAMServicesMockFacade
from bi_testing_ya.cloud_tokens import AccountCredentials
from bi_testing_ya.dlenv import DLEnv
from bi_testing_ya.iam_mock import apply_iam_services_mock


pytest_plugins = ("bi_testing_ya.pytest_plugin",)


@pytest.fixture(scope="session")
def cloud_integration_stand_folder_id(dl_env: DLEnv) -> str:
    assert dl_env == DLEnv.cloud_preprod
    return "aoe0l02661iktbv7pimm"


@pytest.fixture(scope="session")
def cloud_integration_stand_admin_account_creds(dl_env, ext_passport_acct_01_creds) -> AccountCredentials:
    assert dl_env == DLEnv.cloud_preprod
    return ext_passport_acct_01_creds


@pytest.fixture(scope="session")
def cloud_integration_stand_not_admin_account_creds(dl_env, ext_passport_acct_02_creds) -> AccountCredentials:
    assert dl_env == DLEnv.cloud_preprod
    return ext_passport_acct_02_creds


@pytest.fixture()
def iam_services_mock(monkeypatch: Any) -> IAMServicesMockFacade:
    yield from apply_iam_services_mock(monkeypatch)
