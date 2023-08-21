import os
import time

import jwt
import pytest
import requests

from bi_testing_ya.cloud_tokens import AccountCredentials
from bi_testing_ya.dlenv import DLEnv
from bi_testing.env_params.generic import GenericEnvParamGetter
from bi_testing.files import get_file_loader


@pytest.fixture(scope='session')
def env_param_getter() -> GenericEnvParamGetter:
    filepath = os.path.join(os.path.dirname(__file__), 'params.yml')
    filepath = get_file_loader().resolve_path(filepath)
    return GenericEnvParamGetter.from_yaml_file(filepath)


# Override fixture from `lib/bi_testing`
@pytest.fixture(scope='session')
def dl_env() -> DLEnv:
    return DLEnv.dc_prod


@pytest.fixture(scope="session")
def dc_rs_project_id(env_param_getter) -> str:
    return env_param_getter.get_yaml_value('INTEGRATION_TESTS_DC_PROD_DATA')['could_id']  # FIXME: cloud_id?


@pytest.fixture(scope="session")
def dc_rs_user_account(integration_tests_admin_sa_data) -> AccountCredentials:
    # Fixture integration_tests_admin_sa use GRPC TokenService
    #  which is not exposed in DoubleCloud yet.
    # So we need to use public HTTP endpoint to exchange SA key to IAM-token.
    # In DC preprod we use development expose of TokenService, so this way is actual only for DC prod.
    sa_data = integration_tests_admin_sa_data

    now = int(time.time())
    payload = {
        'aud': 'https://auth.double.cloud/oauth/token',
        'iss': sa_data.sa_id,
        'sub': sa_data.sa_id,
        'iat': now,
        'exp': now + 360
    }
    encoded_token = jwt.encode(
        payload,
        sa_data.key_pem_data,
        algorithm='PS256',
        headers={'kid': sa_data.key_id}
    )
    resp = requests.post(
        "https://auth.double.cloud/oauth/token",
        data={
            "grant_type": "urn:ietf:params:oauth:grant-type:jwt-bearer",
            "assertion": encoded_token
        },
    )
    resp.raise_for_status()

    return AccountCredentials(
        user_id=sa_data.sa_id,
        is_sa=True,
        is_intranet_user=False,
        token=resp.json()["access_token"],
    )
