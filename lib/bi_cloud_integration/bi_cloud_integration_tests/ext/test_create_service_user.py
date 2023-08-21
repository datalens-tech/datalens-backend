from __future__ import annotations

import datetime
import time

import pytest
import shortuuid

from bi_cloud_integration.exc import YCPermissionDenied
from bi_cloud_integration.iam_rm_client import IAMRMClient
from bi_cloud_integration.model import ServiceAccountData, AccessBindingAction, IAMResource
from bi_cloud_integration.yc_as_client import DLASClient
from bi_cloud_integration.yc_client_base import DLYCServiceConfig
from bi_cloud_integration.yc_ts_client import get_yc_service_token_sync
from bi_testing import shared_testing_constants
from bi_testing_ya.external_systems_helpers.top import ExternalSystemsHelperCloud
from bi_testing.utils import skip_outside_devhost

DEFAULT_TEST_ROBOT_DESCRIPTION = 'DataLensRobot created during auto-tests. Can be removed without any consequences.'


def generate_service_account_name() -> str:
    return f"test-robot-to-delete-{shortuuid.random(8)}".lower()


@pytest.fixture(scope='function')
def iam_rm_client(
        cloud_integration_stand_admin_account_creds,
        ext_sys_helpers_per_test: ExternalSystemsHelperCloud,
) -> IAMRMClient:
    return ext_sys_helpers_per_test.get_iam_rm_client(cloud_integration_stand_admin_account_creds.token)


@pytest.fixture(scope='function')
def iam_rm_client_non_folder_admin(
        ext_sys_helpers_per_test: ExternalSystemsHelperCloud,
        cloud_integration_stand_not_admin_account_creds,
) -> IAMRMClient:
    return ext_sys_helpers_per_test.get_iam_rm_client(cloud_integration_stand_not_admin_account_creds.token)


@pytest.fixture(scope='function')
def svc_acct_per_test(iam_rm_client, cloud_integration_stand_folder_id):
    folder_id = cloud_integration_stand_folder_id
    svc_acct = iam_rm_client.create_svc_acct_sync(
        name=generate_service_account_name(),
        description=DEFAULT_TEST_ROBOT_DESCRIPTION,
        folder_id=folder_id,
    )
    yield svc_acct
    iam_rm_client.delete_svc_acct_sync(svc_acct.id)


@skip_outside_devhost
def test_svc_acct_creation_no_permissions(iam_rm_client_non_folder_admin, cloud_integration_stand_folder_id):
    folder_id = cloud_integration_stand_folder_id
    ctrl = iam_rm_client_non_folder_admin

    svc_acct_name = generate_service_account_name()
    svc_acct_description = DEFAULT_TEST_ROBOT_DESCRIPTION

    with pytest.raises(YCPermissionDenied) as exc:
        ctrl.create_svc_acct_sync(
            folder_id=folder_id,
            name=svc_acct_name,
            description=svc_acct_description,
        )

    yc_exc_info = exc.value.info
    assert yc_exc_info.operation_code == 'sa_create'


@skip_outside_devhost
def test_svc_acct_creation(iam_rm_client, cloud_integration_stand_folder_id):
    folder_id = cloud_integration_stand_folder_id
    ctrl = iam_rm_client

    svc_acct_name = generate_service_account_name()
    svc_acct_description = DEFAULT_TEST_ROBOT_DESCRIPTION

    new_svc_acct = ctrl.create_svc_acct_sync(
        folder_id=folder_id,
        name=svc_acct_name,
        description=svc_acct_description,
    )
    try:
        svc_acct_id = new_svc_acct.id

        expected_svc_acct = ServiceAccountData(id=svc_acct_id, name=svc_acct_name, description=svc_acct_description)
        assert new_svc_acct == expected_svc_acct

        loaded_svc_acct = ctrl.get_svc_acct_sync(svc_acct_id)

        assert loaded_svc_acct == expected_svc_acct
    finally:
        ctrl.delete_svc_acct_sync(new_svc_acct.id)


@skip_outside_devhost
def test_create_svc_acct_api_key(iam_rm_client, svc_acct_per_test):
    ctrl = iam_rm_client
    svc_acct = svc_acct_per_test

    api_key_description = "My lolly key"

    ts_before_create = datetime.datetime.now(datetime.timezone.utc).replace(microsecond=0)
    api_key_pack = ctrl.create_svc_acct_key_sync(svc_acct.id, description=api_key_description)
    ts_after_create = datetime.datetime.now(datetime.timezone.utc).replace(microsecond=0)

    api_key_info = api_key_pack.svc_acct_key_data

    assert api_key_pack.private_key is not None

    assert api_key_info.id
    assert api_key_info.svc_acct_id == svc_acct.id
    assert api_key_info.description == api_key_description
    assert isinstance(api_key_info.created_at, datetime.datetime)
    assert ts_after_create >= api_key_info.created_at >= ts_before_create


@skip_outside_devhost
def test_list_svc_acct_role_ids(iam_rm_client, svc_acct_per_test, cloud_integration_stand_folder_id):
    folder_id = cloud_integration_stand_folder_id
    svc_acct = svc_acct_per_test

    roles_to_grant = ['datalens.instances.user', 'datalens.instances.admin']

    iam_rm_client.modify_folder_access_bindings_for_svc_acct_sync(
        svc_acct_id=svc_acct.id,
        folder_id=folder_id,
        role_ids=roles_to_grant,
        action=AccessBindingAction.ADD,
    )

    roles = iam_rm_client.list_svc_acct_role_ids_on_folder_sync(svc_acct_id=svc_acct.id, folder_id=folder_id, _page_size=1)
    assert set(roles) == set(roles_to_grant)


@skip_outside_devhost
def test_grant_role_on_sa(
        iam_rm_client,
        svc_acct_per_test,
        ext_sys_helpers_per_test,
        cloud_integration_stand_folder_id,
):
    folder_id = cloud_integration_stand_folder_id
    ctrl = iam_rm_client
    svc_acct = svc_acct_per_test

    as_client = DLASClient(
        service_config=DLYCServiceConfig(
            endpoint=ext_sys_helpers_per_test.ext_sys_requisites.YC_AS_ENDPOINT
        )
    )

    api_key_pack = ctrl.create_svc_acct_key_sync(svc_acct.id, description="Default service account key")

    sa_iam_token = get_yc_service_token_sync(
        key_data=dict(
            service_account_id=svc_acct.id,
            key_id=api_key_pack.svc_acct_key_data.id,
            private_key=api_key_pack.private_key,
        ),
        yc_ts_endpoint=ext_sys_helpers_per_test.ext_sys_requisites.YC_TS_ENDPOINT,
    )

    assert sa_iam_token

    role = 'datalens.instances.user'
    required_permission = 'datalens.instances.use'
    resource = IAMResource(
        id=folder_id,
        type="resource-manager.folder",
    )

    def check_against_access_service():
        as_client.authorize_sync(
            iam_token=sa_iam_token,
            resource_path=[resource],
            permission=required_permission,
        )

    actual_roles = ctrl.list_svc_acct_role_ids_on_folder_sync(svc_acct_id=svc_acct.id, folder_id=folder_id)
    assert actual_roles == []

    with pytest.raises(YCPermissionDenied):
        check_against_access_service()

    ctrl.modify_folder_access_bindings_for_svc_acct_sync(
        svc_acct_id=svc_acct.id,
        folder_id=folder_id,
        role_ids=[role],
        action=AccessBindingAction.ADD,
    )
    actual_roles = ctrl.list_svc_acct_role_ids_on_folder_sync(svc_acct_id=svc_acct.id, folder_id=folder_id)
    assert actual_roles == [role]
    time.sleep(shared_testing_constants.ACCESS_SERVICE_PERMISSIONS_CHECK_DELAY)

    check_against_access_service()

    ctrl.modify_folder_access_bindings_for_svc_acct_sync(
        svc_acct_id=svc_acct.id,
        folder_id=folder_id,
        role_ids=[role],
        action=AccessBindingAction.REMOVE,
    )
    actual_roles = ctrl.list_svc_acct_role_ids_on_folder_sync(svc_acct_id=svc_acct.id, folder_id=folder_id)
    assert actual_roles == []
    time.sleep(shared_testing_constants.ACCESS_SERVICE_PERMISSIONS_CHECK_DELAY)

    with pytest.raises(YCPermissionDenied):
        check_against_access_service()


@skip_outside_devhost
def test_get_robot_key(
        iam_rm_client,
        svc_acct_per_test,
        cloud_integration_stand_folder_id,
):
    ctrl = iam_rm_client
    svc_acct = svc_acct_per_test

    created_key = ctrl.create_svc_acct_key_sync(svc_acct.id, f"test key").svc_acct_key_data
    fetched_key = ctrl.get_svc_acct_key_sync(created_key.id)

    assert fetched_key == created_key


@skip_outside_devhost
def test_list_robot_keys(
        iam_rm_client,
        svc_acct_per_test,
        cloud_integration_stand_folder_id,
):
    ctrl = iam_rm_client
    svc_acct = svc_acct_per_test

    created_key_data_list = []

    for i in range(3):
        created_key_data_list.append(
            ctrl.create_svc_acct_key_sync(svc_acct.id, f"test key {i}").svc_acct_key_data
        )

    fetched_key_data_list = ctrl.list_svc_acct_keys_sync(svc_acct.id, _page_size=2)

    created_key_data_list.sort(key=lambda k: k.id)
    fetched_key_data_list.sort(key=lambda k: k.id)

    assert fetched_key_data_list == created_key_data_list


@skip_outside_devhost
def test_delete_robot_key(
        iam_rm_client,
        svc_acct_per_test,
        cloud_integration_stand_folder_id,
):
    ctrl = iam_rm_client
    svc_acct = svc_acct_per_test

    key_to_preserve = ctrl.create_svc_acct_key_sync(svc_acct.id, f"test key to preserve").svc_acct_key_data
    key_to_delete = ctrl.create_svc_acct_key_sync(svc_acct.id, f"test key to delete").svc_acct_key_data

    ctrl.delete_svc_acct_key_sync(key_to_delete.id)

    fetched_key_list = ctrl.list_svc_acct_keys_sync(svc_acct.id)

    assert fetched_key_list == [key_to_preserve]
