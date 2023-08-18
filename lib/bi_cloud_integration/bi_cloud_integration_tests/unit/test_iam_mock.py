import pytest

from bi_cloud_integration.exc import YCUnauthenticated, YCPermissionDenied
from bi_cloud_integration.model import IAMResource
from bi_cloud_integration.yc_as_client import DLASClient
from bi_cloud_integration.yc_ss_client import DLSSClient


def test_authentication(iam_services_mock):
    as_client = DLASClient(iam_services_mock.service_config)

    the_iam_token = "sdffjnsgnksl"
    the_user_id = "someuserid"

    user = iam_services_mock.data_holder.make_new_user()
    the_user_id = user.id
    the_iam_token = user.iam_tokens[0]

    resp = as_client.authenticate_sync(the_iam_token)
    assert resp.id == the_user_id

    with pytest.raises(YCUnauthenticated):
        as_client.authenticate_sync("dummy-iam-token")


def test_authorization(iam_services_mock):
    as_client = DLASClient(iam_services_mock.service_config)

    resource_type = "resource-manager.folder"
    resource_id = "the-folder-id"
    assigned_permission = "datalens.instance.use"
    non_assigned_permission = "datalens.instance.admin"

    iam_data = iam_services_mock.data_holder
    user = iam_data.make_new_user((iam_data.Resource.folder(resource_id), assigned_permission))
    the_iam_token = user.iam_tokens[0]

    # Note: not the `iam_data.Resource` mock, because this is passed to the usual client interface.
    auth_r = IAMResource(id=resource_id, type=resource_type)

    as_client.authorize_sync(
        iam_token=the_iam_token,
        permission=assigned_permission,
        resource_path=[auth_r]
    )

    with pytest.raises(YCPermissionDenied):
        as_client.authorize_sync(
            iam_token=the_iam_token,
            permission=non_assigned_permission,
            resource_path=[auth_r]
        )


def test_session_service(iam_services_mock):
    as_client = DLASClient(iam_services_mock.service_config)
    ss_client = DLSSClient(iam_services_mock.service_config).clone(
        bearer_token="fakeIAMtokenOFserviceAccount",
    )

    the_user_id = "somecookieuserid"
    the_yc_session_cookie_val = "yc_s_1234"
    the_host_header = "datalens.some.tld"

    iam_data = iam_services_mock.data_holder
    user = iam_data.make_new_user()
    the_user_id = user.id
    the_yc_session_cookie_val = user.cookies[0]

    check_cookie_resp = ss_client.check_sync(
        cookie_header=iam_data.User.yc_session_to_cookie_header(the_yc_session_cookie_val),
        host=the_host_header,
    )

    assigned_iam_token = check_cookie_resp.iam_token.iam_token
    assert assigned_iam_token

    authenticate_response = as_client.authenticate_sync(assigned_iam_token)
    assert authenticate_response.id == the_user_id
