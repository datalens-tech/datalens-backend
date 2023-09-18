from __future__ import annotations

from typing import (
    Any,
    Callable,
    ClassVar,
    Dict,
    Generator,
    Union,
)

import pytest
import shortuuid

from bi_api_commons_ya_cloud.constants import (
    DLHeadersYC,
    YcTokenHeaderMode,
)
from bi_cloud_integration.iam_mock import (
    IAMMockResource,
    IAMMockUser,
    IAMServicesMockFacade,
)
from bi_testing_ya.iam_mock import apply_iam_services_mock
from dl_api_commons.error_messages import UserErrorMessages
from dl_constants.api_constants import (
    DLHeaders,
    DLHeadersCommon,
)


def get_headers(values: Dict[DLHeaders, str]) -> Dict[str, str]:
    return {header.value: value for header, value in values.items()}


class Scenario_YCAuth_Base:  # noqa
    DEFAULT_YC_TOKEN_HEADER_MODE: ClassVar[YcTokenHeaderMode] = YcTokenHeaderMode.INTERNAL

    @pytest.fixture(params=list(YcTokenHeaderMode))
    def yc_token_header_mode(self, request) -> YcTokenHeaderMode:
        actual_param = request.param if hasattr(request, "param") else self.DEFAULT_YC_TOKEN_HEADER_MODE
        assert isinstance(
            actual_param, YcTokenHeaderMode
        ), f"Invalid parameter for 'yc_token_header_mode' fixture: {actual_param.fixture}"
        return actual_param

    @pytest.fixture()
    def iam_token_header_key(self, yc_token_header_mode) -> DLHeaders:
        return {
            YcTokenHeaderMode.INTERNAL: DLHeadersYC.IAM_TOKEN,
            YcTokenHeaderMode.EXTERNAL: DLHeadersCommon.AUTHORIZATION_TOKEN,
            YcTokenHeaderMode.UNIVERSAL: DLHeadersYC.IAM_TOKEN,
        }[yc_token_header_mode]

    @pytest.fixture()
    def iam_token_header_value_encoder(self, iam_token_header_key: DLHeaders) -> Callable[[str], str]:
        return {DLHeadersYC.IAM_TOKEN: lambda s: s, DLHeadersCommon.AUTHORIZATION_TOKEN: lambda s: f"Bearer {s}"}[
            iam_token_header_key
        ]

    @pytest.fixture()
    def iam_token_header_kv_encoder(
        self, iam_token_header_key, iam_token_header_value_encoder
    ) -> Callable[[str], Dict[DLHeaders, str]]:
        return lambda s: {iam_token_header_key: iam_token_header_value_encoder(s)}

    def get_resp_json(self, msg: Union[str, UserErrorMessages]) -> Dict[str, Any]:
        actual_msg = msg.value if isinstance(msg, UserErrorMessages) else msg
        return {"message": actual_msg}

    @pytest.fixture()
    def client(self, yc_token_header_mode) -> Any:
        """
        Here sync testing client should be implemented.
        Please take in account fixtures defined in signature of this method.
        It may be parametrized in particular tests.
        """
        raise NotImplementedError()

    @pytest.fixture()
    def iam(self, monkeypatch) -> Generator[IAMServicesMockFacade, None, None]:
        yield from apply_iam_services_mock(monkeypatch)

    @pytest.mark.parametrize(
        "yc_token_header_mode,should_be_ignored_header",
        [
            (YcTokenHeaderMode.INTERNAL, DLHeadersCommon.AUTHORIZATION_TOKEN),
            (YcTokenHeaderMode.EXTERNAL, DLHeadersYC.IAM_TOKEN),
            (
                YcTokenHeaderMode.UNIVERSAL,
                None,
            ),
            (
                YcTokenHeaderMode.UNIVERSAL,
                None,
            ),
        ],
        indirect=["yc_token_header_mode"],
    )
    def test_no_headers(self, client, should_be_ignored_header):
        """
        For YcTokenHeaderMode.UNIVERSAL we check correctness of message
        For other YcTokenHeaderMode we check that invalid header will be picked as IAM token
        """
        headers = (
            None if should_be_ignored_header is None else get_headers({should_be_ignored_header: "dummy-iam-token"})
        )
        resp = client.get("/auth_ctx", headers=headers)
        assert resp.status_code == 401
        assert resp.json == self.get_resp_json(UserErrorMessages.no_authentication_data_provided)

    @pytest.mark.parametrize(
        "yc_token_header_mode,iam_token_header_name",
        [
            (YcTokenHeaderMode.INTERNAL, DLHeadersYC.IAM_TOKEN),
            (YcTokenHeaderMode.EXTERNAL, DLHeadersCommon.AUTHORIZATION_TOKEN),
            (
                YcTokenHeaderMode.UNIVERSAL,
                DLHeadersCommon.AUTHORIZATION_TOKEN,
            ),
            (
                YcTokenHeaderMode.UNIVERSAL,
                DLHeadersYC.IAM_TOKEN,
            ),
        ],
        indirect=["yc_token_header_mode"],
    )
    def test_invalid_iam_token(self, client, yc_token_header_mode, iam_token_header_name):
        """
        Check that for each YcTokenHeaderMode correct header will be picked as IAM token
        """
        iam_token_header_value = {
            DLHeadersYC.IAM_TOKEN: lambda s: s,
            DLHeadersCommon.AUTHORIZATION_TOKEN: lambda s: f"Bearer {s}",
        }[iam_token_header_name]("dummy_iam_tokenn12312")

        resp = client.get("/auth_ctx", headers=get_headers({iam_token_header_name: iam_token_header_value}))
        assert resp.status_code == 401
        assert resp.json == self.get_resp_json(UserErrorMessages.user_unauthenticated)

    @pytest.mark.parametrize(
        "yc_token_header_mode",
        [
            YcTokenHeaderMode.EXTERNAL,
            YcTokenHeaderMode.UNIVERSAL,
        ],
        indirect=True,
    )
    def test_correct_error_on_invalid_authorization_header(self, client):
        """
        Check that correct error message will be received on missing `Bearer` in `Authorization` header
        """
        resp = client.get(
            "/auth_ctx", headers=get_headers({DLHeadersCommon.AUTHORIZATION_TOKEN: "some-garbage-14351234"})
        )
        assert resp.status_code == 403
        assert resp.json == self.get_resp_json("Invalid 'Authorization' header format")

    @pytest.fixture()
    def skip_auth_exact_path(self):
        return "/skip_auth_exact"

    @pytest.fixture()
    def skip_auth_prefixed_path(self):
        return "/skip_auth_prefix/*"

    def test_skip_auth_exact(self, client, skip_auth_exact_path):
        skip_auth_resp = client.get(skip_auth_exact_path)
        assert skip_auth_resp.status_code == 200

        skip_auth_resp = client.get(f"{skip_auth_exact_path}?a=1")
        assert skip_auth_resp.status_code == 200

    def test_skip_auth_prefixed(self, client, skip_auth_prefixed_path):
        assert skip_auth_prefixed_path.endswith("/*")
        random_postfix = shortuuid.uuid()

        # Cut out `*` and add some random part to path
        skip_auth_resp = client.get(f"{skip_auth_prefixed_path[:-2]}/{random_postfix}")
        assert skip_auth_resp.status_code == 404

        # Cut out `/*` and add some random part to path without splitting
        non_skip_auth_resp = client.get(f"{skip_auth_prefixed_path[:-2]}{random_postfix}")
        assert non_skip_auth_resp.status_code == 401


class Scenario_YCAuth_ModeDataCloud_DenyCookieAuth(Scenario_YCAuth_Base):
    @pytest.fixture()
    def authorized_user(self, project_id, project_required_permission, iam) -> IAMMockUser:
        user = IAMMockUser(
            id="the-ok-user-id",
            iam_tokens=["the_ok_user_iam_token"],
            cookies=["yc-session-cookie-authorized-user-12132471"],
        ).with_permissions(IAMMockResource.cloud(project_id), project_required_permission)
        iam.data_holder.add_users(user)
        return user

    @pytest.fixture()
    def project_required_permission(self) -> str:
        return "dl.inst.use"

    @pytest.fixture()
    def project_id(self) -> str:
        return "__dummy_project_id__"

    def test_authentication_ok(self, client, authorized_user, iam_token_header_kv_encoder, project_id):
        resp = client.get(
            "/auth_ctx",
            headers=get_headers(
                {
                    **iam_token_header_kv_encoder(authorized_user.get_single_iam_token()),
                    DLHeadersCommon.PROJECT_ID: project_id,
                }
            ),
        )
        assert resp.status_code == 200
        assert resp.json == dict(
            user_id=authorized_user.id,
            iam_token=authorized_user.get_single_iam_token(),
        )

    def test_cookie_auth_fails(self, client, authorized_user):
        client.set_cookie("", "yc_session", authorized_user.get_single_yc_cookie())
        resp = client.get("/auth_ctx")
        assert resp.status_code == 401
        assert resp.json == self.get_resp_json(UserErrorMessages.no_authentication_data_provided)


class Scenario_YCAuth_ModeYC(Scenario_YCAuth_Base):
    @pytest.fixture()
    def folder_id(self) -> str:
        return "sample_folder_id"

    @pytest.fixture()
    def org_id(self) -> str:
        return "049kd32f341k"

    @pytest.fixture()
    def folder_required_permission(self) -> str:
        return "dl.inst.use"

    @pytest.fixture()
    def organization_required_permission(self) -> str:
        return "dl.inst.use"

    @pytest.fixture()
    def authorized_user(self, folder_id, folder_required_permission, iam) -> IAMMockUser:
        user = IAMMockUser(
            id="the-ok-user-id",
            iam_tokens=["the_ok_user_iam_token"],
            cookies=["yc-session-cookie-authorized-user-12132471"],
        ).with_permissions(IAMMockResource.folder(folder_id), folder_required_permission)
        iam.data_holder.add_users(user)
        return user

    @pytest.fixture()
    def unauthorized_user(self, iam) -> IAMMockUser:
        user = IAMMockUser(id="the-no-perms-user-id", iam_tokens=["the_no_perms_user_iam_token"])
        iam.data_holder.add_users(user)
        return user

    def test_authentication_ok_no_folder_id(self, client, authorized_user, iam_token_header_kv_encoder):
        resp = client.get(
            "/auth_ctx", headers=get_headers(iam_token_header_kv_encoder(authorized_user.get_single_iam_token()))
        )
        assert resp.status_code == 401
        assert resp.json == self.get_resp_json(UserErrorMessages.no_tenant_specified)

    def test_authentication_ok_authorization_ok(
        self,
        client,
        authorized_user,
        folder_id,
        iam_token_header_kv_encoder,
    ):
        resp = client.get(
            "/auth_ctx",
            headers=get_headers(
                {
                    **iam_token_header_kv_encoder(authorized_user.get_single_iam_token()),
                    DLHeadersYC.FOLDER_ID: folder_id,
                }
            ),
        )
        assert resp.status_code == 200
        if "folder_id" in resp.json:
            raise ValueError()
        assert resp.json == dict(
            user_id=authorized_user.id,
            iam_token=authorized_user.get_single_iam_token(),
            tenant_id=folder_id,
        )

    @pytest.mark.parametrize(
        "tenant_header_mode",
        (
            "folder_id",
            "tenant_id",
        ),
    )
    def test_yc_tenant_resolution_folder(
        self,
        tenant_header_mode,
        folder_id,
        client,
        authorized_user,
        iam_token_header_kv_encoder,
    ):
        tenant_headers = {
            "folder_id": lambda: {DLHeadersYC.FOLDER_ID: folder_id},
            "tenant_id": lambda: {DLHeadersCommon.TENANT_ID: folder_id},
        }[tenant_header_mode]()

        resp = client.get(
            "/auth_ctx",
            headers=get_headers(
                {
                    **iam_token_header_kv_encoder(authorized_user.get_single_iam_token()),
                    **tenant_headers,
                }
            ),
        )
        assert resp.status_code == 200
        assert resp.json == dict(
            user_id=authorized_user.id,
            iam_token=authorized_user.get_single_iam_token(),
            tenant_id=folder_id,
        )

    @pytest.mark.parametrize(
        "tenant_header_mode",
        (
            "org_id",
            "tenant_id",
        ),
    )
    def test_yc_tenant_resolution_org(
        self,
        tenant_header_mode,
        org_id,
        client,
        authorized_user,
        iam_token_header_kv_encoder,
    ):
        tenant_headers = {
            "org_id": lambda: {DLHeadersCommon.ORG_ID: org_id},
            "tenant_id": lambda: {DLHeadersCommon.TENANT_ID: f"org_{org_id}"},
        }[tenant_header_mode]()

        resp = client.get(
            "/auth_ctx",
            headers=get_headers(
                {
                    **iam_token_header_kv_encoder(authorized_user.get_single_iam_token()),
                    **tenant_headers,
                }
            ),
        )

        # TODO FIX: Check that organization is successfully extracted from headers when authorization will be ready
        assert resp.status_code == 403
        assert resp.json == self.get_resp_json(UserErrorMessages.user_unauthorized)

    def test_authentication_ok_authorization_fail(
        self,
        client,
        unauthorized_user,
        folder_id,
        iam_token_header_kv_encoder,
    ):
        resp = client.get(
            "/auth_ctx",
            headers=get_headers(
                {
                    **iam_token_header_kv_encoder(unauthorized_user.get_single_iam_token()),
                    DLHeadersYC.FOLDER_ID: folder_id,
                }
            ),
        )
        assert resp.status_code == 403
        assert resp.json == self.get_resp_json(UserErrorMessages.user_unauthorized)


class Scenario_YCAuth_ModeYC_DenyCookieAuth(Scenario_YCAuth_ModeYC):
    def test_cookie_auth_fails(self, client, authorized_user, folder_id):
        client.set_cookie("", "yc_session", authorized_user.get_single_yc_cookie())
        resp = client.get("/auth_ctx", headers=get_headers({DLHeadersYC.FOLDER_ID: folder_id}))
        assert resp.status_code == 401
        assert resp.json == self.get_resp_json(UserErrorMessages.no_authentication_data_provided)


class Scenario_YCAuth_ModeYC_AllowCookieAuth(Scenario_YCAuth_ModeYC):
    def test_cookie_auth_ok(self, client, authorized_user, folder_id, iam_token_header_kv_encoder):
        previous_iam_token = authorized_user.get_single_iam_token()

        client.set_cookie("", "yc_session", authorized_user.get_single_yc_cookie())
        resp = client.get("/auth_ctx", headers=get_headers({DLHeadersYC.FOLDER_ID: folder_id}))
        assert resp.status_code == 200
        new_iam_token = resp.json["iam_token"]
        # Check that new IAM token was issued for user
        assert previous_iam_token != new_iam_token

        expected_ctx = dict(
            user_id=authorized_user.id,
            iam_token=new_iam_token,
            tenant_id=folder_id,
        )
        assert resp.json == expected_ctx

        # Check that resolved IAM token is valid
        client.delete_cookie("", "yc_session")
        ctx_resp_header = client.get(
            "/auth_ctx",
            headers=get_headers(
                {
                    DLHeadersYC.FOLDER_ID: folder_id,
                    **iam_token_header_kv_encoder(new_iam_token),
                }
            ),
        )
        assert ctx_resp_header.status_code == 200
        assert ctx_resp_header.json == expected_ctx
