from typing import Optional

import attr
import flask

from dl_api_commons.base_models import (
    AuthData,
    NoAuthData,
    TenantCommon,
    TenantDef,
)
from dl_api_commons.flask.middlewares.commit_rci_middleware import ReqCtxInfoMiddleware


@attr.s(frozen=True, auto_attribs=True)
class TrustAuthService:
    """
    Service that unconditionally injects user/tenant info into request context.
    Should be used only in tests
    """

    fake_user_id: Optional[str] = None
    fake_user_name: Optional[str] = None
    fake_tenant: Optional[TenantDef] = None
    fake_auth_data: Optional[AuthData] = None

    def _before_request(self) -> None:
        fake_user_id = self.fake_user_id
        fake_user_name = self.fake_user_name
        fake_tenant = self.fake_tenant

        temp_rci = ReqCtxInfoMiddleware.get_temp_rci().clone(
            auth_data=NoAuthData() if self.fake_auth_data is None else self.fake_auth_data,
            tenant=TenantCommon() if fake_tenant is None else fake_tenant,
        )
        if fake_user_id is not None:
            temp_rci = temp_rci.clone(user_id=fake_user_id)
        if fake_user_name is not None:
            temp_rci = temp_rci.clone(user_name=fake_user_name)

        ReqCtxInfoMiddleware.replace_temp_rci(temp_rci)

    def set_up(self, app: flask.Flask) -> None:
        app.before_request(self._before_request)
