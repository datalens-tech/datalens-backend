from __future__ import annotations

import logging
import threading
from typing import Optional, Tuple

import attr
import flask

from bi_cloud_integration.yc_as_client import DLASClient
from bi_cloud_integration.yc_client_base import DLYCServiceConfig
from bi_cloud_integration.yc_ss_client import DLSSClient
from bi_api_commons_ya_cloud.constants import YcTokenHeaderMode

from bi_api_commons.access_control_common import match_path_prefix, AuthFailureError
from bi_api_commons.flask.middlewares.commit_rci_middleware import ReqCtxInfoMiddleware

from bi_api_commons_ya_cloud.yc_access_control import YCAccessController
from bi_api_commons_ya_cloud.yc_access_control_model import AuthorizationMode

from bi_api_lib.app.control_api.resources import handle_auth_error


LOGGER = logging.getLogger(__name__)


@attr.s
class FlaskYCAuthService:
    _authorization_mode: AuthorizationMode = attr.ib()
    _enable_cookie_auth: bool = attr.ib()
    _access_service_cfg: DLYCServiceConfig = attr.ib()
    _session_service_cfg: Optional[DLYCServiceConfig] = attr.ib(default=None)
    # If false - auth check callback will not be registered in Flask.before_request()
    #  Currently used in materializer where authentication is called directly in handlers
    _run_before_request: bool = attr.ib(default=True)
    _yc_token_header_mode: YcTokenHeaderMode = attr.ib(default=YcTokenHeaderMode.INTERNAL)
    _static_sa_token_for_session_service: Optional[str] = attr.ib(default=None, repr=False)
    _skip_path_list: Tuple[str, ...] = attr.ib(default=())

    # Internals
    _sync_lock: threading.Lock = attr.ib(init=False, factory=threading.Lock, repr=False)
    _app: Optional[flask.Flask] = attr.ib(init=False, default=None, repr=False)
    _base_ss_client: Optional[DLSSClient] = attr.ib(init=False, default=None)
    _as_client: Optional[DLASClient] = attr.ib(init=False, default=None)

    def __attrs_post_init__(self) -> None:
        if self._enable_cookie_auth:
            assert self._session_service_cfg is not None, "To enable cookie auth session_service_cfg must be provided"

    @property
    def as_client(self) -> DLASClient:
        # Access service client is not instantiated within constructor to prevent fork issues
        with self._sync_lock:
            if self._as_client is None:
                self._as_client = DLASClient(service_config=self._access_service_cfg)
        return self._as_client

    def _create_base_ss_client(self) -> DLSSClient:
        assert self._session_service_cfg is not None
        yc_ss_cli = DLSSClient(self._session_service_cfg)

        if self._static_sa_token_for_session_service is not None:
            yc_ss_cli = yc_ss_cli.clone(bearer_token=self._static_sa_token_for_session_service)
        else:
            yc_ss_cli = yc_ss_cli.ensure_fresh_token_sync()
        return yc_ss_cli

    def get_ss_client_for_current_request(self) -> Optional[DLSSClient]:
        """
        Session service client stores IAM token of SA in it's fields.
        So we have to create new instance for each request to re-fetch SA IAM token
         from machine metadata due to it has a limited lifetime.
        Request ID will be actualized in YCAccessController
        """
        # Base client holds gRPC channel so we instantiate it with lock to prevent multiple channels instantiation
        with self._sync_lock:
            if self._base_ss_client is None:
                self._base_ss_client = self._create_base_ss_client()

        actual_client = self._base_ss_client

        if self._static_sa_token_for_session_service is None:
            # Note that this obtains a service user token from yc container metadata,
            # and it likely needs a special permission
            # ( `internal.oauth.client` for now, to be changed later: https://st.yandex-team.ru/CLOUD-41544 )
            # https://bb.yandex-team.ru/projects/CLOUD/repos/iam-sync-configs/commits/65879c374752922149efebf6d6bfd63200d0bf63
            # https://bb.yandex-team.ru/projects/CLOUD/repos/iam-sync-configs/pull-requests/1738/commits/c31956a22d1d094eef7204c3f925b35190bd1a52
            actual_client = actual_client.ensure_fresh_token_sync()

        return actual_client

    def perform_all_security_checks_for_current_request(self) -> None:
        initial_rci = ReqCtxInfoMiddleware.get_temp_rci()
        req_id = initial_rci.request_id
        assert req_id is not None, "Request ID was not set in temp RCI before YC auth security checks"

        request_path = flask.request.path
        should_skip_auth = match_path_prefix(
            prefix_list=self._skip_path_list,
            path=request_path,
        )

        if should_skip_auth:
            LOGGER.info(f"Skipping request auth because of configured skip prefix list. {request_path=!r}")
            return

        ss_client: Optional[DLSSClient] = (
            self.get_ss_client_for_current_request() if self._enable_cookie_auth
            else None
        )

        access_ctrl = YCAccessController(
            authorization_mode=self._authorization_mode,
            cookie_auth_enabled=self._enable_cookie_auth,
            as_client=self.as_client,
            ss_client=ss_client,
            yc_token_header_mode=self._yc_token_header_mode,
            request_headers=dict(flask.request.headers),
            request_id=req_id,
            # TODO FIX: BI-2717 determine if we need it
            # req_logging_ctx_ctrl=RequestLoggingContextControllerMiddleWare.get_for_request(),
        )

        try:
            auth_ctx = access_ctrl.perform_base_security_checks_sync()
            enriched_rci = access_ctrl.update_rci_with_auth_ctx(
                ReqCtxInfoMiddleware.get_temp_rci(),
                auth_ctx,
            )
            ReqCtxInfoMiddleware.replace_temp_rci(enriched_rci)
        except Exception as err:
            if isinstance(err, AuthFailureError) and err.response_code in (401, 403):
                raise
            else:
                LOGGER.exception("yc auth error: %r", err)
                raise
        else:
            LOGGER.info("check_yc_auth successful.")

    # TODO FIX: https://st.yandex-team.ru/BI-2708 move exception handling to common error-response formatter
    @classmethod
    def _plain_flask_err_handler(cls, err: AuthFailureError) -> Tuple[flask.Response, int]:
        resp_dict, status = handle_auth_error(err)
        return flask.jsonify(resp_dict), status

    def set_up(self, app: flask.Flask) -> None:
        LOGGER.info("Set up yc auth with access service")

        with self._sync_lock:
            if self._app is not None:
                raise Exception("Attempt to setup FlaskYCAuthService more than one time")
            self._app = app

        # TODO FIX: https://st.yandex-team.ru/BI-2708 move exception handling to common error-response formatter
        app.register_error_handler(AuthFailureError, self._plain_flask_err_handler)

        app.before_request(self.perform_all_security_checks_for_current_request)
