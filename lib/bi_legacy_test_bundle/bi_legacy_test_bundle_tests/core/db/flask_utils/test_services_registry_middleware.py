from __future__ import annotations

import flask

from dl_constants.enums import USAuthMode

from dl_api_commons.flask.middlewares.commit_rci_middleware import ReqCtxInfoMiddleware
from dl_api_commons.flask.middlewares.logging_context import RequestLoggingContextControllerMiddleWare
from dl_api_commons.flask.middlewares.context_var_middleware import ContextVarMiddleware
from dl_api_commons.flask.middlewares.request_id import RequestIDService

from dl_core.connections_security.base import InsecureConnectionSecurityManager
from dl_core.flask_utils.aio_event_loop_middleware import AIOEventLoopMiddleware
from dl_core.flask_utils.services_registry_middleware import ServicesRegistryMiddleware
from dl_core.flask_utils.trust_auth import TrustAuthService
from dl_core.flask_utils.us_manager_middleware import USManagerFlaskMiddleware
from dl_api_commons.reporting.registry import DefaultReportingRegistry
from dl_core.services_registry import DefaultServicesRegistry, ServicesRegistry
from dl_core.services_registry.conn_executor_factory import DefaultConnExecutorFactory
from dl_core.services_registry.sr_factories import SRFactory
from dl_core.us_connection_base import ClassicConnectionSQL
from dl_core.utils import FutureRef
from dl_core.mdb_utils import MDBDomainManagerFactory


def test_integration(saved_connection, rqe_config_subprocess, caplog, core_test_config):
    caplog.set_level("DEBUG")
    app = flask.Flask(__name__)
    ContextVarMiddleware().wrap_flask_app(app)
    AIOEventLoopMiddleware().wrap_flask_app(app)
    us_config = core_test_config.get_us_config()

    # TODO CONSIDER: Use default SR factory
    class TestSRFactory(SRFactory):
        def make_service_registry(self, request_context_info, caches_redis_client_factory=None) -> ServicesRegistry:
            sr_fut_ref: FutureRef[DefaultServicesRegistry] = FutureRef()
            sr = DefaultServicesRegistry(
                rci=request_context_info,
                conn_exec_factory=DefaultConnExecutorFactory(
                    async_env=False,
                    services_registry_ref=sr_fut_ref,
                    conn_sec_mgr=InsecureConnectionSecurityManager(),
                    mdb_mgr=MDBDomainManagerFactory().get_manager(),
                    rqe_config=rqe_config_subprocess,
                    tpe=None,
                ),
                reporting_registry=DefaultReportingRegistry(rci=request_context_info),
                mutations_cache_factory=None,
                mdb_domain_manager_factory=MDBDomainManagerFactory()
            )
            sr_fut_ref.fulfill(sr)
            return sr

    RequestLoggingContextControllerMiddleWare().set_up(app)
    RequestIDService(
        request_id_app_prefix=None,
        append_local_req_id=False
    ).set_up(app)
    TrustAuthService(
        fake_user_id='_the_tests_syncapp_user_id_',
        fake_user_name='_the_tests_syncapp_user_name_',
    ).set_up(app)
    ReqCtxInfoMiddleware().set_up(app)
    ServicesRegistryMiddleware(
        services_registry_factory=TestSRFactory(),
    ).set_up(app)
    USManagerFlaskMiddleware(
        us_auth_mode=USAuthMode.master,
        us_master_token=us_config.us_master_token,
        crypto_keys_config=core_test_config.get_crypto_keys_config(),
        us_base_url=us_config.us_host,
    ).set_up(app)

    @app.route("/conn/<conn_uuid>/test")
    def test_connection(conn_uuid):
        service_registry = ServicesRegistryMiddleware.get_request_services_registry()
        ce_factory = service_registry.get_conn_executor_factory()
        conn = USManagerFlaskMiddleware.get_request_us_manager().get_by_id(conn_uuid, ClassicConnectionSQL)
        conn.test(conn_executor_factory=ce_factory.get_sync_conn_executor)
        return flask.jsonify({})

    client = app.test_client()

    resp = client.get(f"/conn/{saved_connection.uuid}/test")
    assert resp.status_code == 200
