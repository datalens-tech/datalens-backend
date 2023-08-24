import logging
from concurrent import futures
from contextlib import contextmanager
from typing import (
    Optional,
    TypeVar,
    Generator,
)

import attr
import grpc
from google.protobuf.timestamp_pb2 import Timestamp  # noqa

from bi_configs.settings_loaders.fallback_cfg_resolver import YEnvFallbackConfigResolver
from bi_configs.settings_loaders.loader_env import load_settings_from_env_with_fallback_legacy
from bi_core.logging_config import configure_logging
from bi_defaults.environments import InstallationsMap, EnvAliasesMap
from bi_external_api.enums import ExtAPIType
from bi_external_api.grpc_proxy.common_interceptor import RequestBootstrapInterceptor
from bi_external_api.grpc_proxy.ext_api_client import ExtApiClient
from bi_external_api.grpc_proxy.workbook_service import WorkbookService
from bi_external_api.settings import GrpcProxySettings
from bi_external_api.utils import init_sentry
from doublecloud.visualization.v1 import workbook_service_pb2 as ws
from doublecloud.visualization.v1 import workbook_service_pb2_grpc as ws_grpc

try:
    from src.proto.grpc.health.v1 import health_pb2  # type: ignore
    from src.proto.grpc.health.v1 import health_pb2_grpc  # type: ignore
except ImportError:
    from grpc_health.v1 import health_pb2  # type: ignore
    from grpc_health.v1 import health_pb2_grpc  # type: ignore

from grpc_reflection.v1alpha import reflection

LOGGER = logging.getLogger(__name__)


class GrpcProxyHealthServicer(health_pb2_grpc.HealthServicer):
    def Check(
            self,
            request: health_pb2.HealthCheckRequest,  # type: ignore
            context: grpc.ServicerContext,
    ) -> health_pb2.HealthCheckResponse:  # type: ignore
        return health_pb2.HealthCheckResponse(status=health_pb2.HealthCheckResponse.SERVING)  # type: ignore


@attr.s
class GrpcProxyProvider:
    max_workers: Optional[int] = attr.ib(default=10)

    def create_threadpool_grpc_server(self, ext_api_client: ExtApiClient) -> grpc.Server:
        server = grpc.server(
            futures.ThreadPoolExecutor(max_workers=self.max_workers),
            interceptors=(RequestBootstrapInterceptor(),),
        )
        ws_grpc.add_WorkbookServiceServicer_to_server(WorkbookService(ext_api_client), server)

        health_servicer = GrpcProxyHealthServicer()
        health_pb2_grpc.add_HealthServicer_to_server(health_servicer, server)

        service_names = (
            ws.DESCRIPTOR.services_by_name["WorkbookService"].full_name,
            health_pb2.DESCRIPTOR.services_by_name["Health"].full_name,
            reflection.SERVICE_NAME,
        )
        reflection.enable_server_reflection(service_names, server)

        return server


_SERVER_TV = TypeVar("_SERVER_TV", bound=grpc.Server)


@contextmanager
def serve_grpc_server(
        server: _SERVER_TV,
        host: str,
        port: int,
        server_credentials: Optional[grpc.ServerCredentials] = None,
        timeout: Optional[float] = None,
) -> Generator[_SERVER_TV, None, None]:
    if server_credentials is None:
        server.add_insecure_port(f"{host}:{port}")
    else:
        server.add_secure_port(f"{host}:{port}", server_credentials)

    server.start()
    yield server
    server.wait_for_termination(timeout=timeout)


def run_from_settings(settings: GrpcProxySettings) -> None:
    init_sentry(settings.SENTRY_DSN)
    configure_logging(
        app_name='bi_external_api_grpc_proxy',
    )

    ext_api_client = ExtApiClient(
        base_url=settings.TARGET_ENDPOINT,
        ext_api_type=ExtAPIType.DC,
    )
    server = GrpcProxyProvider(
        max_workers=settings.WORKERS_NUM
    ).create_threadpool_grpc_server(ext_api_client=ext_api_client)
    host = settings.BIND_HOST
    port = int(settings.BIND_PORT)
    with serve_grpc_server(server, host, port):
        LOGGER.info(f"Started grpc proxy service on {host}:{port}")


def main() -> None:
    fallback_resolver = YEnvFallbackConfigResolver(
        installation_map=InstallationsMap,
        env_map=EnvAliasesMap,
    )
    settings = load_settings_from_env_with_fallback_legacy(
        GrpcProxySettings,
        fallback_cfg_resolver=fallback_resolver,
    )
    run_from_settings(settings)


if __name__ == "__main__":
    main()
