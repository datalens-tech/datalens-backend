from __future__ import annotations

from dl_api_commons.base_models import RequestContextInfo
from dl_core.connections_security.base import (
    ConnectionSecurityManager,
    InsecureConnectionSecurityManager,
)
from dl_core.services_registry.env_manager_factory import DefaultEnvManagerFactory


class TestEnvManagerFactory(DefaultEnvManagerFactory):
    def make_security_manager(self, request_context_info: RequestContextInfo) -> ConnectionSecurityManager:
        return InsecureConnectionSecurityManager()
