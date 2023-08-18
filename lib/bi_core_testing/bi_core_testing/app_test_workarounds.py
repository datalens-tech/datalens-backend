from __future__ import annotations

from bi_api_commons.base_models import RequestContextInfo
from bi_core.connections_security.base import InsecureConnectionSecurityManager, ConnectionSecurityManager
from bi_core.services_registry.env_manager_factory import DefaultEnvManagerFactory


class TestEnvManagerFactory(DefaultEnvManagerFactory):
    def make_security_manager(self, request_context_info: RequestContextInfo) -> ConnectionSecurityManager:
        return InsecureConnectionSecurityManager()
