from __future__ import annotations

import logging
from typing import (
    TYPE_CHECKING,
    Optional,
)

import attr
import flask

from bi_api_commons.flask.middlewares.commit_rci_middleware import ReqCtxInfoMiddleware

from ..services_registry.sr_factories import SRFactory

if TYPE_CHECKING:
    from bi_core.services_registry import ServicesRegistry  # noqa


LOGGER = logging.getLogger(__name__)


class NoServiceRegistryForRequest(Exception):
    pass


@attr.s
class ServicesRegistryMiddleware:
    services_registry_factory: SRFactory = attr.ib()

    def set_up(self, app: flask.Flask) -> None:
        app.before_request(self.bind_services_registry_to_request)
        app.teardown_request(self.cleanup_request_services_registry)

    def cleanup_request_services_registry(self, _: Optional[BaseException] = None) -> None:
        try:
            services_registry = self.get_request_services_registry()
        except NoServiceRegistryForRequest:
            LOGGER.debug("Service registry was not created. Close procedure does not required.")
        else:
            try:
                LOGGER.info("Closing services registry...")
                services_registry.close()
            except Exception:  # noqa
                LOGGER.exception("Error during services registry cleanup")
            LOGGER.info("Closed services registry.")

    def bind_services_registry_to_request(self) -> None:
        rci = ReqCtxInfoMiddleware.get_request_context_info()
        flask.g.bi_services_registry = self.services_registry_factory.make_service_registry(rci)

    @classmethod
    def get_request_services_registry(cls) -> "ServicesRegistry":
        try:
            services_registry = flask.g.bi_services_registry
        except AttributeError:
            raise NoServiceRegistryForRequest()
        else:
            if services_registry is None:
                raise NoServiceRegistryForRequest()
            return services_registry

    @classmethod
    def enabled_for_request(cls) -> bool:
        return "bi_services_registry" in flask.g
