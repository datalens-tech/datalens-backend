from __future__ import annotations

import logging

import attr
import flask

from dl_api_commons.base_models import RequestContextInfo
from dl_api_commons.flask.middlewares.commit_rci_middleware import ReqCtxInfoMiddleware
from dl_api_commons.flask.required_resources import (
    RequiredResourceCommon,
    get_required_resources,
)
from dl_configs.crypto_keys import CryptoKeysConfig
from dl_constants.enums import USAuthMode
from dl_core.flask_utils.services_registry_middleware import ServicesRegistryMiddleware
from dl_core.services_registry import ServicesRegistry
from dl_core.us_manager.factory import USMFactory
from dl_core.us_manager.us_manager_sync import SyncUSManager
import dl_retrier


LOGGER = logging.getLogger(__name__)


# TODO FIX: Add tests
@attr.s
class USManagerFlaskMiddleware:
    us_base_url: str = attr.ib()
    us_auth_mode: USAuthMode = attr.ib(
        validator=attr.validators.in_(
            (
                USAuthMode.regular,
                USAuthMode.master,
            )
        )
    )
    crypto_keys_config: CryptoKeysConfig = attr.ib(repr=False)
    ca_data: bytes = attr.ib()
    retry_policy_factory: dl_retrier.BaseRetryPolicyFactory = attr.ib()
    us_master_token: str | None = attr.ib(default=None, repr=False)
    us_public_token: str | None = attr.ib(default=None, repr=False)

    _usm_factory: USMFactory = attr.ib(init=False, default=None)

    def __attrs_post_init__(self) -> None:
        self._usm_factory = USMFactory(
            us_base_url=self.us_base_url,
            crypto_keys_config=self.crypto_keys_config,
            us_master_token=self.us_master_token,
            us_public_token=self.us_public_token,
            ca_data=self.ca_data,
            retry_policy_factory=self.retry_policy_factory,
        )

    def _create_regular_us_manager(
        self,
        rci: RequestContextInfo,
        services_registry: ServicesRegistry,
        required_resources: frozenset[RequiredResourceCommon],
    ) -> SyncUSManager | None:
        if rci.user_id is None:
            LOGGER.info("User US manager will not be created due to no user info in RCI")
            return None

        if (
            RequiredResourceCommon.US_HEADERS_TOKEN in required_resources
        ):  # DEPRECATED, to be removed after DLPROJECTS-500
            LOGGER.info("User US manager will not be created due to US_HEADERS_TOKEN flag in target view")
            return None

        if RequiredResourceCommon.ONLY_SERVICES_ALLOWED in required_resources:
            LOGGER.info("User US manager will not be created due to ONLY_SERVICES_ALLOWED flag in target view")
            return None

        if self.us_auth_mode == USAuthMode.master:  # TODO: to be removed in BI-6973
            LOGGER.info("Creating user US manager with master auth mode")
            return self._usm_factory.get_master_sync_usm(
                rci=rci,
                services_registry=services_registry,
            )

        LOGGER.info("Creating user US manager with regular auth mode")
        return self._usm_factory.get_regular_sync_usm(
            rci=rci,
            services_registry=services_registry,
        )

    def _create_private_us_manager(
        self,
        rci: RequestContextInfo,
        services_registry: ServicesRegistry,
        required_resources: frozenset[RequiredResourceCommon],
    ) -> SyncUSManager | None:
        if (
            RequiredResourceCommon.US_HEADERS_TOKEN in required_resources
        ):  # DEPRECATED, to be removed after DLPROJECTS-500
            LOGGER.info("Creating service US manager with master token from request headers")
            return self._usm_factory.get_master_sync_usm(
                rci=rci,
                services_registry=services_registry,
                is_token_stored=False,
            )

        if (
            RequiredResourceCommon.ONLY_SERVICES_ALLOWED in required_resources
            or RequiredResourceCommon.PRIVATE_US_MANAGER in required_resources
        ):
            LOGGER.info("Creating service US manager")
            return self._usm_factory.get_master_sync_usm(
                rci=rci,
                services_registry=services_registry,
            )

        LOGGER.info("Service US manager will not be created")
        return None

    def bind_us_managers_to_request(self) -> None:
        rci = ReqCtxInfoMiddleware.get_request_context_info()
        assert ServicesRegistryMiddleware.enabled_for_request(), "Services registry must be enabled!"
        services_registry = ServicesRegistryMiddleware.get_request_services_registry()
        required_resources = get_required_resources()

        flask.g.regular_us_manager = self._create_regular_us_manager(rci, services_registry, required_resources)
        flask.g.private_us_manager = self._create_private_us_manager(rci, services_registry, required_resources)

    def set_up(self, app: flask.Flask) -> USManagerFlaskMiddleware:
        app.before_request(self.bind_us_managers_to_request)
        return self

    def run(self) -> None:
        self.bind_us_managers_to_request()

    @classmethod
    def get_request_regular_us_manager(cls) -> SyncUSManager:
        usm = flask.g.regular_us_manager
        if usm is None:
            raise ValueError("Regular USM was not created for current request")

        return usm

    @classmethod
    def get_request_private_us_manager(cls) -> SyncUSManager:
        usm = flask.g.private_us_manager
        if usm is None:
            raise ValueError("Private USM was not created for current request")

        return usm
