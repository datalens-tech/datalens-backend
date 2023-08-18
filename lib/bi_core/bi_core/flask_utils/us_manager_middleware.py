from __future__ import annotations

import logging
from typing import Optional

import attr
import flask

from bi_api_commons.flask.middlewares.commit_rci_middleware import ReqCtxInfoMiddleware
from bi_configs.crypto_keys import CryptoKeysConfig
from bi_constants.enums import USAuthMode
from bi_core.flask_utils.services_registry_middleware import ServicesRegistryMiddleware
from bi_core.us_manager.factory import USMFactory
from bi_core.us_manager.us_manager_sync import SyncUSManager

LOGGER = logging.getLogger(__name__)


# TODO FIX: Add tests
@attr.s
class USManagerFlaskMiddleware:
    us_base_url: str = attr.ib()
    us_auth_mode: USAuthMode = attr.ib(
        validator=attr.validators.in_((
            USAuthMode.regular,
            USAuthMode.master,
        ))
    )
    crypto_keys_config: CryptoKeysConfig = attr.ib(repr=False)
    us_master_token: Optional[str] = attr.ib(default=None, repr=False)
    us_public_token: Optional[str] = attr.ib(default=None, repr=False)

    _usm_factory: USMFactory = attr.ib(init=False, default=None)

    def __attrs_post_init__(self) -> None:
        self._usm_factory = USMFactory(
            us_base_url=self.us_base_url,
            crypto_keys_config=self.crypto_keys_config,
            us_master_token=self.us_master_token,
            us_public_token=self.us_public_token,
        )

    def bind_us_managers_to_request(self) -> None:
        bi_context = ReqCtxInfoMiddleware.get_request_context_info()
        services_registry = (
            ServicesRegistryMiddleware.get_request_services_registry()
            if ServicesRegistryMiddleware.enabled_for_request() else None
        )
        assert services_registry is not None, 'Service registry must be enabled!'

        if bi_context.user_id is None:
            LOGGER.info("User US manager will not be created due to no user info in RCI")
        else:
            LOGGER.info("Creating user US manager")
            usm: SyncUSManager

            if self.us_auth_mode == USAuthMode.regular:
                usm = self._usm_factory.get_regular_sync_usm(rci=bi_context, services_registry=services_registry)
            elif self.us_auth_mode == USAuthMode.master:
                usm = self._usm_factory.get_master_sync_usm(rci=bi_context, services_registry=services_registry)
            else:
                raise AssertionError(f"Mode is not supported by USManagerFlaskMiddleware: {self.us_auth_mode!r}")

            flask.g.us_manager = usm

        if self.us_master_token is not None:
            LOGGER.info("Creating service US manager")
            flask.g.service_us_manager = self._usm_factory.get_master_sync_usm(
                rci=bi_context,
                services_registry=services_registry,
            )
        else:
            LOGGER.info("US master token was not provided. Service USM will not be created")
            flask.g.service_us_manager = None

    def set_up(self, app: flask.Flask) -> USManagerFlaskMiddleware:
        app.before_request(self.bind_us_managers_to_request)
        return self

    def run(self):  # type: ignore  # TODO: fix
        return self.bind_us_managers_to_request()

    @classmethod
    def get_request_us_manager(cls) -> SyncUSManager:
        return flask.g.us_manager

    @classmethod
    def get_request_service_us_manager(cls) -> SyncUSManager:
        usm = flask.g.service_us_manager
        if usm is not None:
            return flask.g.service_us_manager
        else:
            raise ValueError("Service USM was not created for current request")
