from __future__ import annotations

import dataclasses
import logging
import ssl
from typing import Optional

import flask

from dl_api_lib.app.control_api.app import (
    ControlApiAppFactory,
    EnvSetupResult,
)
from dl_api_lib.app_common import (
    SRFactoryBuilder,
    StandaloneServiceRegistryFactory,
)
from dl_api_lib.app_settings import (
    ControlApiAppSettingsOS,
    ControlApiAppTestingsSettings,
    NativeAuthSettingsOS,
    NullAuthSettingsOS,
    ZitadelAuthSettingsOS,
)
from dl_api_lib.connector_availability.base import ConnectorAvailabilityConfig
from dl_cache_engine.primitives import CacheTTLConfig
from dl_configs.enums import RequiredService
from dl_configs.utils import get_multiple_root_certificates
from dl_constants.enums import USAuthMode
from dl_core.services_registry.entity_checker import EntityUsageChecker
from dl_core.services_registry.env_manager_factory import InsecureEnvManagerFactory
from dl_core.services_registry.env_manager_factory_base import EnvManagerFactory
from dl_core.services_registry.rqe_caches import RQECachesSetting


LOGGER = logging.getLogger(__name__)


@dataclasses.dataclass
class AuthSetupResult:
    us_auth_mode: USAuthMode


class StandaloneControlApiSRFactoryBuilder(
    SRFactoryBuilder[ControlApiAppSettingsOS]  # type: ignore # ControlApiAppSettingsOS is not subtype of AppSettings due to migration to new settings
):
    def _get_required_services(self, settings: ControlApiAppSettingsOS) -> set[RequiredService]:
        return set()

    def _get_env_manager_factory(self, settings: ControlApiAppSettingsOS) -> EnvManagerFactory:
        return InsecureEnvManagerFactory()

    def _get_inst_specific_sr_factory(
        self,
        settings: ControlApiAppSettingsOS,
        ca_data: bytes,
    ) -> StandaloneServiceRegistryFactory:
        return StandaloneServiceRegistryFactory()

    def _get_entity_usage_checker(self, settings: ControlApiAppSettingsOS) -> Optional[EntityUsageChecker]:
        return None

    def _get_bleeding_edge_users(self, settings: ControlApiAppSettingsOS) -> tuple[str, ...]:
        return tuple()

    def _get_rqe_caches_settings(self, settings: ControlApiAppSettingsOS) -> Optional[RQECachesSetting]:
        return None

    def _get_default_cache_ttl_settings(self, settings: ControlApiAppSettingsOS) -> Optional[CacheTTLConfig]:
        return None

    def _get_connector_availability(self, settings: ControlApiAppSettingsOS) -> Optional[ConnectorAvailabilityConfig]:
        return settings.CONNECTOR_AVAILABILITY


class StandaloneControlApiAppFactory(
    ControlApiAppFactory[ControlApiAppSettingsOS],  # type: ignore # ControlApiAppSettingsOS is not subtype of AppSettings
    StandaloneControlApiSRFactoryBuilder,
):
    def set_up_environment(
        self,
        app: flask.Flask,
        testing_app_settings: Optional[ControlApiAppTestingsSettings] = None,
    ) -> EnvSetupResult:
        auth_setup_result = self._setup_auth_middleware(app=app)

        return EnvSetupResult(us_auth_mode=auth_setup_result.us_auth_mode)

    def _setup_auth_middleware(
        self,
        app: flask.Flask,
        testing_app_settings: Optional[ControlApiAppTestingsSettings] = None,
    ) -> AuthSetupResult:
        self._settings: ControlApiAppSettingsOS
        settings = self._settings.AUTH

        if settings is None or isinstance(settings, NullAuthSettingsOS):
            return self._setup_auth_middleware_none(app=app, testing_app_settings=testing_app_settings)

        if isinstance(settings, ZitadelAuthSettingsOS):
            return self._setup_auth_middleware_zitadel(
                settings=settings,
                cadata=get_multiple_root_certificates(
                    self._settings.CA_FILE_PATH,
                    *self._settings.EXTRA_CA_FILE_PATHS,
                ),
                app=app,
            )

        if isinstance(settings, NativeAuthSettingsOS):
            return self._setup_auth_middleware_native(settings=settings, app=app)

        raise ValueError(f"Unknown auth type: {settings.type}")

    def _setup_auth_middleware_none(
        self,
        app: flask.Flask,
        testing_app_settings: Optional[ControlApiAppTestingsSettings] = None,
    ) -> AuthSetupResult:
        from dl_api_commons.flask.middlewares.trust_auth import TrustAuthService

        TrustAuthService(
            fake_user_id="_user_id_",
            fake_user_name="_user_name_",
            fake_tenant=None if testing_app_settings is None else testing_app_settings.fake_tenant,
        ).set_up(app)

        us_auth_mode_override = None if testing_app_settings is None else testing_app_settings.us_auth_mode_override
        us_auth_mode = USAuthMode.master if us_auth_mode_override is None else us_auth_mode_override

        return AuthSetupResult(us_auth_mode=us_auth_mode)

    def _setup_auth_middleware_zitadel(
        self,
        settings: ZitadelAuthSettingsOS,
        cadata: bytes,
        app: flask.Flask,
    ) -> AuthSetupResult:
        import httpx

        import dl_zitadel

        httpx_client = httpx.Client(
            verify=ssl.create_default_context(cadata=cadata.decode("ascii")),
        )
        zitadel_client = dl_zitadel.ZitadelSyncClient(
            base_client=httpx_client,
            base_url=settings.BASE_URL,
            project_id=settings.PROJECT_ID,
            client_id=settings.CLIENT_ID,
            client_secret=settings.CLIENT_SECRET,
            app_client_id=settings.APP_CLIENT_ID,
            app_client_secret=settings.APP_CLIENT_SECRET,
        )
        token_storage = dl_zitadel.ZitadelSyncTokenStorage(
            client=zitadel_client,
        )

        dl_zitadel.FlaskMiddleware(
            client=zitadel_client,
            token_storage=token_storage,
        ).set_up(app=app)
        LOGGER.info("Zitadel auth setup complete")

        return AuthSetupResult(us_auth_mode=USAuthMode.regular)

    def _setup_auth_middleware_native(
        self,
        settings: NativeAuthSettingsOS,
        app: flask.Flask,
    ) -> AuthSetupResult:
        assert isinstance(settings, NativeAuthSettingsOS)

        import dl_auth_native

        dl_auth_native.FlaskMiddleware.from_settings(
            settings=dl_auth_native.MiddlewareSettings(
                decoder_key=settings.JWT_KEY,
                decoder_algorithms=[settings.JWT_ALGORITHM],
            )
        ).set_up(app=app)
        LOGGER.info("Native auth setup complete")

        return AuthSetupResult(us_auth_mode=USAuthMode.regular)
