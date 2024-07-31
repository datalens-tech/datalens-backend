from __future__ import annotations

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


class StandaloneControlApiSRFactoryBuilder(SRFactoryBuilder[ControlApiAppSettingsOS]):
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
    ControlApiAppFactory[ControlApiAppSettingsOS], StandaloneControlApiSRFactoryBuilder
):
    def set_up_environment(
        self,
        app: flask.Flask,
        testing_app_settings: Optional[ControlApiAppTestingsSettings] = None,
    ) -> EnvSetupResult:
        us_auth_mode: USAuthMode

        us_auth_mode = USAuthMode.regular
        auth_setup = self._setup_auth_middleware(app=app)

        if not auth_setup:
            from dl_api_commons.flask.middlewares.trust_auth import TrustAuthService

            TrustAuthService(
                fake_user_id="_user_id_",
                fake_user_name="_user_name_",
                fake_tenant=None if testing_app_settings is None else testing_app_settings.fake_tenant,
            ).set_up(app)

            us_auth_mode_override = None if testing_app_settings is None else testing_app_settings.us_auth_mode_override
            us_auth_mode = USAuthMode.master if us_auth_mode_override is None else us_auth_mode_override

        result = EnvSetupResult(us_auth_mode=us_auth_mode)
        return result

    def _setup_auth_middleware(self, app: flask.Flask) -> bool:
        self._settings: ControlApiAppSettingsOS

        if self._settings.AUTH is None:
            LOGGER.warning("No auth settings found, continuing without auth setup")
            return False

        # TODO: Add support for other auth types
        assert self._settings.AUTH.TYPE == "ZITADEL"
        import httpx

        import dl_zitadel

        ca_data = get_multiple_root_certificates(
            self._settings.CA_FILE_PATH,
            *self._settings.EXTRA_CA_FILE_PATHS,
        )

        zitadel_client = dl_zitadel.ZitadelSyncClient(
            base_client=httpx.Client(
                verify=ssl.create_default_context(cadata=ca_data.decode("ascii")),
            ),
            base_url=self._settings.AUTH.BASE_URL,
            project_id=self._settings.AUTH.PROJECT_ID,
            client_id=self._settings.AUTH.CLIENT_ID,
            client_secret=self._settings.AUTH.CLIENT_SECRET,
            app_client_id=self._settings.AUTH.APP_CLIENT_ID,
            app_client_secret=self._settings.AUTH.APP_CLIENT_SECRET,
        )
        token_storage = dl_zitadel.ZitadelSyncTokenStorage(
            client=zitadel_client,
        )

        dl_zitadel.FlaskMiddleware(
            client=zitadel_client,
            token_storage=token_storage,
        ).set_up(app=app)
        LOGGER.info("Zitadel auth setup complete")
        return True
