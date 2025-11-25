import logging
import ssl
from typing import Generic

import pydantic

import dl_app_base.base as base
import dl_app_base.singleton as singleton
import dl_configs
import dl_settings


LOGGER = logging.getLogger(__name__)


class CertificatesSettings(dl_settings.BaseSettings):
    CA_FILE: str = pydantic.Field(default_factory=dl_configs.get_root_certificates_path)


class CertificatesAppSettingsMixin(base.BaseAppSettings):
    CERTIFICATES: CertificatesSettings = pydantic.Field(default_factory=CertificatesSettings)


class CertificatesAppMixin(base.BaseApp):
    ...


class CertificatesAppFactoryMixin(
    base.BaseAppFactory[base.AppType],
    Generic[base.AppType],
):
    settings: CertificatesAppSettingsMixin

    @singleton.singleton_class_method_result
    async def _get_ssl_context(
        self,
    ) -> ssl.SSLContext:
        settings = self.settings.CERTIFICATES

        return ssl.create_default_context(
            cafile=settings.CA_FILE,
        )
