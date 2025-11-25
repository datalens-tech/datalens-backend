import logging

import attrs
import pytest
from typing_extensions import override

import dl_app_base
import dl_testing


LOGGER = logging.getLogger(__name__)


@pytest.mark.asyncio
async def test_default(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("CERTIFICATES__CA_FILE", dl_testing.get_root_certificates_path())

    class Settings(dl_app_base.CertificatesAppSettingsMixin):
        ...

    @attrs.define(frozen=True, kw_only=True)
    class Application(dl_app_base.CertificatesAppMixin):
        ...

    class Factory(dl_app_base.CertificatesAppFactoryMixin[Application]):
        settings: Settings
        app_class = Application

        @override
        @dl_app_base.singleton_class_method_result
        async def _get_logger(
            self,
        ) -> logging.Logger:
            return LOGGER

        async def assert_dependencies(self) -> None:
            ssl_context = await self._get_ssl_context()
            assert ssl_context is not None

    settings = Settings()
    factory = Factory(settings=settings)

    await factory.assert_dependencies()
