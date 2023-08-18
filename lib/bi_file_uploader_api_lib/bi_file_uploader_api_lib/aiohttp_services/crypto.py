from typing import ClassVar

import attr
from aiohttp import web

from bi_configs.crypto_keys import CryptoKeysConfig


@attr.s
class CryptoService:
    APP_KEY: ClassVar[str] = 'CRYPTO'

    crypto_keys_config: CryptoKeysConfig = attr.ib(repr=False)

    def bind_to_app(self, app: web.Application) -> None:
        app[self.APP_KEY] = self

    @classmethod
    def get_config_for_app(cls, app: web.Application) -> CryptoKeysConfig:
        crypto_service = app[cls.APP_KEY]
        assert isinstance(crypto_service, CryptoService)
        return crypto_service.crypto_keys_config
