from typing import ClassVar

import aiohttp.web
import attrs
import typing_extensions

import dl_us_entries_client


@attrs.define(kw_only=True)
class USEntriesClientService:
    APP_KEY: ClassVar[str] = "US_CLIENT_SERVICE"

    _us_entries_client: dl_us_entries_client.USEntriesAsyncClient

    @property
    def client(self) -> dl_us_entries_client.USEntriesAsyncClient:
        return self._us_entries_client

    @classmethod
    def get_full_app_key(cls) -> str:
        return cls.APP_KEY

    @classmethod
    def get_app_instance(cls, app: aiohttp.web.Application) -> typing_extensions.Self:
        service = app.get(cls.APP_KEY, None)
        if service is None:
            raise ValueError(f"{cls.__name__} was not initiated for application")

        assert isinstance(service, cls)
        return service

    async def init_hook(self, target_app: aiohttp.web.Application) -> None:
        target_app[self.APP_KEY] = self

    async def tear_down_hook(self, target_app: aiohttp.web.Application) -> None:
        await self._us_entries_client.close()
