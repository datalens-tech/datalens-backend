import logging
from typing import (
    Any,
    ClassVar,
)

from aiobotocore.client import AioBaseClient
from aiobotocore.session import get_session
from aiohttp import web
import attr
import botocore.client
import botocore.session

LOGGER = logging.getLogger(__name__)


@attr.s
class S3Service:
    APP_KEY: ClassVar[str] = "S3_SERVICE"

    _access_key_id: str = attr.ib(repr=False)
    _secret_access_key: str = attr.ib(repr=False)
    _endpoint_url: str = attr.ib()

    tmp_bucket_name: str = attr.ib()
    persistent_bucket_name: str = attr.ib()

    client: AioBaseClient = attr.ib(init=False, repr=False, hash=False, cmp=False)
    _client_init_params: dict[str, Any] = attr.ib(init=False, repr=False, hash=False, cmp=False)

    @classmethod
    def get_full_app_key(cls) -> str:
        return cls.APP_KEY

    async def init_hook(self, target_app: web.Application) -> None:
        target_app[self.APP_KEY] = self
        await self.initialize()

    async def tear_down_hook(self, target_app: web.Application) -> None:
        await self.tear_down()

    async def initialize(self) -> None:
        LOGGER.info("Initializing S3 service")
        self._client_init_params = dict(
            service_name="s3",
            aws_access_key_id=self._access_key_id,
            aws_secret_access_key=self._secret_access_key,
            endpoint_url=self._endpoint_url,
        )

        session = get_session()
        client = await session._create_client(**self._client_init_params)  # noqa
        self.client = await client.__aenter__()

    async def tear_down(self) -> None:
        LOGGER.info("Tear down S3 service")
        await self.client.close()

    @classmethod
    def get_app_instance(cls, app: web.Application) -> "S3Service":
        service = app.get(cls.APP_KEY, None)
        if service is None:
            raise ValueError("S3BucketService was not initiated for application")

        return service

    def get_client(self) -> AioBaseClient:
        return self.client

    def get_sync_client(self) -> botocore.client.BaseClient:
        session = botocore.session.get_session()
        client = session.create_client(**self._client_init_params)
        return client
