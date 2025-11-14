from __future__ import annotations

import logging
import ssl
from typing import (
    TYPE_CHECKING,
    Any,
    ClassVar,
)

import aiobotocore.httpsession
from aiobotocore.config import AioConfig
from aiobotocore.session import get_session
import aiohttp
import attr
import boto3
from botocore.httpsession import get_cert_path
import typing_extensions


if TYPE_CHECKING:
    from mypy_boto3_s3.client import S3Client as SyncS3Client
    from types_aiobotocore_s3 import S3Client as AsyncS3Client


LOGGER = logging.getLogger(__name__)


class _AIOHTTPSession(aiobotocore.httpsession.AIOHTTPSession):
    def _get_ssl_context(self) -> ssl.SSLContext:
        return self._connector_args["ssl_context"]

    def _create_connector(self, proxy_url) -> TCPConnector:
        ssl_context = None
        if bool(self._verify):
            if proxy_url:
                ssl_context = self._setup_proxy_ssl_context(proxy_url)
                # TODO: add support for
                #    proxies_settings.get('proxy_use_forwarding_for_https')
            else:
                ssl_context = self._get_ssl_context()

            if ssl_context:
                if self._cert_file:
                    ssl_context.load_cert_chain(
                        self._cert_file,
                        self._key_file,
                    )

                # inline self._setup_ssl_cert
                ca_certs = get_cert_path(self._verify)
                if ca_certs:
                    ssl_context.load_verify_locations(ca_certs, None, None)

        self._connector_args.pop("ssl_context", None)
        return aiohttp.TCPConnector(
            limit=self._max_pool_connections,
            ssl=ssl_context or False,
            **self._connector_args,
        )


@attr.s(kw_only=True)
class S3Service:
    APP_KEY: ClassVar[str] = "S3_SERVICE"

    _access_key_id: str = attr.ib(repr=False)
    _secret_access_key: str = attr.ib(repr=False)
    _endpoint_url: str = attr.ib()
    _use_virtual_host_addressing: bool = attr.ib(default=False)
    _ca_data: bytes = attr.ib(repr=False)

    tmp_bucket_name: str = attr.ib()
    persistent_bucket_name: str = attr.ib()

    _client: AsyncS3Client = attr.ib(init=False, repr=False, hash=False, cmp=False)
    _client_init_params: dict[str, Any] = attr.ib(init=False, repr=False, hash=False, cmp=False)

    @property
    def client(self) -> AsyncS3Client:
        return self._client

    @classmethod
    def get_full_app_key(cls) -> str:
        return cls.APP_KEY

    async def init_hook(self, target_app: aiohttp.web.Application) -> None:
        target_app[self.APP_KEY] = self
        await self.initialize()

    async def tear_down_hook(self, target_app: aiohttp.web.Application) -> None:
        await self.tear_down()

    def _init_client_config(self) -> None:
        ssl_context = ssl.create_default_context(cadata=self._ca_data.decode("ascii"))

        self._client_init_params = dict(
            service_name="s3",
            aws_access_key_id=self._access_key_id,
            aws_secret_access_key=self._secret_access_key,
            endpoint_url=self._endpoint_url,
            config=AioConfig(
                http_session_cls=_AIOHTTPSession,
                connector_args=dict(
                    ssl_context=ssl_context,
                ),
                signature_version="s3v4",  # v4 signature is required to generate presigned URLs with restriction policies
                s3={"addressing_style": "virtual" if self._use_virtual_host_addressing else "auto"},
            ),
        )

    async def initialize(self) -> None:
        LOGGER.info("Initializing S3 service")

        self._init_client_config()

        session = get_session()
        client = session.create_client(**self._client_init_params)
        self._client = await client.__aenter__()

    async def tear_down(self) -> None:
        LOGGER.info("Tear down S3 service")
        await self._client.close()

    @classmethod
    def get_app_instance(cls, app: aiohttp.web.Application) -> typing_extensions.Self:
        service = app.get(cls.APP_KEY, None)
        if service is None:
            raise ValueError("S3BucketService was not initiated for application")

        assert isinstance(service, cls)
        return service

    def get_client(self) -> AsyncS3Client:
        return self.client

    def get_sync_client(self) -> SyncS3Client:
        session = boto3.session.Session()
        client = session.client(**self._client_init_params)
        return client
