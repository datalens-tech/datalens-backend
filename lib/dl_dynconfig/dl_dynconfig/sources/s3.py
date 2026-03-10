import contextlib
import datetime
import logging
from typing import (
    Any,
    AsyncGenerator,
    Literal,
    Protocol,
    TypeVar,
)

import aiobotocore.session
import attrs
import pydantic
from typing_extensions import Self
import yaml

import dl_dynconfig.sources.base as base
import dl_dynconfig.sources.cached as cached
import dl_dynconfig.utils.types as types_utils
import dl_settings


T = TypeVar("T")

LOGGER = logging.getLogger(__name__)


class BaseS3SourceSettings(dl_settings.BaseSettings):
    ENDPOINT_URL: str
    ACCESS_KEY_ID: str = pydantic.Field(repr=False)
    SECRET_ACCESS_KEY: str = pydantic.Field(repr=False)
    BUCKET: str
    KEY: str


class S3AuthProviderProtocol(Protocol):
    async def get_access_key_id(self) -> str:
        ...

    async def get_secret_access_key(self) -> str:
        ...

    async def get_session_token(self) -> str | None:
        ...


@attrs.define(kw_only=True)
class DefaultS3AuthProvider:
    _access_key_id: str
    _secret_access_key: str

    async def get_access_key_id(self) -> str:
        return self._access_key_id

    async def get_secret_access_key(self) -> str:
        return self._secret_access_key

    async def get_session_token(self) -> str | None:
        return None


@attrs.define(kw_only=True)
class BaseS3Source(base.BaseSource):
    _s3_session: aiobotocore.session.AioSession
    _endpoint_url: str
    _auth_provider: S3AuthProviderProtocol
    _bucket: str
    _key: str

    @contextlib.asynccontextmanager
    async def _s3_client(self) -> AsyncGenerator[types_utils.AiobotocoreS3Client, None]:
        async with self._s3_session.create_client(
            service_name="s3",
            aws_access_key_id=await self._auth_provider.get_access_key_id(),
            aws_secret_access_key=await self._auth_provider.get_secret_access_key(),
            aws_session_token=await self._auth_provider.get_session_token(),
            endpoint_url=self._endpoint_url,
        ) as s3_client:
            yield s3_client

    async def fetch(self) -> Any:
        async with self._s3_client() as s3_client:
            response = await s3_client.get_object(Bucket=self._bucket, Key=self._key)
        body = await response["Body"].read()
        return yaml.safe_load(body)

    async def store(self, value: T) -> T:
        body = yaml.dump(value).encode()
        async with self._s3_client() as s3_client:
            await s3_client.put_object(Bucket=self._bucket, Key=self._key, Body=body)

        return value

    async def check_readiness(self) -> bool:
        try:
            await self.fetch()
            return True
        except Exception:
            LOGGER.exception("Failed to check readiness of S3 source")
            return False


class S3SourceSettings(base.BaseSourceSettings, BaseS3SourceSettings):
    type: Literal["s3"] = pydantic.Field(alias="TYPE", default="s3")


base.BaseSourceSettings.register("s3", S3SourceSettings)


@attrs.define(kw_only=True)
class S3Source(BaseS3Source):
    @classmethod
    def from_settings(cls, settings: S3SourceSettings) -> Self:
        return cls(
            s3_session=aiobotocore.session.get_session(),
            endpoint_url=settings.ENDPOINT_URL,
            auth_provider=DefaultS3AuthProvider(
                access_key_id=settings.ACCESS_KEY_ID,
                secret_access_key=settings.SECRET_ACCESS_KEY,
            ),
            bucket=settings.BUCKET,
            key=settings.KEY,
        )


class CachedS3SourceSettings(base.BaseSourceSettings, BaseS3SourceSettings):
    type: Literal["cached_s3"] = pydantic.Field(alias="TYPE", default="cached_s3")
    TTL: datetime.timedelta = datetime.timedelta(minutes=5)


base.BaseSourceSettings.register("cached_s3", CachedS3SourceSettings)


@attrs.define(kw_only=True)
class CachedS3Source(cached.CachedSource):
    @classmethod
    def from_settings(cls, settings: CachedS3SourceSettings) -> Self:
        return cls(
            source=S3Source(
                s3_session=aiobotocore.session.get_session(),
                endpoint_url=settings.ENDPOINT_URL,
                auth_provider=DefaultS3AuthProvider(
                    access_key_id=settings.ACCESS_KEY_ID,
                    secret_access_key=settings.SECRET_ACCESS_KEY,
                ),
                bucket=settings.BUCKET,
                key=settings.KEY,
            ),
            ttl=settings.TTL,
        )
