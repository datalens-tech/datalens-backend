import contextlib
import datetime
import logging
from typing import (
    Any,
    AsyncGenerator,
    Literal,
    TypeVar,
)

import aiobotocore.session
import attrs
import pydantic
from typing_extensions import Self
import yaml

import dl_dynconfig.sources.base as base
import dl_dynconfig.utils.types as types_utils
import dl_settings


T = TypeVar("T")
Unset = object()

LOGGER = logging.getLogger(__name__)


class _BaseS3SourceSettings(dl_settings.BaseSettings):
    ENDPOINT_URL: str
    ACCESS_KEY_ID: str = pydantic.Field(repr=False)
    SECRET_ACCESS_KEY: str = pydantic.Field(repr=False)
    BUCKET: str
    KEY: str


@attrs.define(kw_only=True)
class _BaseS3Source(base.Source):
    _s3_session: aiobotocore.session.AioSession
    _endpoint_url: str
    _access_key_id: str
    _secret_access_key: str
    _bucket: str
    _key: str

    @contextlib.asynccontextmanager
    async def _s3_client(self) -> AsyncGenerator[types_utils.AiobotocoreS3Client, None]:
        async with self._s3_session.create_client(
            service_name="s3",
            aws_access_key_id=self._access_key_id,
            aws_secret_access_key=self._secret_access_key,
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


class S3SourceSettings(_BaseS3SourceSettings):
    TYPE: Literal["s3"] = "s3"


@attrs.define(kw_only=True)
class S3Source(_BaseS3Source):
    @classmethod
    def from_settings(cls, settings: S3SourceSettings) -> Self:
        return cls(
            s3_session=aiobotocore.session.get_session(),
            endpoint_url=settings.ENDPOINT_URL,
            access_key_id=settings.ACCESS_KEY_ID,
            secret_access_key=settings.SECRET_ACCESS_KEY,
            bucket=settings.BUCKET,
            key=settings.KEY,
        )


class CachedS3SourceSettings(_BaseS3SourceSettings):
    TYPE: Literal["cached_s3"] = "cached_s3"
    TTL: datetime.timedelta = datetime.timedelta(minutes=5)


@attrs.define(kw_only=True)
class CachedS3Source(_BaseS3Source):
    _ttl: datetime.timedelta = datetime.timedelta(minutes=5)
    _cached_data: Any = attrs.field(init=False, default=Unset)
    _cached_at: datetime.datetime | None = attrs.field(init=False, default=None)

    @classmethod
    def from_settings(cls, settings: CachedS3SourceSettings) -> Self:
        return cls(
            s3_session=aiobotocore.session.get_session(),
            endpoint_url=settings.ENDPOINT_URL,
            access_key_id=settings.ACCESS_KEY_ID,
            secret_access_key=settings.SECRET_ACCESS_KEY,
            bucket=settings.BUCKET,
            key=settings.KEY,
            ttl=settings.TTL,
        )

    @property
    def _is_expired(self) -> bool:
        return (
            self._cached_data is Unset
            or self._cached_at is None
            or self._cached_at + self._ttl < datetime.datetime.now()
        )

    def _update_cache(self, data: T) -> T:
        self._cached_data = data
        self._cached_at = datetime.datetime.now()
        return data

    async def fetch(self) -> Any:
        if not self._is_expired:
            return self._cached_data

        data = await super().fetch()
        return self._update_cache(data)

    async def store(self, value: T) -> T:
        value = await super().store(value)
        return self._update_cache(value)

    async def check_readiness(self) -> bool:
        if not self._is_expired:
            return True

        return await super().check_readiness()
