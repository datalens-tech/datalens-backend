import abc
import datetime
import logging
from typing import Mapping

import attrs

import dl_settings


LOGGER = logging.getLogger(__name__)


@attrs.define(kw_only=True, frozen=True)
class MetadataProvider(abc.ABC):
    """
    Metadata provider.
    ttl:
        TTL of the metadata. If None, the metadata will be set once and then never updated.
    error_retry_delay:
        Error retry delay. If an error occurs, the client will wait for this duration before retrying.
    """

    ttl: datetime.timedelta | None = None
    error_retry_delay: datetime.timedelta = attrs.field(default=datetime.timedelta(seconds=30))

    @abc.abstractmethod
    async def get_metadata(self) -> Mapping[str, str]:
        pass


class MetadataProviderSettings(dl_settings.TypedBaseSettings):
    ...


class EmptyMetadataProviderSettings(MetadataProviderSettings):
    ...


MetadataProviderSettings.register("empty", EmptyMetadataProviderSettings)


@attrs.define(kw_only=True, frozen=True)
class EmptyMetadataProvider(MetadataProvider):
    @classmethod
    def from_settings(cls, settings: MetadataProviderSettings) -> "EmptyMetadataProvider":
        return cls()

    async def get_metadata(self) -> Mapping[str, str]:
        return {}
