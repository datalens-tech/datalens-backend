import abc
import datetime
import logging
from typing import Mapping

import attrs


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


@attrs.define(kw_only=True, frozen=True)
class EmptyMetadataProvider(MetadataProvider):
    async def get_metadata(self) -> Mapping[str, str]:
        return {}
