import abc
import asyncio
import datetime
import logging
from typing import Mapping

import attrs
import temporalio.client as temporalio_client
from typing_extensions import Self


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


@attrs.define(kw_only=True, frozen=True)
class TemporalClientSettings:
    namespace: str
    host: str
    port: int = 7233
    tls: bool = False
    metadata_provider: MetadataProvider = attrs.field(factory=EmptyMetadataProvider)

    @property
    def target_host(self) -> str:
        return f"{self.host}:{self.port}"


@attrs.define(kw_only=True)
class TemporalClient:
    base_client: temporalio_client.Client
    metadata_provider: MetadataProvider

    _update_metadata_task: asyncio.Task = attrs.field(init=False)

    @classmethod
    async def from_settings(cls, settings: TemporalClientSettings) -> Self:
        metadata_provider = settings.metadata_provider
        rpc_metadata = await metadata_provider.get_metadata()

        temporal_client = await temporalio_client.Client.connect(
            target_host=settings.target_host,
            namespace=settings.namespace,
            lazy=True,
            tls=settings.tls,
            rpc_metadata=rpc_metadata,
        )

        return cls(
            base_client=temporal_client,
            metadata_provider=metadata_provider,
        )

    async def _update_metadata(self) -> None:
        if self.metadata_provider.ttl is None:
            LOGGER.debug("Metadata TTL is None, skipping metadata update task")
            return

        while True:
            try:
                LOGGER.debug("Updating temporal client metadata")
                rpc_metadata = await self.metadata_provider.get_metadata()
                self.base_client.rpc_metadata = rpc_metadata
                await asyncio.sleep(self.metadata_provider.ttl.total_seconds())
            except Exception:
                LOGGER.exception("Error updating temporal client metadata")
                await asyncio.sleep(self.metadata_provider.error_retry_delay.total_seconds())

    def __attrs_post_init__(self) -> None:
        self._update_metadata_task = asyncio.create_task(self._update_metadata())

    async def close(self) -> None:
        if self._update_metadata_task.done():
            return
        self._update_metadata_task.cancel()

    async def check_health(self) -> bool:
        try:
            return await self.base_client.service_client.check_health(
                timeout=datetime.timedelta(seconds=1),
            )
        except Exception:
            return False
