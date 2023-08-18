import abc
import asyncio
import logging
from typing import Optional, TypeVar, ClassVar

import attr

from bi_testing_db_provision.dao.resource_dao import ResourceDAO
from bi_testing_db_provision.db.resource import ResourceRecord
from bi_testing_db_provision.model.enums import ResourceState
from bi_testing_db_provision.model.resource import ResourceBase
from bi_testing_db_provision.workers.worker_base import BaseWorker

LOGGER = logging.getLogger(__name__)

_WORKER_TARGET_TV = TypeVar('_WORKER_TARGET_TV')


class ResourceWorker(BaseWorker[ResourceBase], metaclass=abc.ABCMeta):
    interesting_state: ClassVar[ResourceState]
    processing_state: ClassVar[ResourceState]

    _resource_dao: ResourceDAO = attr.ib(init=False, default=None)

    async def _on_reconnect(self) -> None:
        self._resource_dao = ResourceDAO(self._connection)

    async def wait_for_target(self) -> _WORKER_TARGET_TV:
        timeout: Optional[float] = 5

        while True:
            LOGGER.info("Looking for target")
            async with self.transaction_cm():
                reserved_resource = await self.find_and_reserve()

            if reserved_resource is not None:
                LOGGER.info("Got target resource: %s", reserved_resource)
                return reserved_resource  # type: ignore  # TODO: fix
            else:
                LOGGER.info("Target not found")

            try:
                LOGGER.info("Waiting for notification")
                data = await self._resource_dao.wait_for_resource_update(timeout=timeout)
                LOGGER.info("Got notification data %s", data)
            except asyncio.TimeoutError:
                LOGGER.info("Waiting for notification timeout")

    async def find_and_reserve(self) -> Optional[ResourceBase]:
        resource = await self._resource_dao.find_and_lock_resource(
            ResourceRecord.resource_state == self.interesting_state
        )
        if resource is not None:
            return await self._resource_dao.update(
                resource.clone(state=self.processing_state)
            )

        return None

    async def save_target_transactional(self, target: ResourceBase) -> ResourceBase:
        return await self._resource_dao.update(target)
