import logging
from typing import TypeVar

import attr

from bi_testing_db_provision.docker.life_cycle_manager import StandaloneDockerLifeCycleManager
from bi_testing_db_provision.model.enums import ResourceState
from bi_testing_db_provision.model.resource import ResourceBase
from bi_testing_db_provision.model.resource_docker import DockerResourceBase
from bi_testing_db_provision.workers.worker_resource_base import ResourceWorker

LOGGER = logging.getLogger(__name__)

_WORKER_TARGET_TV = TypeVar('_WORKER_TARGET_TV')


@attr.s
class ResourceProvisioningWorker(ResourceWorker):
    interesting_state = ResourceState.create_required
    processing_state = ResourceState.create_in_progress

    def __attrs_post_init__(self) -> None:
        assert self.tpe

    async def handle_target(self, target: ResourceBase) -> ResourceBase:
        final_ok_state: ResourceState = (
            ResourceState.free if target.session_id is None
            else ResourceState.acquired
        )

        lc_manager = StandaloneDockerLifeCycleManager(
            docker_client_url_sequence=('unix://var/run/docker.sock',),
            thread_pool_executor=self.tpe,
            resource_saver=self._resource_dao,
        )
        assert isinstance(target, DockerResourceBase)
        updated_target = await lc_manager.provision(target)

        return updated_target.clone(state=final_ok_state)


class ResourceRecyclingWorker(ResourceWorker):
    interesting_state = ResourceState.recycle_required
    processing_state = ResourceState.recycle_in_progress

    async def handle_target(self, target: ResourceBase) -> ResourceBase:
        lc_manager = StandaloneDockerLifeCycleManager(
            docker_client_url_sequence=('unix://var/run/docker.sock',),
            thread_pool_executor=self.tpe,
            resource_saver=self._resource_dao,
        )
        assert isinstance(target, DockerResourceBase)
        updated_target = await lc_manager.recycle(target)

        return updated_target
