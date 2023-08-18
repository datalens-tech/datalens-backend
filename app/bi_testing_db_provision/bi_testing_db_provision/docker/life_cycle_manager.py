import asyncio
import random
from concurrent.futures.thread import ThreadPoolExecutor
from typing import ClassVar, Dict, Type, Tuple

import attr
import docker.errors
import shortuuid
from docker import DockerClient

from bi_testing_db_provision.dao.resource_dao import ResourceDAO
from bi_testing_db_provision.docker.provisioners.base import DefaultProvisioner
from bi_testing_db_provision.docker.provisioners.clickhouse import ClickHouseProvisioner
from bi_testing_db_provision.docker.provisioners.mssql import MSSQLProvisioner
from bi_testing_db_provision.docker.provisioners.oracle import OracleProvisioner
from bi_testing_db_provision.docker.provisioners.postgres import PGProvisioner
from bi_testing_db_provision.model.enums import ResourceState
from bi_testing_db_provision.model.request_db import DBRequest
from bi_testing_db_provision.model.resource_docker import (
    DockerResourceBase,
    StandalonePostgresResource,
    StandaloneClickHouseResource,
    StandaloneMSSQLResource,
    StandaloneOracleResource,
    DockerResourceInternalData,
)


@attr.s
class StandaloneDockerLifeCycleManager:
    _docker_client_url_sequence: Tuple[str, ...] = attr.ib()
    _thread_pool_executor: ThreadPoolExecutor = attr.ib()
    # TODO FIX: Make dedicated interface for saving resources and do not use ResourceDAO
    _resource_saver: ResourceDAO = attr.ib()

    map_resource_cls_provisioner_class: ClassVar[Dict[Type[DockerResourceBase], Type[DefaultProvisioner]]] = {
        StandalonePostgresResource: PGProvisioner,
        StandaloneClickHouseResource: ClickHouseProvisioner,
        StandaloneMSSQLResource: MSSQLProvisioner,
        StandaloneOracleResource: OracleProvisioner,
    }

    def _create_provisioner(self, resource: DockerResourceBase, docker_url: str) -> DefaultProvisioner:
        db_request = resource.request

        if not isinstance(db_request, DBRequest):
            raise AssertionError(f"Unexpected type of request for resource {resource.id}: {db_request!r}")

        provisioner_cls: Type[DefaultProvisioner] = self.map_resource_cls_provisioner_class[type(resource)]

        return provisioner_cls.create(
            db_request=db_request,
            docker_client=DockerClient(base_url=docker_url),
        )

    def _provision_sync(self, resource: DockerResourceBase, docker_url: str) -> DockerResourceBase:
        provisioner = self._create_provisioner(resource, docker_url)

        try:
            container_name_map = resource.internal_data.container_name_map
            assert set(container_name_map) == provisioner.get_container_code_names(), "Invalid container name map"

            provisioner.provision(resource.internal_data.container_name_map)
            return resource
        finally:
            # TODO FIX: Move to property
            provisioner._docker_client.close()

    async def provision(self, resource: DockerResourceBase) -> DockerResourceBase:
        loop = asyncio.get_event_loop()
        docker_url = random.choice(self._docker_client_url_sequence)

        provisioner = self._create_provisioner(resource, docker_url)
        container_name_map = {
            code_name: f'tdbp_res_{shortuuid.encode(resource.id)}_{code_name}'  # type: ignore
            for code_name in provisioner.get_container_code_names()
        }
        resource = resource.clone(
            internal_data=DockerResourceInternalData(
                allocation_host=docker_url,
                container_name_map=container_name_map,
            )
        )
        resource = await self._resource_saver.update(resource)  # type: ignore  # TODO: fix

        # Prepare all internal data & save
        return await loop.run_in_executor(self._thread_pool_executor, self._provision_sync, resource, docker_url)

    def _remove_containers_sync(self, resource: DockerResourceBase, docker_url: str) -> DockerResourceBase:
        docker_client = DockerClient(base_url=docker_url)
        for container_code, container_name in resource.internal_data.container_name_map.items():
            try:
                container = docker_client.containers.get(container_name)
            except docker.errors.NotFound:
                pass
            else:
                container.remove(v=True, force=True)

        docker_client.close()
        return resource

    async def recycle(self, resource: DockerResourceBase) -> DockerResourceBase:
        loop = asyncio.get_event_loop()
        docker_url = random.choice(self._docker_client_url_sequence)
        resource = await loop.run_in_executor(
            self._thread_pool_executor,
            self._remove_containers_sync,
            resource,
            docker_url,
        )
        return resource.clone(state=ResourceState.deleted)
