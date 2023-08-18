from __future__ import annotations

import abc
import logging
import time
from typing import ClassVar, Optional, Dict, Type, TypeVar, Set

import attr
import docker.errors
import shortuuid
from docker import DockerClient
from docker.models.containers import Container

from bi_testing_db_provision.model.commons_db import DBCreds
from bi_testing_db_provision.model.request_db import DBRequest

LOGGER = logging.getLogger(__name__)

_PROVISIONER_TV = TypeVar('_PROVISIONER_TV', bound='DefaultProvisioner')


class QueryExecutionException(Exception):
    pass


@attr.s
class DefaultProvisioner:
    bootstrap_db_name: ClassVar[Optional[str]]
    bootstrap_db_creds: ClassVar[DBCreds]

    aliveness_check_interval: ClassVar[float]
    aliveness_check_timeout: ClassVar[float] = 30.
    aliveness_check_query: ClassVar[str]

    _db_request: DBRequest = attr.ib()
    _docker_client: DockerClient = attr.ib()

    # Internal to be filled in process of provisioning
    _container: Optional[Container] = attr.ib(init=False, default=None)

    @property
    def container(self) -> Container:
        assert self._container is not None, "Container was not created"
        return self._container

    @abc.abstractmethod
    def get_container_environ(self) -> Dict[str, str]:
        raise NotImplementedError()

    @abc.abstractmethod
    def get_image_name(self) -> str:
        raise NotImplementedError()

    @classmethod
    def get_container_code_names(cls) -> Set[str]:
        return {'main'}

    @property
    @abc.abstractmethod
    def db_client_executable(self) -> str:
        raise NotImplementedError()

    @abc.abstractmethod
    def execute_query(self, query: str, db_name: Optional[str] = None) -> None:
        raise NotImplementedError()

    # noinspection PyMethodMayBeStatic
    def generate_container_name(self) -> str:
        return f"dbp_{shortuuid.uuid()}"

    def provision(self, container_name_map: Optional[Dict[str, str]] = None) -> None:
        self.validate_request()
        effective_container_name = (
            self.generate_container_name()
            if container_name_map is None
            else container_name_map['main']
        )

        # TODO FIX: Drop container if exists with same name
        self._container: Container = self._docker_client.containers.run(
            image=self.get_image_name(),
            name=effective_container_name,
            environment=self.get_container_environ(),
            detach=True,
        )

        self.wait_db_up()
        self.bootstrap_db()

    def is_alive(self) -> bool:
        try:
            self.execute_query(self.aliveness_check_query)
        except QueryExecutionException as exc:
            LOGGER.debug("Got exception during aliveness check: %s", exc)
            return False
        except docker.errors.APIError:
            raise
        return True

    def wait_db_up(self) -> None:
        start_time = time.monotonic()
        while True:
            LOGGER.debug("Checking if DB is alive")
            is_alive = self.is_alive()
            if is_alive:
                LOGGER.debug("Got alive DB")
                return
            LOGGER.debug("Going to sleep for %s before next aliveness check", self.aliveness_check_interval)
            if time.monotonic() - start_time > self.aliveness_check_timeout:
                # TODO FIX: More adequate exception
                raise ValueError("Timeout for waiting DB to become up")
            time.sleep(self.aliveness_check_interval)

    @abc.abstractmethod
    def bootstrap_db(self) -> None:
        pass

    def validate_request(self) -> None:
        # TODO FIX: Validate that there is no overlap with bootstrap
        pass

    def close(self) -> None:
        self._docker_client.close()

    @classmethod
    def create(
            cls: Type[_PROVISIONER_TV],
            db_request: DBRequest,
            docker_client: DockerClient,
    ) -> _PROVISIONER_TV:
        return cls(
            db_request=db_request,
            docker_client=docker_client,
        )
