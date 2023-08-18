import abc
from typing import TypeVar, Dict

import attr

from bi_testing_db_provision.model.enums import ResourceKind, ResourceType
from bi_testing_db_provision.model.request_base import ResourceRequest
from bi_testing_db_provision.model.request_db import DBRequest
from bi_testing_db_provision.model.resource import ResourceBase, ResourceInternalData

_DOCKER_RESOURCE_REQUEST_TV = TypeVar('_DOCKER_RESOURCE_REQUEST_TV', bound=ResourceRequest)


@attr.s(frozen=True)
class DockerResourceInternalData(ResourceInternalData):
    allocation_host: str = attr.ib()
    container_name_map: Dict[str, str] = attr.ib()


@attr.s(frozen=True)
class DockerResourceBase(ResourceBase[_DOCKER_RESOURCE_REQUEST_TV], metaclass=abc.ABCMeta):
    kind = ResourceKind.single_docker

    internal_data: DockerResourceInternalData = attr.ib()


@attr.s(frozen=True)
class StandalonePostgresResource(DockerResourceBase[DBRequest]):
    type = ResourceType.standalone_postgres

    request: DBRequest = attr.ib()


@attr.s(frozen=True)
class StandaloneClickHouseResource(DockerResourceBase[DBRequest]):
    type = ResourceType.standalone_clickhouse

    request: DBRequest = attr.ib()


@attr.s(frozen=True)
class StandaloneMSSQLResource(DockerResourceBase[DBRequest]):
    type = ResourceType.standalone_mssql

    request: DBRequest = attr.ib()


@attr.s(frozen=True)
class StandaloneOracleResource(DockerResourceBase[DBRequest]):
    type = ResourceType.standalone_oracle

    request: DBRequest = attr.ib()
