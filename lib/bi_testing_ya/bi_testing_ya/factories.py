from __future__ import annotations

import abc
from typing import (
    TYPE_CHECKING,
    Any,
    Generic,
    TypeVar,
)

import attr

from bi_cloud_integration.iam_rm_client import DLFolderServiceClient
from bi_cloud_integration.yc_client_base import DLYCServiceConfig


if TYPE_CHECKING:
    from bi_testing_ya.cloud_tokens import AccountCredentials

_FACTORY_TV = TypeVar("_FACTORY_TV", bound="AbstractTestResourceFactory")
_RESOURCE_TV = TypeVar("_RESOURCE_TV")
_RESOURCE_REQUEST_TV = TypeVar("_RESOURCE_REQUEST_TV")


@attr.s
class AbstractTestResourceFactory(Generic[_RESOURCE_TV, _RESOURCE_REQUEST_TV], metaclass=abc.ABCMeta):
    _created_instances: list = attr.ib(init=False, factory=list)

    @abc.abstractmethod
    def _create_resource(self, resource_request: _RESOURCE_REQUEST_TV) -> _RESOURCE_TV:
        raise NotImplementedError()

    @abc.abstractmethod
    def _close_resource(self, resource: _RESOURCE_TV) -> None:
        raise NotImplementedError()

    def create(self, resource_request: _RESOURCE_REQUEST_TV) -> _RESOURCE_TV:
        resource = self._create_resource(resource_request)
        self._created_instances.append(resource)
        return resource

    def close_all(self) -> None:
        for resource in self._created_instances:
            try:
                self._close_resource(resource)
            except Exception:  # noqa
                pass

    def __enter__(self: _FACTORY_TV) -> _FACTORY_TV:
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.close_all()


_ASYNC_FACTORY_TV = TypeVar("_ASYNC_FACTORY_TV", bound="AbstractAsyncTestResourceFactory")


@attr.s
class AbstractAsyncTestResourceFactory(Generic[_RESOURCE_TV, _RESOURCE_REQUEST_TV], metaclass=abc.ABCMeta):
    _created_instances: list = attr.ib(init=False, factory=list)

    @abc.abstractmethod
    async def _create_resource(self, resource_request: _RESOURCE_REQUEST_TV) -> _RESOURCE_TV:
        raise NotImplementedError()

    @abc.abstractmethod
    async def _close_resource(self, resource: _RESOURCE_TV) -> None:
        raise NotImplementedError()

    async def create(self, resource_request: _RESOURCE_REQUEST_TV) -> _RESOURCE_TV:
        resource = await self._create_resource(resource_request)
        self._created_instances.append(resource)
        return resource

    async def close_all(self) -> None:
        for resource in self._created_instances:
            try:
                await self._close_resource(resource)
            except Exception:  # noqa
                pass

    async def __aenter__(self: _ASYNC_FACTORY_TV) -> _ASYNC_FACTORY_TV:
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        await self.close_all()


@attr.s(frozen=True, auto_attribs=True)
class FolderServiceRequest:
    acct_creds: AccountCredentials


@attr.s(frozen=True)
class FolderServiceFactory(AbstractTestResourceFactory[DLFolderServiceClient, FolderServiceRequest]):
    endpoint: str = attr.ib()

    def _create_resource(self, resource_request: FolderServiceRequest) -> DLFolderServiceClient:
        config = DLYCServiceConfig(
            endpoint=self.endpoint,
        )
        return DLFolderServiceClient(
            service_config=config,
            bearer_token=resource_request.acct_creds.token,
        )

    def _close_resource(self, resource: DLFolderServiceClient) -> None:
        resource.channel.close()
