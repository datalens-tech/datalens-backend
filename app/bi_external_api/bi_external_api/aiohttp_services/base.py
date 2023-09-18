from __future__ import annotations

import abc
import enum
from functools import cached_property
from typing import ClassVar, Optional, Any

import attr
import yaml
from aiohttp import web

from dl_api_commons.base_models import TenantDef
from dl_api_commons.aiohttp.aiohttp_wrappers import RequiredResource, DLRequestBase
from bi_external_api.enums import ExtAPIType
from bi_external_api.internal_api_clients.main import InternalAPIClients
from bi_external_api.internal_api_clients.united_storage import MiniUSClient


class ExtAPIRequiredResource(RequiredResource):
    INT_API_CLIENTS = enum.auto()


class SerializationType(enum.Enum):
    json = enum.auto()
    yaml = enum.auto()


class InternalAPIClientsFactory(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def get_internal_api_clients(self, tenant_override: TenantDef) -> InternalAPIClients:
        raise NotImplementedError()

    @abc.abstractmethod
    def get_super_user_us_client(self) -> MiniUSClient:
        raise NotImplementedError()


class ExtAPIRequest(DLRequestBase):
    KEY_INTERNAL_API_CLIENTS_FACTORY: ClassVar[str] = 'ext_api_internal_api_clients_factory'

    # TODO FIX: Remove this property after all middleware will be switched to grab resources from view object
    @property
    def required_resources(self) -> frozenset[RequiredResource]:
        view_cls = self.request.match_info.handler
        if isinstance(view_cls, type) and issubclass(view_cls, BaseView):
            return view_cls.get_required_resources(self.request.method)
        return super().required_resources

    @property
    def internal_api_clients_factory(self) -> InternalAPIClientsFactory:
        int_api_clients_factory = self.request[self.KEY_INTERNAL_API_CLIENTS_FACTORY]
        assert isinstance(int_api_clients_factory, InternalAPIClientsFactory)
        return int_api_clients_factory

    def set_internal_api_clients_factory(self, int_api_clients_factory: InternalAPIClientsFactory) -> None:
        self._set_attr_once(self.KEY_INTERNAL_API_CLIENTS_FACTORY, int_api_clients_factory)


@attr.s(frozen=True)
class AppConfig:
    _APP_KEY: ClassVar[str] = "DL_EXT_API_APP_CONFIG"

    use_workbooks_api: bool = attr.ib()
    api_type: ExtAPIType = attr.ib()
    do_add_exc_message: bool = attr.ib()

    @classmethod
    def from_app(cls, app: web.Application) -> 'AppConfig':
        return app[cls._APP_KEY]

    def bind(self, app: web.Application) -> None:
        app[self._APP_KEY] = self


class BaseView(web.View):
    @property
    def app_request(self) -> ExtAPIRequest:
        app_rq = self.request[DLRequestBase.KEY_DL_REQUEST]
        assert isinstance(app_rq, ExtAPIRequest)
        return app_rq

    @property
    def app_config(self) -> AppConfig:
        return AppConfig.from_app(self.request.app)

    @classmethod
    def get_required_resources(cls, method_name: str) -> frozenset[RequiredResource]:
        return frozenset()

    @staticmethod
    def _mime_type_to_serialization_type(mime: Optional[str]) -> Optional[SerializationType]:
        if mime is None or mime.lower() == "*/*" or mime.lower() == "application/octet-stream":
            return None
        if mime.lower().endswith("/json"):
            return SerializationType.json
        if mime.lower().endswith("/yaml") or mime.lower().endswith("/yml"):
            return SerializationType.yaml
        raise AssertionError(f"Unexpected mime type: {mime}")

    @cached_property
    def req_serialization_type(self) -> Optional[SerializationType]:
        return self._mime_type_to_serialization_type(self.request.content_type)

    @cached_property
    def resp_serialization_type(self) -> Optional[SerializationType]:
        return self._mime_type_to_serialization_type(self.request.headers.get("accept"))

    async def get_request_dict(self) -> dict[str, Any]:
        requested_serialization_type = self.req_serialization_type
        effective_serialization_type: SerializationType = (
            SerializationType.json
            if requested_serialization_type is None
            else requested_serialization_type
        )

        if effective_serialization_type == SerializationType.json:
            return await self.request.json()

        if effective_serialization_type == SerializationType.yaml:
            body = await self.request.content.read()
            return yaml.safe_load(body)

        raise AssertionError(f"Unexpected serialization type: {effective_serialization_type}")
