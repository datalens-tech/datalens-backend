from __future__ import annotations

import typing
from typing import Any, Dict

import attr
from aiohttp import web

from bi_api_commons.aiohttp.aiohttp_wrappers import RequiredResource
from bi_core.aio.aiohttp_wrappers_data_core import DLRequestDataCore


@attr.s(frozen=True)
class AppWrapper:
    allow_query_cache_usage: bool = attr.ib()
    allow_notifications: bool = attr.ib()

    _APP_KEY = "DL_APP_WRAPPER"

    @classmethod
    def from_app(cls, app: web.Application) -> 'AppWrapper':
        return app[cls._APP_KEY]

    def bind(self, app: web.Application) -> None:
        app[self._APP_KEY] = self


class DSAPIRequest(DLRequestDataCore):
    @property
    def app_wrapper(self) -> AppWrapper:
        return AppWrapper.from_app(self.request.app)

    # TODO FIX: Remove this property after all middleware will be switched to grab resources from view object
    @property
    def required_resources(self) -> typing.FrozenSet[RequiredResource]:
        from bi_api_lib.app.data_api.resources.base import BaseView
        view_cls = self.request.match_info.handler
        if isinstance(view_cls, type) and issubclass(view_cls, BaseView):
            return view_cls.get_required_resources(self.request.method)
        return frozenset()

    # TODO FIX: Move to common
    KEY_BODY_JSON = "body_json"

    # TODO FIX: Move to common
    def store_parsed_json_body(self, val: Dict[str, Any]) -> None:
        self._set_attr_once(self.KEY_BODY_JSON, val)

    # TODO FIX: Move to common
    @property
    def json(self) -> Dict[str, Any]:
        if self.KEY_BODY_JSON in self.request:
            return self.request[self.KEY_BODY_JSON]
        raise ValueError("JSON was not parsed for this request")
