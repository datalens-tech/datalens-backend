# TODO FIX: Move to common

from __future__ import annotations

import enum
import functools
import inspect
import itertools
import typing

from bi_api_commons.aiohttp.aiohttp_wrappers import DLRequestView, RequiredResource

from bi_api_lib.aio import aiohttp_wrappers


_METHOD_REQ_RESOURCES_ATTR_NAME = "__dl_required_resources__"


class RequiredResourceDSAPI(RequiredResource):
    JSON_REQUEST = enum.auto()


class BaseView(DLRequestView[aiohttp_wrappers.DSAPIRequest]):
    dl_request_cls = aiohttp_wrappers.DSAPIRequest

    _COMMON_REQUIRED_RESOURCES: typing.ClassVar[typing.FrozenSet[RequiredResource]] = frozenset()

    @classmethod
    def get_required_resources(cls, method_name: str) -> typing.FrozenSet[RequiredResource]:
        method_name = method_name.lower()
        if hasattr(cls, method_name):
            method = getattr(cls, method_name)
            if hasattr(method, _METHOD_REQ_RESOURCES_ATTR_NAME):
                return frozenset(itertools.chain(
                    cls._COMMON_REQUIRED_RESOURCES,
                    getattr(method, _METHOD_REQ_RESOURCES_ATTR_NAME)
                ))
            else:
                return cls._COMMON_REQUIRED_RESOURCES

        # If method is not implemented - we will not use any resource
        return frozenset()

    @staticmethod
    def with_resolved_entities(coro):  # type: ignore  # TODO: fix
        @functools.wraps(coro)
        async def wrapper(self, *args, **kwargs):  # type: ignore  # TODO: fix
            await self.resolve_entities()
            return await coro(self, *args, **kwargs)

        return wrapper

    async def resolve_entities(self):  # type: ignore  # TODO: fix
        pass


# TODO FIX: add ability to exclude resources for particular handles
def requires(*resources: RequiredResource, skip_parent_resources: bool = False):  # type: ignore  # TODO: fix
    def real_deco(func_or_class):  # type: ignore  # TODO: fix
        if isinstance(func_or_class, type) and issubclass(func_or_class, BaseView):
            clz = func_or_class
            if skip_parent_resources:
                all_resources = resources
            else:
                all_resources = itertools.chain(  # type: ignore  # TODO: fix
                    clz._COMMON_REQUIRED_RESOURCES,
                    resources,
                )
            clz._COMMON_REQUIRED_RESOURCES = frozenset(all_resources)

        elif inspect.iscoroutinefunction(func_or_class):
            func = func_or_class
            setattr(func, _METHOD_REQ_RESOURCES_ATTR_NAME, frozenset(resources))

        else:
            raise ValueError(
                "Only BaseView or coroutine functions inside of them can be used with requires() decorator."
                f" Not '{func_or_class}'"
            )

        return func_or_class

    return real_deco
