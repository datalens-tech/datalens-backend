from __future__ import annotations

import os
import re
from typing import (
    Any,
    ClassVar,
    Optional,
)

import flask
from flask_restx import Resource

from dl_api_commons.base_models import RequestContextInfo
from dl_api_commons.flask.middlewares.commit_rci_middleware import ReqCtxInfoMiddleware
from dl_api_commons.flask.required_resources import RequiredResourceCommon
from dl_api_connector.api_schema.extras import OperationsMode
from dl_api_lib import api_decorators
from dl_api_lib.schemas.tools import prepare_schema_context
from dl_api_lib.service_registry.service_registry import ApiServiceRegistry
from dl_core.flask_utils.us_manager_middleware import USManagerFlaskMiddleware
from dl_core.us_manager.us_manager_sync import SyncUSManager


PROFILE_REQUESTS = os.environ.get("PROFILE_REQUESTS", "")
PROFILE_REQ_CLASSES = {v for v in set(os.environ.get("PROFILE_REQ_CLASSES", "").split(",")) if v}
PROFILE_REQ_METHODS = {v for v in set(os.environ.get("PROFILE_REQ_METHODS", "").lower().split(",")) if v}
PROFILE_REQ_PATH_RE = os.environ.get("PROFILE_REQ_PATH_RE")

# When turning on the profiler try to be as specific as possible. Example:
# PROFILE_REQUESTS="/tmp/cprofiler/"
# PROFILE_REQ_CLASSES="DatasetVersionResult"
# PROFILE_REQ_METHODS="POST"
# PROFILE_REQ_PATH_RE="/api/v1/datasets/njJYHhj8oikj/versions/draft/result"


def _profile_request_check(*args, **kwargs) -> bool:  # type: ignore  # TODO: fix
    """
    Check whether request handler should be profiled with current settings:
    - ``PROFILE_PATH_RE``: limit profiling only to requests with path matching the given regex
    """
    if PROFILE_REQ_PATH_RE:
        if not re.match(PROFILE_REQ_PATH_RE, flask.request.path):
            return False

    return True


COMMON_INFO_HEADERS = ("Referer", "X-Chart-Id")  # TODO: bi-common


class BIResourceMeta(type(Resource)):  # type: ignore  # TODO: fix
    """
    Metaclass for applying decorators to all BI handler classes and their methods
    (e.g. profiling, logging, etc.)
    """

    @staticmethod
    def _get_future_class_attr(bases, attrs, attr_name):  # type: ignore  # TODO: fix
        """Get attribute of future class described by ``bases`` and ``attrs``"""
        if attr_name in attrs:
            # attribute is redefined in new class
            return attrs[attr_name]
        else:
            # attribute is inherited from one of the base classes
            # (i.e. the first one in `bases` with this attribute)
            for base in bases:
                if hasattr(base, attr_name):
                    return getattr(base, attr_name)

    def __new__(mcs, name, bases, attrs):  # type: ignore  # TODO: fix
        profile_methods = mcs._get_future_class_attr(bases, attrs, "profile_methods")
        if PROFILE_REQ_METHODS:
            profile_methods = set(profile_methods) & PROFILE_REQ_METHODS

        if PROFILE_REQUESTS and profile_methods and (not PROFILE_REQ_CLASSES or (name in PROFILE_REQ_CLASSES)):
            # enable request profiler
            stats_dir = os.path.join(PROFILE_REQUESTS, name)
            attrs = {
                # decorate handler methods with profiler
                meth_name: (
                    api_decorators.with_profiler_stats(
                        os.path.join(stats_dir, meth_name),
                        # turn on checker only if there is something to check
                        condition_check=_profile_request_check if PROFILE_REQ_PATH_RE else None,  # type: ignore  # 2024-01-24 # TODO: Argument "condition_check" to "with_profiler_stats" has incompatible type "Callable[[VarArg(Any), KwArg(Any)], bool] | None"; expected "Callable[..., Any]"  [arg-type]
                    )(method)
                    if meth_name in profile_methods
                    else method
                )
                for meth_name, method in attrs.items()
            }

        return super().__new__(mcs, name, bases, attrs)


class BIResource(Resource, metaclass=BIResourceMeta):
    """Base class for all BI handler classes"""

    REQUIRED_RESOURCES: ClassVar[frozenset[RequiredResourceCommon]] = frozenset()

    @classmethod
    def get_current_rci(cls) -> RequestContextInfo:
        return ReqCtxInfoMiddleware.get_request_context_info()

    @classmethod
    def get_schema_ctx(
        cls, schema_operations_mode: Optional[OperationsMode] = None, editable_object: Any = None
    ) -> dict:
        return prepare_schema_context(
            usm=cls.get_service_us_manager()
            if RequiredResourceCommon.US_HEADERS_TOKEN in cls.REQUIRED_RESOURCES
            else cls.get_us_manager(),
            op_mode=schema_operations_mode,
            editable_object=editable_object,
        )

    @classmethod
    def get_us_manager(cls) -> SyncUSManager:
        return USManagerFlaskMiddleware.get_request_us_manager()

    @classmethod
    def get_service_us_manager(cls) -> SyncUSManager:
        return USManagerFlaskMiddleware.get_request_service_us_manager()

    @classmethod
    def get_service_registry(cls) -> ApiServiceRegistry:
        usm = (
            cls.get_service_us_manager()
            if RequiredResourceCommon.US_HEADERS_TOKEN in cls.REQUIRED_RESOURCES
            else cls.get_us_manager()
        )
        service_registry = usm.get_services_registry()
        assert isinstance(service_registry, ApiServiceRegistry)
        return service_registry

    @classmethod
    def enrich_us_with_tenant_header(cls, usm: SyncUSManager) -> None:
        tenant = cls.get_current_rci().tenant
        if tenant is not None:
            usm.set_tenant_override(tenant)

    profile_methods = ("get", "post", "delete", "put", "patch", "options", "head")
