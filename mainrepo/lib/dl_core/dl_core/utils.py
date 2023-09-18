from __future__ import annotations

import ipaddress
import logging
import os
import re
from typing import (
    Any,
    Generic,
    Iterable,
    Optional,
    Type,
    TypeVar,
)
from urllib.parse import urlparse
import uuid

from aiohttp import (
    ClientResponse,
    ClientResponseError,
    RequestInfo,
)
import attr
import jaeger_client
from multidict import (
    CIMultiDict,
    CIMultiDictProxy,
)
import opentracing
import requests
import requests.adapters
import requests.exceptions
import shortuuid
import sqlalchemy as sa

from dl_api_commons import clean_secret_data_in_headers
from dl_api_commons.base_models import (
    AuthData,
    RequestContextInfo,
)
from dl_api_commons.utils import (
    stringify_dl_cookies,
    stringify_dl_headers,
)
from dl_configs.settings_loaders.env_remap import remap_env
from dl_constants.api_constants import DLHeadersCommon

LOGGER = logging.getLogger(__name__)


def make_url(
    protocol: str,
    host: str,
    port: int,
    path: Optional[str] = None,
) -> str:
    # TODO FIX: Sanitize/use urllib
    if path is None:
        path = ""
    return f"{protocol}://{host}:{port}/{path.lstrip('/')}"


def get_requests_session() -> requests.Session:
    session = requests.Session()
    ua = "{}, Datalens".format(requests.utils.default_user_agent())
    session.headers.update({"User-Agent": ua})
    return session


def get_retriable_requests_session() -> requests.Session:
    session = get_requests_session()

    retry_conf = requests.adapters.Retry(
        total=5,
        backoff_factor=0.5,
        status_forcelist=[500, 501, 502, 503, 504, 521],
        redirect=10,
        method_whitelist=frozenset(["HEAD", "TRACE", "GET", "PUT", "OPTIONS", "DELETE", "POST"]),
        # # TODO:
        # # (the good: will return a response when it's an error response)
        # # (the bad: need to raise_for_status() manually, same as without retry conf)
        # raise_on_status=False,
    )

    for schema in ("http://", "https://"):
        session.mount(
            schema,
            # noinspection PyUnresolvedReferences
            requests.adapters.HTTPAdapter(
                max_retries=retry_conf,
            ),
        )

    return session


def make_user_auth_headers(
    rci: RequestContextInfo,
    auth_data_override: Optional[AuthData] = None,
    sudo_override: Optional[bool] = None,
    allow_superuser_override: Optional[bool] = None,
) -> dict[str, str]:
    headers: dict[str, str] = {}

    effective_auth_data: Optional[AuthData] = rci.auth_data if auth_data_override is None else auth_data_override
    if effective_auth_data is not None:
        headers.update(stringify_dl_headers(effective_auth_data.get_headers()))

    req_id = rci.request_id
    if req_id is not None:
        headers.update({DLHeadersCommon.REQUEST_ID.value: req_id})

    tenant = rci.tenant
    assert tenant is not None
    headers.update(stringify_dl_headers(tenant.get_outbound_tenancy_headers()))

    def may_be_add_extra_header(header: DLHeadersCommon, override_value: Optional[Any]) -> None:
        incoming_value = rci.plain_headers.get(header.value)
        if override_value is not None:
            if isinstance(override_value, str):
                headers[header.value] = override_value
            if isinstance(override_value, bool):
                if override_value:
                    headers[header.value] = "1"
            else:
                raise AssertionError(f"Unsupported type of header override: {type(override_value)}")
        else:
            if incoming_value is not None:
                headers[header.value] = incoming_value

    may_be_add_extra_header(DLHeadersCommon.SUDO, sudo_override)
    may_be_add_extra_header(DLHeadersCommon.ALLOW_SUPERUSER, allow_superuser_override)
    may_be_add_extra_header(DLHeadersCommon.DL_CONTEXT, None)

    return headers


def make_user_auth_cookies(
    rci: RequestContextInfo,
    auth_data_override: Optional[AuthData] = None,
) -> dict[str, str]:
    effective_auth_data: Optional[AuthData] = rci.auth_data if auth_data_override is None else auth_data_override
    if effective_auth_data is not None:
        return stringify_dl_cookies(effective_auth_data.get_cookies())
    return {}


# TODO FIX: Remove after migration to Connection Executors
def compile_query_for_debug(query, dialect):  # type: ignore  # TODO: fix
    """
    Compile query to string.
    This function is only suitable for logging and not execution of the result.
    Its result might not be valid SQL if query contains date/datetime literals.
    """
    try:
        try:
            return str(query.compile(dialect=dialect, compile_kwargs={"literal_binds": True}))
        except NotImplementedError:
            compiled = query.compile(dialect=dialect)
            return "{0}; {1!r}".format(str(query), compiled.params)
    except Exception:
        return "-"


def parse_comma_separated_hosts(host: Optional[str]) -> tuple[str, ...]:
    if not host:
        return tuple()
    return tuple(h.strip() for h in host.split(","))


def validate_hostname_or_ip_address(hostname: str):  # type: ignore  # TODO: fix
    # IP address case
    try:
        ipaddress.ip_address(hostname)
    except ValueError:
        pass  # Not a valid IP address, continue to check if valid hostname
    else:
        return

    # DNS hostname case
    if hostname[-1] == ".":
        hostname = hostname[:-1]
    if len(hostname) > 253:
        raise ValueError("Hostname is too long")

    label_list = hostname.split(".")

    if re.match(r"[0-9]+$", label_list[-1]):
        raise ValueError("TLD must be not all-numeric")

    label_re = re.compile(r"(?!-)[a-z0-9-]{1,63}(?<!-)$", re.IGNORECASE)

    if not all(label_re.match(label) for label in label_list):
        raise ValueError("Not a valid domain name")

    # Ensure valid netloc (from previous hostname validation mechanism)
    parsed_url = urlparse("//{}".format(hostname))
    if parsed_url.netloc != hostname:
        raise ValueError("Not a valid netloc")


def shorten_uuid(some_uuid: str):  # type: ignore  # TODO: fix
    return shortuuid.encode(uuid.UUID(some_uuid))


_FUTURE_REF_TV = TypeVar("_FUTURE_REF_TV")


@attr.s(eq=False, hash=False, order=False)
class FutureRef(Generic[_FUTURE_REF_TV]):
    __ref: Optional[_FUTURE_REF_TV] = attr.ib(init=False, default=None)

    @property
    def ref(self) -> "_FUTURE_REF_TV":
        if self.__ref is None:
            raise ValueError("FutureRef was not fulfilled")
        return self.__ref

    def fulfill(self, obj: _FUTURE_REF_TV) -> None:
        if self.__ref is not None:
            raise ValueError("FutureRef already fulfilled")
        self.__ref = obj

    @classmethod
    def fulfilled(cls, obj: _FUTURE_REF_TV) -> "FutureRef[_FUTURE_REF_TV]":
        fr = cls()
        fr.fulfill(obj)
        return fr


# TODO FIX: BI-2497 try to load in generic ways
def get_eqe_secret_key() -> bytes:
    effective_env = remap_env(os.environ)
    return effective_env.get("RQE_SECRET_KEY", "").encode()


def make_id() -> str:
    return shortuuid.uuid()


_MODEL_TYPE_TV = TypeVar("_MODEL_TYPE_TV", bound=attr.AttrsInstance)


def attrs_evolve_to_subclass(cls: Type[_MODEL_TYPE_TV], inst: Any, **kwargs) -> _MODEL_TYPE_TV:  # type: ignore  # TODO: fix
    """
    Evolve an attr.s instance to a subclass instance with additional attributes.
    Note that this works correctly only for attributes with ``init=True``.
    """

    super_cls = type(inst)
    if super_cls is cls:
        if kwargs:
            return attr.evolve(inst, **kwargs)
        else:
            return inst
    assert issubclass(cls, super_cls), f"Expected subclass of {super_cls.__name__}, got {cls.__name__}"
    all_attrs = {f.name: getattr(inst, f.name.lstrip("_")) for f in attr.fields(super_cls) if f.init}
    all_attrs.update(kwargs)
    return cls(**all_attrs)  # type: ignore  # TODO: fix


def attrs_evolve_to_superclass(cls: Type[_MODEL_TYPE_TV], inst: Any, **kwargs) -> _MODEL_TYPE_TV:  # type: ignore  # TODO: fix
    """
    Evolve an attr.s instance to a superclass instance with additional attributes.
    Note that this works correctly only for attributes with ``init=True``.
    """

    sub_cls = type(inst)
    if sub_cls is cls:
        if kwargs:
            return attr.evolve(inst, **kwargs)
        else:
            return inst
    assert issubclass(sub_cls, cls), f"Expected superclass of {sub_cls.__name__}, got {cls.__name__}"
    all_attrs = {f.name: getattr(inst, f.name.lstrip("_")) for f in attr.fields(cls) if f.init}
    all_attrs.update(kwargs)
    return cls(**all_attrs)  # type: ignore  # TODO: fix


def get_current_w3c_tracing_headers(
    tracer: Optional[opentracing.Tracer] = None,
    req_id: Optional[str] = None,
    override_sampled: Optional[bool] = None,
) -> dict[str, str]:
    actual_tracer = opentracing.global_tracer() if tracer is None else tracer
    tracing_headers: dict[str, str] = {}

    active_span = actual_tracer.active_span

    if active_span is not None:
        span_ctx = active_span.context

        if isinstance(span_ctx, jaeger_client.SpanContext):
            if override_sampled is None:
                flags = "01" if span_ctx.flags & 0x1 else "00"
            else:
                flags = "01" if override_sampled else "00"

            header_value = f"00-{span_ctx.trace_id:032x}-{span_ctx.span_id:016x}-{flags}"
            LOGGER.info("Traceparent header was generated for req_id %r: %r", req_id, header_value)
            tracing_headers.update(
                traceparent=header_value,
            )
        else:
            LOGGER.debug("Could not create W3C tracing headers (span is not Jaeger span) for req id: %s", req_id)

    else:
        LOGGER.debug("Could not create W3C tracing headers (no active span) for req id: %s", req_id)

    return tracing_headers


def generate_revision_id() -> str:
    return shortuuid.uuid()


def sa_plain_text(query: str) -> sa.sql.elements.TextClause:
    """sa.text without SA semantics (':'-parameters)"""
    # query = query.replace('\\', r'\\')  # weirdly, seems unnecessary
    query = query.replace(":", r"\:")
    return sa.text(query)


SECREPR_SIDE_SIZE = 0


def secrepr(value: Optional[str]) -> str:
    """Convenience function for attrs-repr of secrets"""
    if value is None:
        return repr(value)
    if not value:
        return repr(value)
    if not isinstance(value, str):
        # Really shouldn't raise in a `repr`
        return f"???{type(value)!r}???"

    side_size = SECREPR_SIDE_SIZE  # not a kwarg just to keep mypy happier
    if not side_size or len(value) <= side_size * 3:
        return "..."
    return repr(f"{value[:side_size]}...{value[-side_size:]}")


def _multidict_to_list(md: CIMultiDictProxy[str]) -> Iterable[tuple[str, str]]:
    return [(str(k), str(v)) for k, v in md.items()]


def raise_for_status_and_hide_secret_headers(response: ClientResponse) -> None:
    """
    Overriden function from aiohttp which raises ClientResponseError is response result is not ok,
    but hides secret data in headers for exception
    """
    if not response.ok:
        # reason should always be not None for a started response
        assert response.reason is not None
        response.release()
        new_request_info = RequestInfo(
            response.request_info.url,
            response.request_info.method,
            CIMultiDict(clean_secret_data_in_headers(_multidict_to_list(response.request_info.headers))),
        )
        raise ClientResponseError(
            new_request_info,
            response.history,
            status=response.status,
            message=response.reason,
            headers=CIMultiDict(clean_secret_data_in_headers(_multidict_to_list(response.headers))),
        )
