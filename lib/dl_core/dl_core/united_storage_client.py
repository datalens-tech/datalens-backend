from __future__ import annotations

import abc
from contextlib import contextmanager
from datetime import (
    datetime,
    timedelta,
    timezone,
)
from json.decoder import JSONDecodeError
import logging
import re
import time
from typing import (
    Any,
    ClassVar,
    Generator,
    Iterable,
    NamedTuple,
    Optional,
    Type,
    Union,
    cast,
)

import attr
import requests
from requests.exceptions import HTTPError

from dl_api_commons.base_models import (
    AuthData,
    AuthTarget,
    TenantCommon,
    TenantDef,
)
from dl_api_commons.logging import RequestObfuscator
from dl_api_commons.tracing import get_current_tracing_headers
from dl_api_commons.utils import (
    get_retriable_requests_session,
    stringify_dl_cookies,
    stringify_dl_headers,
)
from dl_constants.api_constants import (
    DLCookies,
    DLHeaders,
    DLHeadersCommon,
)
from dl_core.base_models import EntryLocation
from dl_core.enums import USApiType
import dl_core.exc as exc
from dl_core.retrier.policy import BaseRetryPolicyFactory
from dl_core.retrier.requests import RequestsPolicyRetrier


LOGGER = logging.getLogger(__name__)


class USClientHTTPExceptionWrapper(Exception):
    pass


@attr.s
class USAuthContextBase:
    DEFAULT_US_PREFIX: ClassVar[USApiType] = USApiType.v1
    IS_TENANT_ID_MUTABLE: ClassVar[bool] = False

    dl_component: str = attr.ib(default="backend", kw_only=True)

    def is_tenant_id_mutable(self) -> bool:
        return self.IS_TENANT_ID_MUTABLE

    def get_folder_id(self) -> Optional[str]:
        return None

    def get_default_prefix(self) -> str:
        val = self.DEFAULT_US_PREFIX.value
        if isinstance(val, str):
            return val

        raise ValueError(f"US prefix enum member {self.DEFAULT_US_PREFIX} is not a string")

    @abc.abstractmethod
    def get_tenant(self) -> TenantDef:
        raise NotImplementedError()

    @abc.abstractmethod
    def get_outbound_headers(self, include_tenancy: bool = True) -> dict[DLHeaders, str]:
        raise NotImplementedError()

    @abc.abstractmethod
    def get_outbound_cookies(self) -> dict[DLCookies, str]:
        raise NotImplementedError()


@attr.s
class USAuthContextRegular(USAuthContextBase):
    auth_data: AuthData = attr.ib(kw_only=True)
    tenant: TenantDef = attr.ib(kw_only=True)
    # Raw header value. None is interpreted as no header
    dl_sudo: Optional[str] = attr.ib(kw_only=True)
    # Raw header value. None is interpreted as no header
    dl_allow_super_user: Optional[str] = attr.ib(kw_only=True)

    def get_tenant(self) -> TenantDef:
        return self.tenant

    def get_outbound_headers(self, include_tenancy: bool = True) -> dict[DLHeaders, str]:
        flags: dict[DLHeaders, Optional[str]] = {
            DLHeadersCommon.SUDO: self.dl_sudo,
            DLHeadersCommon.ALLOW_SUPERUSER: self.dl_allow_super_user,
        }

        return {
            **(self.tenant.get_outbound_tenancy_headers() if include_tenancy else {}),
            **self.auth_data.get_headers(AuthTarget.UNITED_STORAGE),
            **{name: val for name, val in flags.items() if val is not None},
        }

    def get_outbound_cookies(self) -> dict[DLCookies, str]:
        return self.auth_data.get_cookies(AuthTarget.UNITED_STORAGE)


@attr.s(frozen=True)
class USAuthContextPublic(USAuthContextBase):
    DEFAULT_US_PREFIX = USApiType.public
    DEFAULT_TENANT = TenantCommon()

    us_public_token: str = attr.ib(repr=False)

    def get_tenant(self) -> TenantDef:
        return self.DEFAULT_TENANT

    def get_outbound_headers(self, include_tenancy: bool = True) -> dict[DLHeaders, str]:
        return {
            DLHeadersCommon.US_PUBLIC_TOKEN: self.us_public_token,
        }

    def get_outbound_cookies(self) -> dict[DLCookies, str]:
        return {}


@attr.s(frozen=True)
class USAuthContextMaster(USAuthContextBase):
    DEFAULT_US_PREFIX = USApiType.private
    DEFAULT_TENANT = TenantCommon()
    IS_TENANT_ID_MUTABLE = True

    us_master_token: str = attr.ib(repr=False)

    def get_tenant(self) -> TenantDef:
        return self.DEFAULT_TENANT

    def get_outbound_headers(self, include_tenancy: bool = True) -> dict[DLHeaders, str]:
        return {DLHeadersCommon.US_MASTER_TOKEN: self.us_master_token}

    def get_outbound_cookies(self) -> dict[DLCookies, str]:
        return {}


@attr.s(frozen=True)
class USAuthContextNoAuth(USAuthContextBase):
    DEFAULT_TENANT = TenantCommon()

    def get_tenant(self) -> TenantDef:
        return self.DEFAULT_TENANT

    def get_outbound_headers(self, include_tenancy: bool = True) -> dict[DLHeaders, str]:
        return {}

    def get_outbound_cookies(self) -> dict[DLCookies, str]:
        return {}


@attr.s(frozen=True)
class USAuthContextEmbed(USAuthContextBase):
    DEFAULT_US_PREFIX = USApiType.embeds

    auth_data: AuthData = attr.ib(kw_only=True)
    tenant: TenantDef = attr.ib(kw_only=True)

    def get_tenant(self) -> TenantDef:
        return self.tenant

    def get_outbound_headers(self, include_tenancy: bool = True) -> dict[DLHeaders, str]:
        return {
            **(self.tenant.get_outbound_tenancy_headers() if include_tenancy else {}),
            **self.auth_data.get_headers(AuthTarget.UNITED_STORAGE),
        }

    def get_outbound_cookies(self) -> dict[DLCookies, str]:
        return self.auth_data.get_cookies(AuthTarget.UNITED_STORAGE)


class UStorageClientBase:
    class RequestAdapter(metaclass=abc.ABCMeta):
        @property
        @abc.abstractmethod
        def relative_url(self) -> str:
            pass

        @property
        @abc.abstractmethod
        def method(self) -> str:
            pass

        @abc.abstractmethod
        def get_header(self, name: str) -> Optional[str]:
            pass

        @property
        @abc.abstractmethod
        def json(self) -> Optional[dict]:
            pass

    class ResponseAdapter(metaclass=abc.ABCMeta):
        @property
        @abc.abstractmethod
        def status_code(self) -> int:
            pass

        @abc.abstractmethod
        def get_header(self, name: str) -> Optional[str]:
            pass

        @property
        @abc.abstractmethod
        def elapsed_seconds(self) -> float:
            pass

        @property
        @abc.abstractmethod
        def content(self) -> bytes:
            pass

        @property
        @abc.abstractmethod
        def request(self) -> "UStorageClientBase.RequestAdapter":
            pass

        @abc.abstractmethod
        def raise_for_status(self) -> None:
            pass

        @abc.abstractmethod
        def json(self) -> dict:
            pass

    ERROR_MAP: list[tuple[int, Optional[re.Pattern], Type[exc.USReqException]]] = [
        (400, None, exc.USBadRequestException),
        (403, None, exc.USAccessDeniedException),
        (403, re.compile("Workbook isolation interruption"), exc.USWorkbookIsolationInterruptionException),
        (404, None, exc.USObjectNotFoundException),
        (409, re.compile("The entry already exists"), exc.USAlreadyExistsException),
        (409, re.compile("Incorrect entryId for embed"), exc.USIncorrectEntryIdForEmbed),
        (409, None, exc.USIncorrectTenantIdException),
        (423, None, exc.USLockUnacquiredException),
        (451, None, exc.USReadOnlyModeEnabledException),
        (530, None, exc.USPermissionCheckError),
    ]

    RequestData = NamedTuple(
        "RequestData",
        [
            ("method", str),
            ("relative_url", str),
            ("params", Optional[dict[str, str]]),
            ("json", Optional[dict]),
        ],
    )

    def __init__(
        self,
        host: str,
        auth_ctx: USAuthContextBase,
        retry_policy_factory: BaseRetryPolicyFactory,
        prefix: Optional[str] = None,
        context_request_id: Optional[str] = None,
        context_forwarded_for: Optional[str] = None,
        context_workbook_id: Optional[str] = None,
    ):
        self.host = host
        self.prefix = auth_ctx.get_default_prefix() if prefix is None else prefix

        self._disabled = False
        self._folder_id = auth_ctx.get_folder_id()
        self._auth_ctx = auth_ctx
        self._tenant_override: Optional[TenantDef] = None
        self._retry_policy_factory = retry_policy_factory

        # Default headers for HTTP sessions
        self._default_headers = self._auth_ctx_to_default_headers(auth_ctx)
        if context_forwarded_for is not None:
            self._default_headers[DLHeadersCommon.FORWARDED_FOR.value] = context_forwarded_for
        if context_workbook_id is not None:
            self._default_headers[DLHeadersCommon.WORKBOOK_ID.value] = context_workbook_id
        # Initial cookies for HTTP session
        self._cookies = self._auth_ctx_to_cookies(auth_ctx)
        # Headers that might be changed during lifecycle e.g. Folder ID
        self._extra_headers: dict[str, str] = {}

        if context_request_id is not None:
            self._default_headers["X-Request-Id"] = context_request_id

        self._ensure_extra_headers()

    def _ensure_extra_headers(self) -> None:
        effective_tenant = self.get_effective_tenant()
        expected_extra_headers = stringify_dl_headers(effective_tenant.get_outbound_tenancy_headers())
        if self._extra_headers != expected_extra_headers:
            self._extra_headers = expected_extra_headers

    def get_effective_tenant(self) -> TenantDef:
        if self._tenant_override is not None:
            return self._tenant_override
        return self._auth_ctx.get_tenant()

    def set_tenant_override(self, tenant: TenantDef) -> None:
        if self._auth_ctx.is_tenant_id_mutable():
            self._tenant_override = tenant
            self._ensure_extra_headers()
        else:
            raise AssertionError(
                f"US client folder ID is immutable with auth context {type(self._auth_ctx).__qualname__}"
            )

    @staticmethod
    def parse_datetime(dt: str) -> datetime:
        # TODO: remove after migrating to python 3.11 or above
        if dt[-1] == "Z":
            return datetime.fromisoformat(dt.removesuffix("Z")).replace(tzinfo=timezone.utc)
        return datetime.fromisoformat(dt)

    @staticmethod
    def _auth_ctx_to_default_headers(ctx: USAuthContextBase) -> dict[str, str]:
        headers: dict[DLHeaders, str] = {
            **ctx.get_outbound_headers(include_tenancy=False),
            DLHeadersCommon.DL_COMPONENT: ctx.dl_component,
        }
        return stringify_dl_headers(headers)

    @staticmethod
    def _auth_ctx_to_cookies(ctx: USAuthContextBase) -> dict[str, str]:
        return stringify_dl_cookies(ctx.get_outbound_cookies())

    def _get_full_url(self, relative_url: str) -> str:
        return "/".join(map(lambda s: s.strip("/"), (self.host, self.prefix, relative_url)))

    @staticmethod
    def _log_request_start(request_data: RequestData) -> None:
        LOGGER.info(
            "Asking UnitedStorage: method: %s, url: %s, params:(%s)",
            request_data.method,
            request_data.relative_url,
            request_data.params or {},
        )

    @classmethod
    def _get_us_json_from_response(cls, response: ResponseAdapter) -> dict:
        LOGGER.info(
            "Got %s from UnitedStorage (%s %s), content length: %s, request-id: %s, elapsed: %s",
            response.status_code,
            response.request.method,
            response.request.relative_url,
            len(response.content),
            response.get_header("X-Request-ID"),
            response.elapsed_seconds,
        )
        if response.status_code >= 400:
            LOGGER.info(
                "US error response on %s %s (%s): %s, folder_id: %s, org_id: %s, req_id: %s",
                response.request.method,
                response.request.relative_url,
                RequestObfuscator().mask_sensitive_fields_by_name_in_json_recursive(response.request.json),
                response.content,
                # TODO: BI-4918 move to local injection and reuse bi_api_commons_ya_cloud.constants.DLHeadersYC
                response.request.get_header(DLHeadersCommon.FOLDER_ID.value),
                response.request.get_header(DLHeadersCommon.ORG_ID.value),
                response.get_header("X-Request-ID"),
            )
        try:
            response.raise_for_status()
        except USClientHTTPExceptionWrapper as http_err_ex_wrapper:
            http_err_ex = cast(Optional[Exception], http_err_ex_wrapper.__cause__)

            message: Optional[str] = response.json().get("message")
            for status_code, error_regex, exc_cls in cls.ERROR_MAP:
                if response.status_code == status_code:
                    if error_regex is None or message is not None and error_regex.match(message):
                        raise exc_cls(orig_exc=http_err_ex) from http_err_ex_wrapper

            raise exc.USReqException(orig_exc=http_err_ex) from http_err_ex_wrapper

        try:
            return response.json()
        except JSONDecodeError as ex:
            LOGGER.info("Got http status %s with invalid json %s from US", response.status_code, response.content)
            raise exc.USInvalidResponse from ex

    def _raise_for_disabled_interactions(self) -> None:
        if self._disabled:
            raise exc.USInteractionDisabled("Interaction with US is forbidden")

    @contextmanager
    def interaction_disabled(self) -> Generator[None, None, None]:
        try:
            self._disabled = True
            yield
        except Exception:
            raise
        finally:
            self._disabled = False

    # Requests definitions
    @classmethod
    def _req_data_create_entry(
        cls,
        key: EntryLocation,
        scope: str,
        meta: Optional[dict[str, str]] = None,
        data: Optional[dict[str, Any]] = None,
        type_: Optional[str] = None,
        hidden: Optional[bool] = None,
        links: Optional[dict[str, Any]] = None,
        mode: str = "publish",
        unversioned_data: Optional[dict[str, Any]] = None,
        **kwargs: Any,
    ) -> RequestData:
        meta = meta or {}
        data = data or {}
        unversioned_data = unversioned_data or {}
        type_ = type_ or ""
        links = links or {}

        return cls.RequestData(
            method="post",
            relative_url="/entries",
            params=None,
            json={
                "scope": scope,
                **key.to_us_req_api_params(),
                "meta": meta,
                "data": data,
                "unversionedData": unversioned_data,
                "type": type_,
                "recursion": True,
                "hidden": hidden,
                "links": links,
                "mode": mode,
                **kwargs,
            },
        )

    @classmethod
    def _req_data_get_entry(
        cls,
        entry_id: str,
        params: Optional[dict[str, str]] = None,
        include_permissions: bool = True,
        include_links: bool = True,
    ) -> RequestData:
        params = params or {}
        if include_permissions:
            params["includePermissionsInfo"] = "1"
        if include_links:
            params["includeLinks"] = "1"

        return cls.RequestData(
            method="get",
            relative_url="/entries/{}".format(entry_id),
            params=params,
            json=None,
        )

    @classmethod
    def _req_data_move_entry(cls, entry_id: str, destination: str) -> RequestData:
        return cls.RequestData(
            method="post",
            relative_url="/entries/{}/move".format(entry_id),
            params=None,
            json={"destination": destination},
        )

    @classmethod
    def _req_data_update_entry(
        cls,
        entry_id: str,
        data: Optional[dict[str, Any]] = None,
        unversioned_data: Optional[dict[str, Any]] = None,
        meta: Optional[dict[str, str]] = None,
        mode: str = "publish",
        lock: Optional[str] = None,
        hidden: Optional[bool] = None,
        links: Optional[dict[str, Any]] = None,
        update_revision: Optional[bool] = None,
    ) -> RequestData:
        data = data or {}
        unversioned_data = unversioned_data or {}
        meta = meta or {}
        links = links or {}

        json_data: dict[str, Any] = {
            "data": data,
            "unversionedData": unversioned_data,
            "meta": meta,
            "mode": mode,
            "links": links,
        }
        if hidden is not None:
            json_data["hidden"] = hidden
        if lock is not None:
            json_data["lockToken"] = lock
        if update_revision is not None:
            json_data["updateRevision"] = update_revision
        return cls.RequestData(
            method="post",
            relative_url="/entries/{}".format(entry_id),
            params=None,
            json=json_data,
        )

    @classmethod
    def _req_data_delete_entry(cls, entry_id: str, lock: Optional[str] = None) -> RequestData:
        params = None
        if lock is not None:
            params = {"lockToken": lock}
        return cls.RequestData(method="delete", relative_url="/entries/{}".format(entry_id), params=params, json=None)

    @classmethod
    def _req_data_iter_entries(
        cls,
        scope: str,
        entry_type: Optional[str] = None,
        meta: Optional[dict[str, str]] = None,
        all_tenants: bool = False,
        include_data: bool = False,
        ids: Optional[Iterable[str]] = None,
        creation_time: Optional[dict[str, Union[str, int, None]]] = None,
        page: int = 0,
        created_at_from: float = 0,
        limit: Optional[int] = None,
    ) -> RequestData:
        req_params: dict[Any, Any] = dict(scope=scope, includeData=int(include_data))
        if entry_type:
            req_params.update(type=entry_type)
        meta = meta or {}
        if meta:
            req_params.update({"meta[{}]".format(k): v for k, v in meta.items()})
        if creation_time:
            req_params.update({"creationTime[{}]".format(k): v for k, v in creation_time.items()})
        if ids:
            req_params["ids"] = ids
        if limit:
            req_params["limit"] = limit

        if all_tenants:
            assert include_data is False  # not supported for this endpoint
            assert creation_time is None  # not supported for this endpoint
            req_params["createdAtFrom"] = created_at_from
            endpoint = "/inter-tenant/entries"
        else:
            req_params["page"] = page
            endpoint = "/entries"

        return cls.RequestData(
            method="get",
            relative_url=endpoint,
            params=req_params,
            json=None,
        )

    @classmethod
    def _req_data_acquire_lock(
        cls,
        entry_id: str,
        duration: Optional[int] = None,
        force: Optional[bool] = None,
    ) -> RequestData:
        """
        :param duration: in seconds. Default = 300 (5min)
        :return: lock token
        """
        params: dict[str, Any] = {}
        if duration is not None:
            params.update(duration=duration * 1000)  # US accepts duration in milliseconds
        if force is not None:
            params.update(force=force)

        return cls.RequestData(method="post", relative_url="/locks/{}".format(entry_id), params=None, json=params)

    @classmethod
    def _req_data_release_lock(cls, entry_id: str, lock: str) -> RequestData:
        return cls.RequestData(
            method="delete", relative_url="/locks/{}".format(entry_id), params={"lockToken": lock}, json=None
        )

    @classmethod
    def _req_data_entries_in_path(cls, *, us_path: str, page_size: int, page_idx: int) -> RequestData:
        return cls.RequestData(
            method="get",
            relative_url="/navigation",
            params=dict(
                page=str(page_idx),
                pageSize=str(page_size),
                path=us_path,
                includePermissionsInfo="false",
            ),
            json=None,
        )

    @classmethod
    def _req_data_entry_revisions(cls, entry_id: str) -> RequestData:
        return cls.RequestData(
            method="get",
            relative_url="/entries/{}/revisions".format(entry_id),
            params=None,
            json=None,
        )


class UStorageClient(UStorageClientBase):
    class RequestAdapter(UStorageClientBase.RequestAdapter):
        def __init__(
            self,
            request: requests.PreparedRequest,
            request_data: UStorageClientBase.RequestData,
        ):
            self.req = request
            self._request_data = request_data

        @property
        def relative_url(self) -> str:
            return self._request_data.relative_url

        @property
        def method(self) -> str:
            assert self.req.method is not None
            return self.req.method.lower()

        def get_header(self, name: str) -> Optional[str]:
            return self.req.headers.get(name)

        @property
        def json(self) -> Optional[dict]:
            return self._request_data.json

    class ResponseAdapter(UStorageClientBase.ResponseAdapter):
        def __init__(self, response: requests.Response, request_data: "UStorageClient.RequestData"):
            self.resp = response
            self._req_adapter = UStorageClient.RequestAdapter(response.request, request_data)

        @property
        def status_code(self) -> int:
            return self.resp.status_code

        def get_header(self, name: str) -> Optional[str]:
            return self.resp.headers.get(name)

        @property
        def elapsed_seconds(self) -> float:
            return self.resp.elapsed.total_seconds()

        @property
        def content(self) -> bytes:
            return self.resp.content

        @property
        def request(self) -> "UStorageClientBase.RequestAdapter":
            return self._req_adapter

        def raise_for_status(self) -> None:
            try:
                self.resp.raise_for_status()
            except HTTPError as http_error:
                raise USClientHTTPExceptionWrapper(str(http_error)) from http_error

        def json(self) -> dict:
            return self.resp.json()

    def __init__(
        self,
        host: str,
        auth_ctx: USAuthContextBase,
        retry_policy_factory: BaseRetryPolicyFactory,
        prefix: Optional[str] = None,
        context_request_id: Optional[str] = None,
        context_forwarded_for: Optional[str] = None,
        context_workbook_id: Optional[str] = None,
    ):
        super().__init__(
            host=host,
            auth_ctx=auth_ctx,
            prefix=prefix,
            retry_policy_factory=retry_policy_factory,
            context_request_id=context_request_id,
            context_forwarded_for=context_forwarded_for,
            context_workbook_id=context_workbook_id,
        )

        self._session = get_retriable_requests_session()
        self._session.headers.update(self._default_headers)
        self._session.cookies.update(self._cookies)

    def close(self) -> None:
        self._session.close()

    def _request(
        self,
        request_data: UStorageClientBase.RequestData,
        retry_policy_name: Optional[str] = None,
    ) -> dict[str, Any]:
        self._raise_for_disabled_interactions()

        url = self._get_full_url(request_data.relative_url)

        self._log_request_start(request_data)

        request_kwargs: dict[str, Any] = {"json": request_data.json} if request_data.json is not None else {}
        tracing_headers = get_current_tracing_headers()

        retry_policy = self._retry_policy_factory.get_policy(retry_policy_name)
        retrier = RequestsPolicyRetrier(retry_policy=retry_policy)

        response = retrier.retry_request(
            self._session.request,
            method=request_data.method,
            url=url,
            stream=False,
            params=request_data.params,
            headers={
                **self._extra_headers,
                **tracing_headers,
            },
            **request_kwargs,
        )

        response_adapter = self.ResponseAdapter(response, request_data)
        return self._get_us_json_from_response(response_adapter)

    def create_entry(
        self,
        key: EntryLocation,
        scope: str,
        meta: Optional[dict[str, str]] = None,
        data: Optional[dict[str, Any]] = None,
        unversioned_data: Optional[dict[str, Any]] = None,
        type_: Optional[str] = None,
        hidden: Optional[bool] = None,
        links: Optional[dict[str, Any]] = None,
        **kwargs: Any,
    ) -> dict[str, Any]:
        rq_data = self._req_data_create_entry(
            key=key,
            scope=scope,
            meta=meta,
            data=data,
            unversioned_data=unversioned_data,
            type_=type_,
            hidden=hidden,
            links=links,
            **kwargs,
        )
        return self._request(
            rq_data,
            retry_policy_name="create_entry",
        )

    def get_entry(
        self,
        entry_id: str,
        params: Optional[dict[str, str]] = None,
        include_permissions: bool = True,
        include_links: bool = True,
    ) -> dict[str, Any]:
        return self._request(
            self._req_data_get_entry(
                entry_id, params=params, include_permissions=include_permissions, include_links=include_links
            ),
            retry_policy_name="get_entry",
        )

    def move_entry(self, entry_id: str, destination: str) -> dict[str, Any]:
        return self._request(
            self._req_data_move_entry(entry_id, destination=destination),
            retry_policy_name="move_entry",
        )

    def update_entry(
        self,
        entry_id: str,
        data: Optional[dict[str, Any]] = None,
        unversioned_data: Optional[dict[str, Any]] = None,
        meta: Optional[dict[str, str]] = None,
        lock: Optional[str] = None,
        hidden: Optional[bool] = None,
        links: Optional[dict[str, Any]] = None,
        update_revision: Optional[bool] = None,
    ) -> dict[str, Any]:
        return self._request(
            self._req_data_update_entry(
                entry_id,
                data=data,
                unversioned_data=unversioned_data,
                meta=meta,
                lock=lock,
                hidden=hidden,
                links=links,
                update_revision=update_revision,
            ),
            retry_policy_name="update_entry",
        )

    def entries_iterator(
        self,
        scope: str,
        entry_type: Optional[str] = None,
        meta: Optional[dict] = None,
        all_tenants: bool = False,
        include_data: bool = False,
        ids: Optional[Iterable[str]] = None,
        creation_time: Optional[dict[str, Union[str, int, None]]] = None,
        limit: Optional[int] = None,
    ) -> Generator[dict, None, None]:
        """
        implements 2-in-1 pagination:
        - by page number (in this case entries are returned from the US along with a nextPageToken)
        - by creation time (entries are returned as a list ordered by creation time)

        :param scope:
        :param entry_type:
        :param meta: Filter entries by "meta" section values.
        :param all_tenants: Look up across all tenants. False by default.
        :param include_data: Return full US entry data. False by default.
        :param ids: Filter entries by uuid.
        :param creation_time: Filter entries by creation_time. Available filters: eq, ne, gt, gte, lt, lte
        :return:
        """
        created_at_from: datetime = datetime(1970, 1, 1)  # for creation time pagination
        previous_page_entry_ids = set()  # for deduplication
        page: int = 0  # for page number pagination

        done = False
        while not done:
            # 1. Prepare and make request
            created_at_from_ts = created_at_from.timestamp()
            unseen_entry_ids = set()
            resp = self._request(
                self._req_data_iter_entries(
                    scope,
                    entry_type=entry_type,
                    meta=meta,
                    all_tenants=all_tenants,
                    include_data=include_data,
                    ids=ids,
                    creation_time=creation_time,
                    page=page,
                    created_at_from=created_at_from_ts,
                    limit=limit,
                ),
                retry_policy_name="entries_iterator",
            )

            # 2. Deal with pagination
            page_entries: list
            if isinstance(resp, list):
                page_entries = resp
                if page_entries:
                    created_at_from = self.parse_datetime(page_entries[-1]["createdAt"]) - timedelta(milliseconds=1)
                    # minus 1 ms to account for cases where entries, created during a single millisecond, happen to
                    # return on the border of two batches (one in batch 1 and the other in batch 2),
                    # hence the deduplication
                else:
                    LOGGER.info("Got an empty entries list in the US response, the listing is completed")
                    done = True
            else:
                page_entries = resp.get("entries", [])
                if resp.get("nextPageToken"):
                    page = resp["nextPageToken"]
                else:
                    LOGGER.info("Got no nextPageToken in the US response, the listing is completed")
                    done = True

            # 3. Yield results
            for entr in page_entries:
                if entr["entryId"] not in previous_page_entry_ids:
                    unseen_entry_ids.add(entr["entryId"])
                    yield entr

            # 4. Stop if got no nextPageToken or unseen entries
            previous_page_entry_ids = unseen_entry_ids.copy()
            if not unseen_entry_ids:
                LOGGER.info("US response is not empty, but we got no unseen entries, assuming the listing is completed")
                done = True

    def delete_entry(self, entry_id: str, lock: Optional[str] = None) -> None:
        self._request(
            self._req_data_delete_entry(entry_id, lock=lock),
            retry_policy_name="delete_entry",
        )

    def acquire_lock(
        self,
        entry_id: str,
        duration: Optional[int] = None,
        wait_timeout: Optional[int] = None,
        force: Optional[bool] = None,
    ) -> str:
        """
        :param entry_id:
        :param force:
        :param duration: in seconds. Default = 300 (5min)
        :param wait_timeout: in seconds
        :return: lock token
        """
        req_data = self._req_data_acquire_lock(entry_id, duration=duration, force=force)
        start_ts = time.time()
        while True:
            try:
                resp = self._request(
                    req_data,
                    retry_policy_name="acquire_lock",
                )
                lock = resp["lockToken"]
                LOGGER.info('Acquired lock "%s" for object "%s"', lock, entry_id)
                return lock
            except exc.USLockUnacquiredException:
                if wait_timeout and time.time() - start_ts < wait_timeout:
                    time.sleep(0.25)
                else:
                    raise

    def release_lock(self, entry_id: str, lock: str) -> None:
        try:
            self._request(
                self._req_data_release_lock(entry_id, lock=lock),
                retry_policy_name="release_lock",
            )
        except exc.USReqException:
            LOGGER.exception('Unable to release lock "%s"', lock)

    def get_entries_info_in_path(self, us_path: str) -> list[dict[str, Any]]:
        resp = self._request(
            self._req_data_entries_in_path(
                us_path=us_path,
                page_size=100,
                page_idx=0,
            ),
            retry_policy_name="get_entries_info_in_path",
        )
        return resp["entries"]

    def get_entry_revisions(self, entry_id: str) -> list[dict[str, Any]]:
        resp = self._request(
            self._req_data_entry_revisions(entry_id),
            retry_policy_name="get_entry_revisions",
        )
        return resp["entries"]
