from __future__ import annotations

import copy
from datetime import (
    datetime,
    timezone,
)
from typing import (
    Any,
    Optional,
)

from cryptography import fernet
import shortuuid

from dl_api_commons.base_models import RequestContextInfo
from dl_api_commons.retrier.policy import (
    BaseRetryPolicyFactory,
    DefaultRetryPolicyFactory,
)
from dl_configs.crypto_keys import CryptoKeysConfig
from dl_core.base_models import EntryLocation
from dl_core.exc import USObjectNotFoundException
from dl_core.services_registry.top_level import (
    DummyServiceRegistry,
    ServicesRegistry,
)
from dl_core.united_storage_client import (
    USAuthContextBase,
    USAuthContextMaster,
    UStorageClient,
    UStorageClientBase,
)
from dl_core.us_manager.us_manager_sync import SyncUSManager


class MockedUStorageClient(UStorageClient):
    def __init__(
        self,
        host: str,
        auth_ctx: USAuthContextBase,
        retry_policy_factory: BaseRetryPolicyFactory,
        prefix: Optional[str] = None,
        context_request_id: Optional[str] = None,
    ):
        super().__init__(
            host=host,
            auth_ctx=auth_ctx,
            retry_policy_factory=retry_policy_factory,
            prefix=prefix,
            context_request_id=context_request_id,
        )
        self._saved_entries: dict[str, dict[str, Any]] = {}

    @classmethod
    def format_dt(cls, dt: datetime) -> str:
        assert dt.tzinfo
        pre_formatted = (
            dt.replace(microsecond=dt.microsecond // 1000 * 1000)
            .astimezone(timezone.utc)
            .strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3]
        )
        return f"{pre_formatted}Z"

    def _request(
        self,
        request_data: UStorageClientBase.RequestData,
        retry_policy_name: Optional[str] = None,
    ) -> dict[str, Any]:
        raise NotImplementedError("This is dummy US client")

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
        assert not kwargs, "Not supported by dummy"
        entry_id = shortuuid.uuid()
        rev_id = shortuuid.uuid()
        subject = "unknown"
        created_at = self.format_dt(datetime.now(timezone.utc))

        resp = dict(
            entryId=entry_id,
            scope=scope,
            type=type_,
            unversionedData=copy.deepcopy(unversioned_data),
            createdBy=subject,
            createdAt=created_at,
            updatedBy=subject,
            updatedAt=created_at,
            savedId=rev_id,
            # publishedId="???",
            revId=rev_id,
            tenantId="common",
            data=copy.deepcopy(data),
            meta=copy.deepcopy(meta),
            hidden=hidden,
            # mirrored=???,
            # public=???,
            links=links,
            **key.to_us_resp_api_params(None),
        )

        self._saved_entries[entry_id] = resp
        return copy.deepcopy(resp)

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
        previous_resp = self._saved_entries[entry_id]

        new_revision_id = shortuuid.uuid()
        new_updated_at = self.format_dt(datetime.now(timezone.utc))
        subject = "unknown"

        previous_resp.update(
            data=copy.deepcopy(data),
            unversionedData=copy.deepcopy(unversioned_data),
            meta=copy.deepcopy(meta),
            hidden=hidden,
            revId=new_revision_id,
            savedId=new_revision_id,
            updatedBy=subject,
            updatedAt=new_updated_at,
        )

        return copy.deepcopy(previous_resp)

    def get_entry(
        self,
        entry_id: str,
        params: Optional[dict[str, Any]] = None,
        include_permissions: bool = True,
        include_links: bool = True,
        include_favorite: bool = True,
    ) -> dict[str, Any]:
        assert params is None
        try:
            previous_resp = self._saved_entries[entry_id]
        except KeyError as e:
            raise USObjectNotFoundException() from e
        else:
            return previous_resp


class MockedSyncUSManager(SyncUSManager):
    def __init__(
        self,
        bi_context: RequestContextInfo = RequestContextInfo.create_empty(),  # noqa: B008
        crypto_keys_config: Optional[CryptoKeysConfig] = None,
        services_registry: ServicesRegistry = DummyServiceRegistry(rci=RequestContextInfo.create_empty()),  # noqa: B008
    ):
        super().__init__(
            bi_context=bi_context,
            us_base_url="http://localhost:66000",
            us_api_prefix="dummy",
            crypto_keys_config=CryptoKeysConfig(  # type: ignore  # 2024-01-30 # TODO: Unexpected keyword argument "map_id_key" for "CryptoKeysConfig"  [call-arg]
                map_id_key={"dummy_usm_key": fernet.Fernet.generate_key()},
                actual_key_id="dummy_usm_key",
            )
            if crypto_keys_config is None
            else crypto_keys_config,
            us_auth_context=USAuthContextMaster("FakeKey"),
            services_registry=services_registry,
            retry_policy_factory=DefaultRetryPolicyFactory(),
        )

    def _create_us_client(self) -> UStorageClient:
        return MockedUStorageClient(
            host=self._us_base_url,
            auth_ctx=self._us_auth_context,
            prefix=self._us_api_prefix,
            retry_policy_factory=self._retry_policy_factory,
        )
