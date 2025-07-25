from __future__ import annotations

from contextlib import asynccontextmanager
import logging
from typing import (
    TYPE_CHECKING,
    Any,
    AsyncGenerator,
    AsyncIterable,
    Iterable,
    Optional,
    Type,
    TypeVar,
    Union,
    overload,
)

from dl_api_commons.base_models import RequestContextInfo
from dl_app_tools.profiling_base import generic_profiler_async
from dl_configs.crypto_keys import CryptoKeysConfig
from dl_core import exc
from dl_core.base_models import (
    ConnectionRef,
    DefaultConnectionRef,
)
from dl_core.retrier.policy import BaseRetryPolicyFactory
from dl_core.united_storage_client import USAuthContextBase
from dl_core.united_storage_client_aio import UStorageClientAIO
from dl_core.us_connection_base import ConnectionBase
from dl_core.us_dataset import Dataset
from dl_core.us_entry import USEntry
from dl_core.us_manager.broken_link import (
    BrokenUSLink,
    BrokenUSLinkErrorKind,
)
from dl_core.us_manager.schema_migration.factory_base import EntrySchemaMigrationFactoryBase
from dl_core.us_manager.us_manager import USManagerBase
from dl_utils.aio import shield_wait_for_complete


if TYPE_CHECKING:
    from dl_core.lifecycle.factory_base import EntryLifecycleManagerFactoryBase
    from dl_core.services_registry import ServicesRegistry


LOGGER = logging.getLogger(__name__)

_ENTRY_TV = TypeVar("_ENTRY_TV", bound=USEntry)


class AsyncUSManager(USManagerBase):
    _us_client: UStorageClientAIO
    _ca_data: bytes

    def __init__(
        self,
        us_auth_context: USAuthContextBase,
        us_base_url: str,
        ca_data: bytes,
        bi_context: RequestContextInfo,
        services_registry: ServicesRegistry,
        retry_policy_factory: BaseRetryPolicyFactory,
        crypto_keys_config: Optional[CryptoKeysConfig] = None,
        us_api_prefix: Optional[str] = None,
        lifecycle_manager_factory: Optional[EntryLifecycleManagerFactoryBase] = None,
        schema_migration_factory: Optional[EntrySchemaMigrationFactoryBase] = None,
    ):
        self._us_client = UStorageClientAIO(
            host=us_base_url,
            prefix=us_api_prefix,
            auth_ctx=us_auth_context,
            context_request_id=bi_context.request_id,
            context_forwarded_for=bi_context.forwarder_for,
            context_workbook_id=bi_context.workbook_id,
            ca_data=ca_data,
            retry_policy_factory=retry_policy_factory,
        )
        self._ca_data = ca_data

        super().__init__(
            bi_context=bi_context,
            crypto_keys_config=crypto_keys_config,
            us_base_url=us_base_url,
            us_api_prefix=us_api_prefix,
            us_auth_context=us_auth_context,
            services_registry=services_registry,
            lifecycle_manager_factory=lifecycle_manager_factory,
            schema_migration_factory=schema_migration_factory,
            retry_policy_factory=retry_policy_factory,
        )

    @property
    def us_client(self) -> UStorageClientAIO:
        return self._us_client

    def clone(self) -> AsyncUSManager:
        return AsyncUSManager(
            crypto_keys_config=self._crypto_keys_config,
            us_auth_context=self._us_auth_context,
            us_base_url=self._us_base_url,
            us_api_prefix=self._us_api_prefix,
            bi_context=self._bi_context,
            services_registry=self._services_registry,
            lifecycle_manager_factory=self._lifecycle_manager_factory,
            schema_migration_factory=self._schema_migration_factory,
            ca_data=self._ca_data,
            retry_policy_factory=self._retry_policy_factory,
        )

    async def close(self) -> None:
        await self._us_client.close()

    async def __aenter__(self) -> AsyncUSManager:
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        try:
            await self.close()
        except Exception:  # noqa
            LOGGER.warning("Error during closing AsyncUSManager", exc_info=True)

    @overload
    async def get_by_id(
        self,
        entry_id: str,
        expected_type: None = None,
        params: Optional[dict[str, str]] = None,
    ) -> USEntry:
        pass

    @overload
    async def get_by_id(
        self,
        entry_id: str,
        expected_type: Optional[Type[_ENTRY_TV]] = None,
        params: Optional[dict[str, str]] = None,
    ) -> _ENTRY_TV:
        pass

    @generic_profiler_async("us-fetch-entity")  # type: ignore  # TODO: fix
    async def get_by_id(
        self,
        entry_id: str,
        expected_type: Optional[Type[USEntry]] = None,
        params: Optional[dict[str, str]] = None,
    ) -> USEntry:
        with self._enrich_us_exception(
            entry_id=entry_id,
            entry_scope=expected_type.scope if expected_type is not None else None,
        ):
            us_resp = await self.get_migrated_entry(entry_id, params=params)

        obj = self._entry_dict_to_obj(us_resp, expected_type)
        await self.get_lifecycle_manager(entry=obj).post_init_async_hook()

        return obj

    @generic_profiler_async("us-fetch-entity-raw")  # type: ignore  # TODO: fix
    async def get_by_id_raw(
        self,
        entry_id: str,
        expected_type: Optional[Type[USEntry]] = None,
        params: Optional[dict[str, str]] = None,
    ) -> dict[str, Any]:
        """Get raw `us_resp` from response without deserialization"""

        with self._enrich_us_exception(
            entry_id=entry_id,
            entry_scope=expected_type.scope if expected_type is not None else None,
        ):
            us_resp = await self.get_migrated_entry(entry_id, params=params)

        return us_resp

    @generic_profiler_async("us-deserialize-entity-raw")  # type: ignore  # TODO: fix
    async def deserialize_us_resp(
        self,
        us_resp: dict[str, Any],
        expected_type: Optional[Type[USEntry]] = None,
    ) -> USEntry:
        """Used on result of `get_by_id_raw()` call for proper deserialization flow"""

        obj = self._entry_dict_to_obj(us_resp, expected_type)
        await self.get_lifecycle_manager(entry=obj).post_init_async_hook()

        return obj

    @generic_profiler_async("us-get-migrated-entity")  # type: ignore  # TODO: fix
    async def get_migrated_entry(self, entry_id: str, params: Optional[dict[str, str]] = None) -> dict[str, Any]:
        us_resp = await self._us_client.get_entry(entry_id, params=params)
        return await self._migrate_response(us_resp)

    async def _migrate_response(self, us_resp: dict) -> dict:
        initial_type = us_resp["type"]
        while True:
            schema_migration = self.get_schema_migration(
                entry_scope=us_resp["scope"],
                entry_type=us_resp["type"],
            )
            us_resp = await schema_migration.migrate_async(us_resp)
            if us_resp["type"] == initial_type:
                break
        return us_resp

    async def save(self, entry: USEntry, update_revision: Optional[bool] = None) -> None:
        lifecycle_manager = self.get_lifecycle_manager(entry=entry)
        lifecycle_manager.pre_save_hook()

        save_params = self._get_entry_save_params(entry)
        us_scope = save_params.pop("scope")
        us_type = save_params.pop("type")
        assert "data" in save_params and "unversioned_data" in save_params

        if not entry.stored_in_db:
            entry_loc = entry.entry_key
            assert entry_loc is not None, "Entry location must be set before saving US entry"

            resp = await self._us_client.create_entry(
                entry_loc,
                scope=us_scope,
                type_=us_type,
                **save_params,
            )
            entry.uuid = resp["entryId"]
            entry.stored_in_db = True
        else:
            save_params["update_revision"] = update_revision
            assert entry.uuid is not None
            resp = await self._us_client.update_entry(entry.uuid, lock=entry._lock, **save_params)

        entry._us_resp = resp

    async def delete(self, entry: USEntry) -> None:
        # TODO FIX: Use pre_delete_async_hook!!!
        self.get_lifecycle_manager(entry=entry).pre_delete_hook()

        if entry.uuid is None:
            raise ValueError("Entry has no id")

        await self._us_client.delete_entry(entry.uuid, lock=entry._lock)
        entry.stored_in_db = False

        try:
            # TODO FIX: Use post_delete_async_hook!!!
            self.get_lifecycle_manager(entry=entry).post_delete_hook()
        except Exception:
            LOGGER.exception("Error during post-delete hook execution for entry %s", entry.uuid)

    async def reload_data(self, entry: USEntry) -> None:
        assert entry.uuid is not None
        us_resp = await self.get_migrated_entry(entry.uuid)
        reloaded_entry = self._entry_dict_to_obj(us_resp, expected_type=type(entry))
        entry.data = reloaded_entry.data
        entry._us_resp = us_resp

    @asynccontextmanager
    async def locked_entry_cm(
        self,
        entry_id: str,
        expected_type: Type[_ENTRY_TV],
        wait_timeout_sec: int = 30,
        duration_sec: int = 300,
        force: bool = False,
    ) -> AsyncGenerator[_ENTRY_TV, None]:
        entry: Optional[_ENTRY_TV] = None
        lock_token = await self._us_client.acquire_lock(
            entry_id,
            wait_timeout=wait_timeout_sec,
            duration=duration_sec,
            force=force,
        )

        try:
            entry = await self.get_by_id(entry_id, expected_type)
            entry._lock = lock_token
            assert entry is not None
            yield entry

        finally:
            await shield_wait_for_complete(self._us_client.release_lock(entry_id, lock_token))
            if entry is not None and hasattr(entry, "_lock"):
                entry._lock = None

    async def ensure_entry_preloaded(self, conn_ref: ConnectionRef) -> None:
        await self._ensure_conn_in_cache(None, conn_ref)

    async def _ensure_conn_in_cache(
        self, referrer: Optional[USEntry], conn_ref: ConnectionRef
    ) -> Optional[ConnectionBase]:
        conn: Union[USEntry, BrokenUSLink]
        if conn_ref in self._loaded_entries:
            conn = self._loaded_entries[conn_ref]
        else:
            try:
                assert isinstance(conn_ref, DefaultConnectionRef)
                conn = await self.get_by_id(conn_ref.conn_id, ConnectionBase)
            except (exc.USObjectNotFoundException, exc.USAccessDeniedException) as err:
                r_scope = referrer.scope if referrer is not None else "unk"
                r_uuid = referrer.uuid if referrer is not None else "unk"
                LOGGER.info(
                    "Can not load dependency for parent=%s/%s: connection %s (%s)",
                    r_scope,
                    r_uuid,
                    conn_ref,
                    err.message,
                )
                err_kind = {
                    exc.USObjectNotFoundException: BrokenUSLinkErrorKind.NOT_FOUND,
                    exc.USAccessDeniedException: BrokenUSLinkErrorKind.ACCESS_DENIED,
                }.get(type(err), BrokenUSLinkErrorKind.OTHER)
                conn = BrokenUSLink(reference=conn_ref, error_kind=err_kind)
            self._loaded_entries[conn_ref] = conn

        if isinstance(conn, BrokenUSLink):
            if referrer is not None:
                assert referrer.uuid is not None
                conn.add_referrer_id(referrer.uuid)
            return None
        elif isinstance(conn, ConnectionBase):
            return conn
        else:
            raise ValueError("Entry was in cache but it is not a connection: %s", type(conn))

    # TODO FIX: Think about cache control
    @generic_profiler_async("us-load-dependencies")  # type: ignore  # TODO: fix
    async def load_dependencies(self, entry: USEntry) -> None:
        if not isinstance(entry, Dataset):
            raise NotImplementedError("Links loading is supported only for dataset")

        processed_refs: set[ConnectionRef] = set()
        # TODO FIX: Find a way to track direct source of link
        refs_to_load_queue = self._get_entry_links(
            entry,
        )

        while refs_to_load_queue:
            ref = refs_to_load_queue.pop()
            processed_refs.add(ref)
            try:
                resolved_ref = await self._ensure_conn_in_cache(referrer=entry, conn_ref=ref)
            except Exception:
                LOGGER.exception("Can not load linked US entry %s for entry %s", ref, entry.uuid)
                raise

            refs_to_load_queue.update(
                self._get_entry_links(
                    resolved_ref,
                )
                - processed_refs
            )

    def get_raw_collection(
        self,
        entry_scope: str,
        entry_type: Optional[str] = None,
        all_tenants: bool = False,
        creation_time: Optional[dict[str, Union[str, int, None]]] = None,
    ) -> AsyncIterable[dict]:
        return self._us_client.entries_iterator(
            scope=entry_scope,
            entry_type=entry_type,
            all_tenants=all_tenants,
            creation_time=creation_time,
            include_data=False,
            limit=self.ITER_ENTRIES_PAGE_SIZE,
        )

    async def get_collection(
        self,
        entry_cls: Optional[Type[_ENTRY_TV]],
        entry_type: Optional[str] = None,
        entry_scope: Optional[str] = None,
        meta: Optional[dict[str, Union[str, int, None]]] = None,
        all_tenants: bool = False,
        include_data: bool = True,
        ids: Optional[Iterable[str]] = None,
        creation_time: Optional[dict[str, Union[str, int, None]]] = None,
        raise_on_broken_entry: bool = False,
    ) -> AsyncGenerator[_ENTRY_TV, None]:
        if all_tenants and include_data:
            raise ValueError("all_tenants and include_data cannot both be True")

        effective_scope = entry_scope
        if entry_cls and entry_cls.scope:
            if effective_scope:
                raise ValueError("US scope can not be provided in entry class and in parameters simultaneously")
            effective_scope = entry_cls.scope
        if effective_scope is None:
            raise ValueError("US scope not provided neither in entry class nor in parameters")

        effective_type = entry_type
        if not effective_type and entry_cls and entry_cls.type_:
            effective_type = entry_cls.type_
        assert isinstance(effective_scope, str)

        us_entry_iterator = self._us_client.entries_iterator(
            scope=effective_scope,
            entry_type=effective_type,
            meta=meta,
            all_tenants=all_tenants,
            include_data=include_data,
            ids=ids,
            creation_time=creation_time,
            limit=self.ITER_ENTRIES_PAGE_SIZE,
        )

        async for us_resp in us_entry_iterator:
            try:
                us_resp = await self._migrate_response(us_resp)
                yield self._entry_dict_to_obj(us_resp, expected_type=entry_cls)
            except Exception:
                LOGGER.exception("Failed to load US object: %s", us_resp)
                if raise_on_broken_entry:
                    raise
