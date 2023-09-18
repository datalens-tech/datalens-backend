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
from dl_core.united_storage_client import USAuthContextBase
from dl_core.united_storage_client_aio import UStorageClientAIO
from dl_core.us_connection_base import ConnectionBase
from dl_core.us_dataset import Dataset
from dl_core.us_entry import USEntry
from dl_core.us_manager.broken_link import (
    BrokenUSLink,
    BrokenUSLinkErrorKind,
)
from dl_core.us_manager.us_manager import USManagerBase
from dl_utils.aio import shield_wait_for_complete

if TYPE_CHECKING:
    from dl_core.lifecycle.factory_base import EntryLifecycleManagerFactoryBase
    from dl_core.services_registry import ServicesRegistry


LOGGER = logging.getLogger(__name__)

_ENTRY_TV = TypeVar("_ENTRY_TV", bound=USEntry)


class AsyncUSManager(USManagerBase):
    _us_client: UStorageClientAIO

    def __init__(
        self,
        us_auth_context: USAuthContextBase,
        us_base_url: str,
        bi_context: RequestContextInfo,
        services_registry: ServicesRegistry,
        crypto_keys_config: Optional[CryptoKeysConfig] = None,
        us_api_prefix: Optional[str] = None,
        lifecycle_manager_factory: Optional[EntryLifecycleManagerFactoryBase] = None,
    ):
        self._us_client = UStorageClientAIO(
            host=us_base_url,
            prefix=us_api_prefix,
            auth_ctx=us_auth_context,
            context_request_id=bi_context.request_id if bi_context is not None else None,
            context_forwarded_for=bi_context.forwarder_for,
        )

        super().__init__(
            bi_context=bi_context,
            crypto_keys_config=crypto_keys_config,
            us_base_url=us_base_url,
            us_api_prefix=us_api_prefix,
            us_auth_context=us_auth_context,
            services_registry=services_registry,
            lifecycle_manager_factory=lifecycle_manager_factory,
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
    async def get_by_id(self, entry_id: str, expected_type: type(None) = None) -> USEntry:  # type: ignore  # TODO: fix
        pass

    @overload  # noqa
    async def get_by_id(self, entry_id: str, expected_type: Type[_ENTRY_TV] = None) -> _ENTRY_TV:  # type: ignore  # TODO: fix
        pass

    @generic_profiler_async("us-fetch-entity")  # type: ignore  # TODO: fix
    async def get_by_id(self, entry_id: str, expected_type: Type[_ENTRY_TV] = None) -> _ENTRY_TV:  # type: ignore  # TODO: fix
        with self._enrich_us_exception(
            entry_id=entry_id,
            entry_scope=expected_type.scope if expected_type is not None else None,
        ):
            us_resp = await self._us_client.get_entry(entry_id)

        obj: _ENTRY_TV = self._entry_dict_to_obj(us_resp, expected_type)  # type: ignore  # TODO: fix
        await self.get_lifecycle_manager(entry=obj).post_init_async_hook()

        return obj

    async def save(self, entry: USEntry) -> None:
        self.get_lifecycle_manager(entry=entry).pre_save_hook()

        save_params = self._get_entry_save_params(entry)
        us_scope = save_params.pop("scope")
        us_type = save_params.pop("type")
        assert "data" in save_params and "unversioned_data" in save_params

        # noinspection PyProtectedMember
        if not entry._stored_in_db:
            entry_loc = entry.entry_key
            assert entry_loc is not None, "Entry location must be set before saving US entry"

            resp = await self._us_client.create_entry(
                entry_loc,
                scope=us_scope,
                type_=us_type,
                **save_params,
            )
            entry.uuid = resp["entryId"]
            entry._stored_in_db = True
        else:
            # noinspection PyProtectedMember
            resp = await self._us_client.update_entry(entry.uuid, lock=entry._lock, **save_params)

        entry._us_resp = resp

    async def delete(self, entry: USEntry) -> None:
        # TODO FIX: Use pre_delete_async_hook!!!
        self.get_lifecycle_manager(entry=entry).pre_delete_hook()

        if entry.uuid is None:
            raise ValueError("Entry has no id")

        # noinspection PyProtectedMember
        await self._us_client.delete_entry(entry.uuid, lock=entry._lock)
        entry._stored_in_db = False

        # noinspection PyBroadException
        try:
            # TODO FIX: Use post_delete_async_hook!!!
            self.get_lifecycle_manager(entry=entry).post_delete_hook()
        except Exception:
            LOGGER.exception("Error during post-delete hook execution for entry %s", entry.uuid)

    async def reload_data(self, entry: USEntry) -> None:
        assert entry.uuid is not None
        us_resp = await self._us_client.get_entry(entry.uuid)
        reloaded_entry = self._entry_dict_to_obj(us_resp, expected_type=type(entry))
        entry.data = reloaded_entry.data
        entry._us_resp = us_resp

    @asynccontextmanager  # type: ignore  # TODO: fix
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
            entry = await self.get_by_id(entry_id, expected_type)  # type: ignore
            entry._lock = lock_token  # type: ignore  # TODO: fix
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
                conn.add_referrer_id(referrer.uuid)  # type: ignore  # TODO: fix
            return None
        elif isinstance(conn, ConnectionBase):
            return conn
        else:
            raise ValueError("Entry was in cache but it is not a connection: %s", type(conn))

    # TODO FIX: Think about cache control
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
                    resolved_ref,  # type: ignore  # TODO: fix
                )
                - processed_refs
            )

    def get_raw_collection(
        self,
        entry_scope: str,
        entry_type: Optional[str] = None,
        all_tenants: bool = False,
    ) -> AsyncIterable[dict]:
        return self._us_client.entries_iterator(
            scope=entry_scope,
            entry_type=entry_type,
            all_tenants=all_tenants,
            include_data=False,
        )

    async def get_collection(
        self,
        entry_cls: Optional[Type[USEntry]],
        entry_type: Optional[str] = None,
        entry_scope: Optional[str] = None,
        meta: Optional[dict[str, Union[str, int, None]]] = None,
        all_tenants: bool = False,
        include_data: bool = True,
        ids: Optional[Iterable[str]] = None,
        creation_time: Optional[dict[str, Union[str, int, None]]] = None,
        raise_on_broken_entry: bool = False,
    ) -> AsyncGenerator[USEntry, None]:
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
        )

        async for us_resp in us_entry_iterator:
            if us_resp:
                # noinspection PyBroadException
                try:
                    obj: USEntry = self._entry_dict_to_obj(us_resp, entry_cls)
                    yield obj
                except Exception:
                    LOGGER.exception("Failed to load US object: %s", us_resp)
                    if raise_on_broken_entry:
                        raise
