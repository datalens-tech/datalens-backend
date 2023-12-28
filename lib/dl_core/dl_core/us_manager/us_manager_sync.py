from __future__ import annotations

import contextlib
import logging
from typing import (
    TYPE_CHECKING,
    Any,
    ContextManager,
    Dict,
    Generator,
    Iterable,
    List,
    Optional,
    Set,
    Type,
    TypeVar,
    Union,
    overload,
)

from dl_api_commons.base_models import RequestContextInfo
from dl_app_tools.profiling_base import generic_profiler
from dl_configs.crypto_keys import CryptoKeysConfig
from dl_core import exc
from dl_core.base_models import (
    ConnectionRef,
    DefaultConnectionRef,
)
from dl_core.united_storage_client import (
    USAuthContextBase,
    UStorageClient,
)
from dl_core.us_connection_base import ConnectionBase
from dl_core.us_dataset import Dataset
from dl_core.us_entry import USEntry
from dl_core.us_manager.broken_link import (
    BrokenUSLink,
    BrokenUSLinkErrorKind,
)
from dl_core.us_manager.us_manager import USManagerBase
from dl_utils.aio import await_sync


if TYPE_CHECKING:
    from dl_core.lifecycle.factory_base import EntryLifecycleManagerFactoryBase
    from dl_core.services_registry import ServicesRegistry


LOGGER = logging.getLogger(__name__)

_ENTRY_TV = TypeVar("_ENTRY_TV", bound=USEntry)


class SyncUSManager(USManagerBase):
    _us_client: UStorageClient

    def __init__(
        self,
        us_auth_context: USAuthContextBase,
        us_base_url: str,
        bi_context: RequestContextInfo,
        services_registry: ServicesRegistry,
        crypto_keys_config: Optional[CryptoKeysConfig] = None,
        us_api_prefix: Optional[str] = None,
        # caches_redis: Optional[aioredis.Redis] = None,
        request_timeout_sec: int = 30,  # WARNING: unused.
        lifecycle_manager_factory: Optional[EntryLifecycleManagerFactoryBase] = None,
    ):
        super().__init__(
            bi_context=bi_context,
            crypto_keys_config=crypto_keys_config,
            us_base_url=us_base_url,
            us_api_prefix=us_api_prefix,
            us_auth_context=us_auth_context,
            services_registry=services_registry,
            lifecycle_manager_factory=lifecycle_manager_factory,
        )
        self._us_client = self._create_us_client()

    def _create_us_client(self) -> UStorageClient:
        return UStorageClient(
            host=self._us_base_url,
            prefix=self._us_api_prefix,
            auth_ctx=self._us_auth_context,
            context_request_id=self._bi_context.request_id if self._bi_context is not None else None,
            context_forwarded_for=self._bi_context.forwarder_for,
            context_workbook_id=self._bi_context.workbook_id,
        )

    def clone(self, **kwargs):  # type: ignore  # TODO: fix
        """This should've been an `attr.evolve` wrapper"""
        base_kwargs = dict(
            us_auth_context=self._us_auth_context,
            us_base_url=self._us_base_url,
            bi_context=self._bi_context,
            crypto_keys_config=self._crypto_keys_config,
            us_api_prefix=self._us_api_prefix,
            # request_timeout_sec=self._request_timeout_sec,
            services_registry=self._services_registry,
            lifecycle_manager_factory=self._lifecycle_manager_factory,
        )
        kwargs = {
            **base_kwargs,
            **kwargs,
        }
        return self.__class__(**kwargs)

    def close(self) -> None:
        self._us_client.close()

    def __enter__(self) -> SyncUSManager:
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        try:
            self.close()
        except Exception:
            LOGGER.warning("Error during closing SyncUSManager", exc_info=True)

    # CRUD
    #
    def save(self, entry: USEntry) -> None:
        lifecycle_manager = self.get_lifecycle_manager(entry=entry, service_registry=self._services_registry)
        lifecycle_manager.pre_save_hook()

        save_params = self._get_entry_save_params(entry)
        us_scope = save_params.pop("scope")
        us_type = save_params.pop("type")
        entry_loc = entry.entry_key
        assert "data" in save_params and "unversioned_data" in save_params
        assert entry_loc is not None, "Can not save entry without key/workbook data"

        # noinspection PyProtectedMember
        if not entry._stored_in_db:
            resp = self._us_client.create_entry(
                key=entry_loc,
                scope=us_scope,
                type_=us_type,
                **save_params,
            )
            entry.uuid = resp["entryId"]
            entry._stored_in_db = True
        else:
            # noinspection PyProtectedMember
            resp = self._us_client.update_entry(
                entry.uuid, lock=entry._lock, **save_params  # type: ignore  # TODO: fix
            )

        entry._us_resp = resp  # type: ignore  # TODO: fix
        lifecycle_manager.post_save_hook()

    def delete(self, entry: USEntry) -> None:
        lifecycle_manager = self.get_lifecycle_manager(entry=entry)
        lifecycle_manager.pre_delete_hook()

        self._us_client.delete_entry(entry.uuid, lock=entry._lock)
        entry._stored_in_db = False
        # noinspection PyBroadException
        try:
            LOGGER.info("Executing post-delete hook %s", entry.uuid)
            lifecycle_manager.post_delete_hook()
        except Exception:
            LOGGER.exception("Error during post-delete hook execution for entry %s", entry.uuid)

    @overload
    def get_by_id(self, entry_id: str, expected_type: type(None) = None) -> USEntry:  # type: ignore  # TODO: fix
        pass

    @overload  # noqa
    def get_by_id(self, entry_id: str, expected_type: Type[_ENTRY_TV] = None) -> _ENTRY_TV:  # type: ignore  # TODO: fix
        pass

    @generic_profiler("us-fetch-entity")
    def get_by_id(self, entry_id: str, expected_type: Optional[Type[USEntry]] = None) -> USEntry:
        with self._enrich_us_exception(
            entry_id=entry_id,
            entry_scope=expected_type.scope if expected_type is not None else None,
        ):
            us_resp = self._us_client.get_entry(entry_id)
        obj = self._entry_dict_to_obj(us_resp, expected_type)
        await_sync(self.get_lifecycle_manager(entry=obj).post_init_async_hook())
        return obj

    @overload
    def get_by_key(self, entry_key: str, expected_type: type(None) = None) -> USEntry:  # type: ignore  # TODO: fix
        pass

    @overload  # noqa
    def get_by_key(self, entry_key: str, expected_type: Type[_ENTRY_TV] = None) -> _ENTRY_TV:  # type: ignore  # TODO: fix
        pass

    def get_by_key(self, entry_key: str, expected_type=None):  # type: ignore  # TODO: fix  # noqa
        raise NotImplementedError()

    def get_collection(
        self,
        entry_cls: Optional[Type[_ENTRY_TV]],
        entry_type: Optional[str] = None,
        entry_scope: Optional[str] = None,
        meta: Optional[Dict[str, Union[str, int, None]]] = None,
        all_tenants: bool = False,
        include_data: bool = True,
        ids: Optional[Iterable[str]] = None,
        creation_time: Optional[Dict[str, Union[str, int, None]]] = None,
        raise_on_broken_entry: bool = False,
    ) -> Generator[_ENTRY_TV, None, None]:
        if all_tenants and include_data:
            # Not supported; see _req_data_iter_entries
            raise ValueError("all_tenants and include_data cannot both be True")
        if entry_scope and entry_cls.scope:  # type: ignore  # TODO: fix
            raise ValueError("US scope can not be provided in entry class and in parameters simultaneously")
        if not entry_scope and not entry_cls.scope:  # type: ignore  # TODO: fix
            raise ValueError("US scope not provided neither in entry class nor in parameters")

        effective_scope = entry_scope or entry_cls.scope  # type: ignore  # TODO: fix
        effective_type = entry_type or entry_cls.type_  # type: ignore  # TODO: fix
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

        for us_resp in us_entry_iterator:
            # noinspection PyBroadException
            try:
                yield self._entry_dict_to_obj(us_resp, expected_type=entry_cls)  # type: ignore  # TODO: fix
            except Exception:
                LOGGER.exception("Failed to load US object: %s", us_resp)
                if raise_on_broken_entry:
                    raise

    def get_raw_entry(self, entry_id: str, include_permissions: bool = True, include_links: bool = True) -> dict:
        return self._us_client.get_entry(entry_id, include_permissions=include_permissions, include_links=include_links)

    def get_raw_collection(
        self,
        entry_scope: str,
        entry_type: Optional[str] = None,
        all_tenants: bool = False,
    ) -> Iterable[dict]:
        return self._us_client.entries_iterator(
            scope=entry_scope,
            entry_type=entry_type,
            all_tenants=all_tenants,
            include_data=False,
        )

    def move(self, entry: USEntry, destination: str) -> None:
        self._us_client.move_entry(entry.uuid, destination)
        self.reload_data(entry)  # maybe we have to update only entry_key

    def reload_data(self, entry: USEntry) -> None:
        assert entry.uuid is not None
        us_resp = self._us_client.get_entry(entry.uuid)
        reloaded_entry = self._entry_dict_to_obj(us_resp, expected_type=type(entry))
        entry.data = reloaded_entry.data
        entry._us_resp = us_resp

    # Locks
    #
    def acquire_lock(self, entry: USEntry, duration: Optional[int] = None, wait_timeout: Optional[int] = None) -> str:
        lock_token = self._us_client.acquire_lock(entry.uuid, duration, wait_timeout)
        entry._lock = lock_token
        return lock_token

    def release_lock(self, entry: USEntry) -> None:
        # noinspection PyProtectedMember
        if entry._lock:
            self._us_client.release_lock(entry.uuid, entry._lock)
            entry._lock = None

    @contextlib.contextmanager  # type: ignore  # TODO: fix
    def locked_cm(  # type: ignore  # TODO: fix
        self, entry: USEntry, duration: Optional[int] = None, wait_timeout: Optional[int] = None
    ) -> ContextManager:
        self.acquire_lock(entry, duration=duration, wait_timeout=wait_timeout)
        try:
            yield
        finally:
            self.release_lock(entry)

    @contextlib.contextmanager  # type: ignore  # TODO: fix
    def get_locked_entry_cm(  # type: ignore  # TODO: fix
        self,
        expected_type: Type[_ENTRY_TV],
        entry_id: str,
        duration: Optional[int] = None,
        wait_timeout: Optional[int] = None,
    ) -> ContextManager[_ENTRY_TV]:
        lock_token = self._us_client.acquire_lock(entry_id, duration, wait_timeout)
        entry = None
        try:
            entry = self.get_by_id(entry_id, expected_type=expected_type)
            entry._lock = lock_token
            yield entry
        finally:
            self._us_client.release_lock(entry_id, lock_token)
            if entry is not None:
                entry._lock = None

    # Dependencies
    #
    def get_loaded_us_connection(self, identity: Union[str, ConnectionRef]) -> ConnectionBase:
        # Temporary workaround to mitigate forgotten .load_dependencies()
        if isinstance(identity, str):
            identity = DefaultConnectionRef(conn_id=identity)

        if identity not in self._loaded_entries:
            # TODO FIX: Uncomment after migration to dataset v7
            # _log.warning("SyncUSManager.get_loaded_us_connection() called without connection in cache")
            self._ensure_conn_in_cache(None, identity)
        return super().get_loaded_us_connection(identity)

    def ensure_entry_preloaded(self, conn_ref: ConnectionRef) -> None:
        self._ensure_conn_in_cache(None, conn_ref)

    def _ensure_conn_in_cache(self, referrer: Optional[USEntry], conn_ref: ConnectionRef) -> Optional[ConnectionBase]:
        conn: Union[USEntry, BrokenUSLink]
        if conn_ref in self._loaded_entries:
            conn = self._loaded_entries[conn_ref]
        else:
            try:
                assert isinstance(conn_ref, DefaultConnectionRef)
                conn = self.get_by_id(conn_ref.conn_id, ConnectionBase)
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
    def load_dependencies(self, entry: USEntry) -> None:
        if not isinstance(entry, Dataset):
            raise NotImplementedError("Links loading is supported only for dataset")

        processed_refs: Set[ConnectionRef] = set()
        # TODO FIX: Find a way to track direct source of link
        refs_to_load_queue = self._get_entry_links(
            entry,
        )

        while refs_to_load_queue:
            ref = refs_to_load_queue.pop()
            processed_refs.add(ref)
            try:
                resolved_ref = self._ensure_conn_in_cache(referrer=entry, conn_ref=ref)
            except Exception:
                LOGGER.exception("Can not load linked US entry %s for entry %s", ref, entry.uuid)
                raise

            refs_to_load_queue.update(
                self._get_entry_links(
                    resolved_ref,  # type: ignore  # TODO: fix
                )
                - processed_refs
            )

    def load_get_entries_at_path(self, us_path: str) -> list[dict[str, Any]]:
        return self._us_client.get_entries_info_in_path(us_path)
