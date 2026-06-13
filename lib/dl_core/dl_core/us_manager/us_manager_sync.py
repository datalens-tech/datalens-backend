from __future__ import annotations

from collections.abc import (
    Generator,
    Iterable,
)
import contextlib
import logging
from typing import (
    TYPE_CHECKING,
    Any,
    Self,
    TypeVar,
    overload,
)

import typing_extensions

from dl_api_commons.base_models import RequestContextInfo
from dl_app_tools.profiling_base import generic_profiler
from dl_configs.crypto_keys import CryptoKeysConfig
from dl_constants.enums import (
    ConnectionType,
    DataSourceType,
)
from dl_core import exc
from dl_core.base_models import (
    ConnectionRef,
    DefaultConnectionRef,
)
from dl_core.components.accessor import DatasetComponentAccessor
from dl_core.data_source.type_mapping import get_connection_type_for_source_type
from dl_core.enums import (
    USEntryBranch,
    USEntryMode,
)
from dl_core.united_storage_client import (
    USAuthContextBase,
    UStorageClient,
)
from dl_core.us_connection import get_connection_class
from dl_core.us_connection_base import ConnectionBase
from dl_core.us_dataset import Dataset
from dl_core.us_entry import USEntry
from dl_core.us_manager.broken_link import (
    BrokenUSLink,
    BrokenUSLinkErrorKind,
)
from dl_core.us_manager.schema_migration.factory_base import EntrySchemaMigrationFactoryBase
from dl_core.us_manager.us_manager import USManagerBase
import dl_retrier
from dl_utils.aio import await_sync

if TYPE_CHECKING:
    from dl_constants.enums import ConnectionType
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
        retry_policy_factory: dl_retrier.BaseRetryPolicyFactory,
        crypto_keys_config: CryptoKeysConfig | None = None,
        us_api_prefix: str | None = None,
        # caches_redis: Optional[aioredis.Redis] = None,
        lifecycle_manager_factory: EntryLifecycleManagerFactoryBase | None = None,
        schema_migration_factory: EntrySchemaMigrationFactoryBase | None = None,
    ):
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
        self._us_client = self._create_us_client()

    def _create_us_client(self) -> UStorageClient:
        return UStorageClient(
            host=self._us_base_url,
            prefix=self._us_api_prefix,
            auth_ctx=self._us_auth_context,
            context_request_id=self._bi_context.request_id if self._bi_context is not None else None,
            context_forwarded_for=self._bi_context.forwarded_for,
            context_real_ip=self._bi_context.real_ip,
            context_workbook_id=self._bi_context.workbook_id,
            retry_policy_factory=self._retry_policy_factory,
        )

    def clone(self, **kwargs: Any) -> Self:
        """This should've been an `attr.evolve` wrapper"""
        base_kwargs = {
            "us_auth_context": self._us_auth_context,
            "us_base_url": self._us_base_url,
            "bi_context": self._bi_context,
            "crypto_keys_config": self._crypto_keys_config,
            "us_api_prefix": self._us_api_prefix,
            "services_registry": self._services_registry,
            "lifecycle_manager_factory": self._lifecycle_manager_factory,
            "schema_migration_factory": self._schema_migration_factory,
            "retry_policy_factory": self._retry_policy_factory,
        }
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
    @typing_extensions.deprecated("Use create/update instead", category=DeprecationWarning)
    def save(
        self,
        entry: USEntry,
        update_revision: bool | None = None,
        original_entry: USEntry | None = None,
        mode: USEntryMode = USEntryMode.publish,
    ) -> None:
        self._save(
            entry=entry,
            update_revision=update_revision,
            original_entry=original_entry,
            mode=mode,
        )

    def _save(
        self,
        entry: USEntry,
        update_revision: bool | None = None,
        original_entry: USEntry | None = None,
        mode: USEntryMode = USEntryMode.publish,
    ) -> None:
        """
        Save USEntry to US.

        :param entry: US entry to save
        :param update_revision: Update revision on save
        :param original_entry: Previous version of the entry for lifecycle hook handling
        """

        lifecycle_manager = self.get_lifecycle_manager(
            entry=entry,
            service_registry=self._services_registry,
            original_entry=original_entry,
        )
        lifecycle_manager.pre_save_hook()

        save_params = self._get_entry_save_params(entry)
        us_scope = save_params.pop("scope")
        us_type = save_params.pop("type")
        entry_loc = entry.entry_key
        assert "data" in save_params
        assert "unversioned_data" in save_params
        assert entry_loc is not None, "Can not save entry without key/workbook data"

        if not entry.stored_in_db:
            resp = self._us_client.create_entry(
                key=entry_loc,
                scope=us_scope,
                type_=us_type,
                mode=mode,
                **save_params,
            )
            entry.uuid = resp["entryId"]
            entry.stored_in_db = True
        else:
            save_params["update_revision"] = update_revision
            assert entry.uuid is not None
            resp = self._us_client.update_entry(
                entry_id=entry.uuid,
                lock=entry.lock,
                mode=mode,
                **save_params,
            )

        entry._us_resp = resp

        post_save_result = lifecycle_manager.post_save_hook()
        if post_save_result.additional_save_needed:
            save_params = self._prepare_update_entry_params(entry, False)
            assert entry.uuid is not None
            entry._us_resp = self._us_client.update_entry(entry.uuid, lock=entry.lock, **save_params)

    def create(
        self,
        entry: USEntry,
        update_revision: bool | None = None,
        mode: USEntryMode = USEntryMode.publish,
    ) -> None:
        """
        Create entry - alias for save without previous entry.
        """

        self._save(
            entry=entry,
            original_entry=None,
            update_revision=update_revision,
            mode=mode,
        )

    def update(
        self,
        entry: USEntry,
        original_entry: USEntry | None = None,
        update_revision: bool | None = None,
        mode: USEntryMode = USEntryMode.publish,
    ) -> None:
        """
        Update entry - alias for save with a previous/original entry.
        """

        self._save(
            entry=entry,
            original_entry=original_entry,
            update_revision=update_revision,
            mode=mode,
        )

    def delete(self, entry: USEntry) -> None:
        lifecycle_manager = self.get_lifecycle_manager(entry=entry)
        lifecycle_manager.pre_delete_hook()

        assert entry.uuid is not None
        self._us_client.delete_entry(entry.uuid, lock=entry.lock)
        entry.stored_in_db = False
        try:
            LOGGER.info("Executing post-delete hook %s", entry.uuid)
            lifecycle_manager.post_delete_hook()
        except Exception:
            LOGGER.exception("Error during post-delete hook execution for entry %s", entry.uuid)

    @overload
    def get_by_id(
        self,
        entry_id: str,
        expected_type: None = None,
        params: dict[str, str] | None = None,
        context_name: str | None = None,
        branch: USEntryBranch = USEntryBranch.published,
    ) -> USEntry:
        pass

    @overload
    def get_by_id(
        self,
        entry_id: str,
        expected_type: type[_ENTRY_TV] | None = None,
        params: dict[str, str] | None = None,
        context_name: str | None = None,
        branch: USEntryBranch = USEntryBranch.published,
    ) -> _ENTRY_TV:
        pass

    @generic_profiler("us-fetch-entity")
    def get_by_id(
        self,
        entry_id: str,
        expected_type: type[USEntry] | None = None,
        params: dict[str, str] | None = None,
        context_name: str | None = None,
        branch: USEntryBranch = USEntryBranch.published,
    ) -> USEntry:
        with self._enrich_us_exception(
            entry_id=entry_id,
            entry_scope=expected_type.scope if expected_type is not None else None,
        ):
            us_resp = self.get_migrated_entry(
                entry_id,
                params=params,
                context_name=context_name,
                branch=branch,
            )

        obj = self._entry_dict_to_obj(us_resp, expected_type)
        await_sync(self.get_lifecycle_manager(entry=obj).post_init_async_hook())

        return obj

    @generic_profiler("us-fetch-entity-raw")
    def get_by_id_raw(
        self,
        entry_id: str,
        expected_type: type[USEntry] | None = None,
        params: dict[str, str] | None = None,
    ) -> dict[str, Any]:
        """Get raw `u_resp` from response without deserialization"""

        with self._enrich_us_exception(
            entry_id=entry_id,
            entry_scope=expected_type.scope if expected_type is not None else None,
        ):
            return self.get_migrated_entry(entry_id, params=params)

    @generic_profiler("us-deserialize-entity-raw")
    def deserialize_us_resp(
        self,
        us_resp: dict[str, Any],
        expected_type: type[USEntry] | None = None,
    ) -> USEntry:
        """Used on result of `get_by_id_raw()` call for proper deserialization flow"""

        obj = self._entry_dict_to_obj(us_resp, expected_type)
        await_sync(self.get_lifecycle_manager(entry=obj).post_init_async_hook())

        return obj

    @generic_profiler("us-get-migrated-entity")
    def get_migrated_entry(
        self,
        entry_id: str,
        params: dict[str, str] | None = None,
        context_name: str | None = None,
        branch: USEntryBranch = USEntryBranch.published,
    ) -> dict[str, Any]:
        us_resp = self._us_client.get_entry(
            entry_id,
            params=params,
            context_name=context_name,
            branch=branch,
        )
        return self._migrate_response(us_resp)

    def _migrate_response(self, us_resp: dict) -> dict:
        initial_type = us_resp["type"]
        while True:
            schema_migration = self.get_schema_migration(
                entry_scope=us_resp["scope"],
                entry_type=us_resp["type"],
            )
            us_resp = schema_migration.migrate(us_resp)
            if us_resp["type"] == initial_type:
                break
        return us_resp

    @overload
    def get_by_key(self, entry_key: str, expected_type: None = None) -> USEntry:
        pass

    @overload
    def get_by_key(self, entry_key: str, expected_type: type[_ENTRY_TV] | None = None) -> _ENTRY_TV:
        pass

    def get_by_key(self, entry_key: str, expected_type: type[USEntry] | None = None) -> USEntry:
        raise NotImplementedError()

    def get_collection(
        self,
        entry_cls: type[_ENTRY_TV] | None,
        entry_type: str | None = None,
        entry_scope: str | None = None,
        meta: dict[str, str | int | None] | None = None,
        all_tenants: bool = False,
        include_data: bool = True,
        ids: Iterable[str] | None = None,
        creation_time: dict[str, str | int | None] | None = None,
        raise_on_broken_entry: bool = False,
    ) -> Generator[_ENTRY_TV, None, None]:
        entry_cls_scope = entry_cls.scope if entry_cls is not None else None
        entry_cls_type = entry_cls.type_ if entry_cls is not None else None
        if all_tenants and include_data:
            # Not supported; see _req_data_iter_entries
            raise ValueError("all_tenants and include_data cannot both be True")
        if entry_scope and entry_cls_scope:
            raise ValueError("US scope can not be provided in entry class and in parameters simultaneously")
        if not entry_scope and not entry_cls_scope:
            raise ValueError("US scope not provided neither in entry class nor in parameters")

        effective_scope = entry_scope or entry_cls_scope
        effective_type = entry_type or entry_cls_type
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

        for us_resp in us_entry_iterator:
            try:
                us_resp = self._migrate_response(us_resp)
                yield self._entry_dict_to_obj(us_resp, expected_type=entry_cls)
            except Exception:
                LOGGER.exception("Failed to load US object: %s", us_resp)
                if raise_on_broken_entry:
                    raise

    def get_raw_collection(
        self,
        entry_scope: str,
        entry_type: str | None = None,
        all_tenants: bool = False,
    ) -> Iterable[dict]:
        return self._us_client.entries_iterator(
            scope=entry_scope,
            entry_type=entry_type,
            all_tenants=all_tenants,
            include_data=False,
            limit=self.ITER_ENTRIES_PAGE_SIZE,
        )

    def move(self, entry: USEntry, destination: str) -> None:
        assert entry.uuid is not None
        self._us_client.move_entry(entry.uuid, destination)
        self.reload_data(entry)  # maybe we have to update only entry_key

    def reload_data(self, entry: USEntry) -> None:
        assert entry.uuid is not None
        us_resp = self.get_migrated_entry(entry.uuid)
        reloaded_entry = self._entry_dict_to_obj(us_resp, expected_type=type(entry))
        entry.data = reloaded_entry.data
        entry._us_resp = us_resp

    # Locks
    #
    def acquire_lock(self, entry: USEntry, duration: int | None = None, wait_timeout: int | None = None) -> str:
        assert entry.uuid is not None
        lock_token = self._us_client.acquire_lock(entry.uuid, duration, wait_timeout)
        entry.lock = lock_token
        return lock_token

    def release_lock(self, entry: USEntry) -> None:
        if entry.lock:
            assert entry.uuid is not None
            self._us_client.release_lock(entry.uuid, entry.lock)
            entry.lock = None

    @contextlib.contextmanager
    def locked_cm(
        self, entry: USEntry, duration: int | None = None, wait_timeout: int | None = None
    ) -> Generator[None, None, None]:
        self.acquire_lock(entry, duration=duration, wait_timeout=wait_timeout)
        try:
            yield
        finally:
            self.release_lock(entry)

    @contextlib.contextmanager
    def get_locked_entry_cm(
        self,
        expected_type: type[_ENTRY_TV],
        entry_id: str,
        duration: int | None = None,
        wait_timeout: int | None = None,
        context_name: str | None = None,
    ) -> Generator[_ENTRY_TV, None, None]:
        lock_token = self._us_client.acquire_lock(entry_id, duration, wait_timeout)
        entry = None
        try:
            entry = self.get_by_id(
                entry_id,
                expected_type=expected_type,
                context_name=context_name,
            )
            entry.lock = lock_token
            yield entry
        finally:
            self._us_client.release_lock(entry_id, lock_token)
            if entry is not None:
                entry.lock = None

    # Dependencies
    #
    def get_loaded_us_connection(self, identity: str | ConnectionRef) -> ConnectionBase:
        # Temporary workaround to mitigate forgotten .load_dataset_dependencies()
        if isinstance(identity, str):
            identity = DefaultConnectionRef(conn_id=identity)

        if identity not in self._loaded_entries:
            LOGGER.debug("Connection %s is not preloaded", identity)
            # TODO FIX: Uncomment after migration to dataset v7
            # _log.warning("SyncUSManager.get_loaded_us_connection() called without connection in cache")
            self._ensure_conn_in_cache(
                conn_ref=identity,
                referrer=None,
            )
        return super().get_loaded_us_connection(identity)

    def ensure_source_preloaded(
        self,
        conn_ref: ConnectionRef,
        referrer: USEntry | None,
        source_type: DataSourceType | str | None = None,
    ) -> ConnectionBase | None:
        if isinstance(source_type, str):
            if not DataSourceType.is_declared(source_type):
                LOGGER.warning("Source type %s is not defined", source_type)
                source_type = None
            else:
                source_type = DataSourceType(source_type)

        connection_type = get_connection_type_for_source_type(source_type)

        if source_type is not None and connection_type is None:
            LOGGER.warning("Failed get connection_type for source_type %s", source_type)

        return self.ensure_connection_preloaded(
            conn_ref=conn_ref,
            referrer=referrer,
            connection_type=connection_type,
        )

    def ensure_connection_preloaded(
        self,
        conn_ref: ConnectionRef,
        referrer: USEntry | None,
        connection_type: ConnectionType | None = None,
    ) -> ConnectionBase | None:
        return self._ensure_conn_in_cache(
            referrer=referrer,
            conn_ref=conn_ref,
            connection_type=connection_type,
        )

    def _ensure_conn_in_cache(
        self,
        conn_ref: ConnectionRef,
        referrer: USEntry | None,
        connection_type: ConnectionType | None = None,
    ) -> ConnectionBase | None:
        conn: USEntry | BrokenUSLink
        if conn_ref in self._loaded_entries:
            conn = self._loaded_entries[conn_ref]
        else:
            assert isinstance(conn_ref, DefaultConnectionRef)

            # Try to construct a virtual connection without going to US
            if connection_type is not None:
                connection_cls = get_connection_class(connection_type)

                if connection_cls.is_virtual:
                    conn = connection_cls.sync_create_virtual(
                        connection_id=conn_ref.conn_id,
                        us_manager=self,
                    )

                    self._loaded_entries[conn_ref] = conn
                    return conn

            try:
                conn = self.get_by_id(
                    conn_ref.conn_id,
                    ConnectionBase,
                    context_name="connection",
                )
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
        if isinstance(conn, ConnectionBase):
            return conn
        raise ValueError("Entry was in cache but it is not a connection: %s", type(conn))

    # TODO FIX: Think about cache control
    @generic_profiler("us-load-dependencies")
    def load_dataset_dependencies(
        self,
        dataset: Dataset,
        respect_sources: bool = False,
    ) -> None:
        """
        Load Dataset dependencies from US.

        When ``respect_sources`` is ``False``, this method loads dependencies using ``USEntry.links`` dict of ``key-id``
        pairs without guessing types.

        When ``respect_sources`` is ``True``, this method collects ids and types from dataset sources and then adds the
        rest from ``USEntry.links`` without types. This allows loading connections with type enforcing from sources.
        """

        # Collect refs from dataset sources
        if respect_sources:
            ds_accessor = DatasetComponentAccessor(dataset=dataset)
            sources_to_load = ds_accessor.collect_data_source_types()
        else:
            sources_to_load = {}

        # TODO FIX: Find a way to track direct source of link
        # Collect refs from USEntry links
        for entry_ref in self._get_entry_links(dataset):

            # Dataset's sources have priority over USEntry links
            if entry_ref not in sources_to_load:
                sources_to_load[entry_ref] = None

        processed_sources: dict[ConnectionRef, DataSourceType | None] = {}

        while sources_to_load:
            ref, source_type = sources_to_load.popitem()
            processed_sources[ref] = source_type

            try:
                resolved_ref = self.ensure_source_preloaded(
                    conn_ref=ref,
                    referrer=dataset,
                    source_type=source_type,
                )
            except Exception:
                LOGGER.exception("Can not load linked US entry %s for entry %s", ref, dataset.uuid)
                raise

            # Preload entries for loaded entry
            new_links = self._get_entry_links(resolved_ref)

            # Add new entries to queue
            for connection_ref in new_links:

                # Ignore already processed and already queued
                if (connection_ref not in processed_sources) and (connection_ref not in sources_to_load):
                    sources_to_load[connection_ref] = None

    def load_get_entries_at_path(self, us_path: str) -> list[dict[str, Any]]:
        return self._us_client.get_entries_info_in_path(us_path)
