from __future__ import annotations

from collections import ChainMap
from contextlib import contextmanager
import copy
import logging
from typing import (
    ClassVar,
    Optional,
    TypeVar,
    Union,
)
from typing import (
    TYPE_CHECKING,
    Any,
)
from typing import ChainMap as ChainMapGeneric

import attr
import marshmallow

from dl_api_commons.base_models import (
    RequestContextInfo,
    TenantDef,
)
from dl_configs.crypto_keys import (
    CryptoKeysConfig,
    get_crypto_keys_config_from_env,
)
from dl_constants.enums import (
    ConnectionType,
    MigrationStatus,
)
from dl_core import exc
from dl_core.base_models import (
    ConnectionRef,
    DefaultConnectionRef,
    EntryLocation,
    PathEntryLocation,
    WorkbookEntryLocation,
)
from dl_core.lifecycle.factory import DefaultEntryLifecycleManagerFactory
from dl_core.united_storage_client import (
    USAuthContextBase,
    UStorageClientBase,
)
from dl_core.us_connection import get_connection_class
from dl_core.us_connection_base import ConnectionBase
from dl_core.us_dataset import Dataset
from dl_core.us_entry import (
    BaseAttrsDataModel,
    USEntry,
    USMigrationEntry,
)
from dl_core.us_manager.crypto.main import CryptoController
from dl_core.us_manager.local_cache import USEntryBuffer
from dl_core.us_manager.schema_migration.base import BaseEntrySchemaMigration
from dl_core.us_manager.schema_migration.factory import DefaultEntrySchemaMigrationFactory
from dl_core.us_manager.schema_migration.factory_base import EntrySchemaMigrationFactoryBase
from dl_core.us_manager.storage_schemas.connection_schema_registry import MAP_TYPE_TO_SCHEMA_MAP_TYPE_TO_SCHEMA
from dl_core.us_manager.storage_schemas.dataset import DatasetStorageSchema
from dl_core.us_manager.us_entry_serializer import (
    USDataPack,
    USEntrySerializer,
    USEntrySerializerMarshmallow,
)
from dl_core.us_manager.utils.fake_us_client import FakeUSClient
from dl_utils.utils import AddressableData


if TYPE_CHECKING:
    from dl_core.lifecycle.base import EntryLifecycleManager
    from dl_core.lifecycle.factory_base import EntryLifecycleManagerFactoryBase
    from dl_core.services_registry import ServicesRegistry


LOGGER = logging.getLogger(__name__)

_ENTRY_TV = TypeVar("_ENTRY_TV", bound=USEntry)


@attr.s(frozen=True)
class CryptoKeyInfo:
    key_id: Optional[str] = attr.ib()  # None value is None
    key_kind: Optional[str] = attr.ib()


class USManagerBase:
    ITER_ENTRIES_PAGE_SIZE: ClassVar[int] = 1500

    _MAP_TYPE_TO_SCHEMA: ClassVar[ChainMapGeneric[type[BaseAttrsDataModel], type[marshmallow.Schema]]] = ChainMap(
        MAP_TYPE_TO_SCHEMA_MAP_TYPE_TO_SCHEMA,  # type: ignore  # TODO: fix
        {
            Dataset.DataModel: DatasetStorageSchema,
        },
    )

    _bi_context: RequestContextInfo

    _loaded_entries: USEntryBuffer

    _us_auth_context: USAuthContextBase
    _us_client: UStorageClientBase
    _fake_us_client: FakeUSClient
    _lifecycle_manager_factory: EntryLifecycleManagerFactoryBase
    _schema_migration_factory: EntrySchemaMigrationFactoryBase

    def __init__(
        self,
        bi_context: RequestContextInfo,
        crypto_keys_config: Optional[CryptoKeysConfig],
        us_base_url: str,
        us_api_prefix: Optional[str],
        us_auth_context: USAuthContextBase,
        services_registry: ServicesRegistry,
        lifecycle_manager_factory: Optional[EntryLifecycleManagerFactoryBase] = None,
        schema_migration_factory: Optional[EntrySchemaMigrationFactoryBase] = None,
    ):
        # TODO FIX: Try to connect it together to eliminate possible divergence
        if services_registry is not None:
            try:
                conn_factory = services_registry.get_conn_executor_factory()
            except NotImplementedError:
                # Got some kind of dummy SR.
                # Skip this check because we'll get NIError anyway if we try ue it
                pass
            else:
                assert bi_context is conn_factory.req_ctx_info

        if crypto_keys_config is None:
            actual_crypto_keys_config = get_crypto_keys_config_from_env()
        else:
            actual_crypto_keys_config = crypto_keys_config

        self._bi_context = bi_context
        self._us_auth_context = us_auth_context

        self._crypto_keys_config = actual_crypto_keys_config
        self._us_base_url = us_base_url
        self._us_api_prefix = us_api_prefix

        self._crypto_controller = CryptoController(self._crypto_keys_config)
        self._loaded_entries = USEntryBuffer()
        # To prevent usage of flask context
        self._fake_us_client = FakeUSClient()
        self._services_registry: ServicesRegistry = services_registry

        lifecycle_manager_factory = lifecycle_manager_factory or DefaultEntryLifecycleManagerFactory()
        assert lifecycle_manager_factory is not None
        self._lifecycle_manager_factory = lifecycle_manager_factory

        schema_migration_factory = schema_migration_factory or DefaultEntrySchemaMigrationFactory()
        assert schema_migration_factory is not None
        self._schema_migration_factory = schema_migration_factory

    def get_entry_buffer(self) -> USEntryBuffer:
        return self._loaded_entries

    @property
    def bi_context(self) -> RequestContextInfo:
        return self._bi_context

    @property
    def actual_crypto_key_id(self) -> str:
        return self._crypto_keys_config.actual_key_id

    def get_lifecycle_manager(
        self, entry: USEntry, service_registry: Optional[ServicesRegistry] = None
    ) -> EntryLifecycleManager:
        if service_registry is None:
            service_registry = self.get_services_registry()
        return self._lifecycle_manager_factory.get_lifecycle_manager(
            entry=entry, us_manager=self, service_registry=service_registry
        )

    def get_schema_migration(
        self, entry_scope: str, entry_type: str, service_registry: Optional[ServicesRegistry] = None
    ) -> BaseEntrySchemaMigration:
        if service_registry is None:
            service_registry = self.get_services_registry()
        return self._schema_migration_factory.get_schema_migration(entry_scope, entry_type, service_registry)

    # TODO FIX: Prevent saving entries with tenant ID that doesn't match current tenant ID
    def set_tenant_override(self, tenant: TenantDef) -> None:
        if not self._us_auth_context.is_tenant_id_mutable():
            raise AssertionError("Tenant ID might be set only for US manager with master US auth context")
        self._us_client.set_tenant_override(tenant)

    @classmethod
    def get_load_storage_schema(cls, data_cls: type[BaseAttrsDataModel]) -> marshmallow.Schema:
        schema_cls = cls._MAP_TYPE_TO_SCHEMA.get(data_cls)
        return schema_cls()  # type: ignore  # TODO: fix

    @classmethod
    def get_dump_storage_schema(cls, data_cls: type[BaseAttrsDataModel]) -> marshmallow.Schema:
        schema_cls = cls._MAP_TYPE_TO_SCHEMA.get(data_cls)
        return schema_cls()  # type: ignore  # TODO: fix

    @classmethod
    def get_us_entry_serializer(cls, entry_cls: type[USEntry]) -> USEntrySerializer:
        data_cls = entry_cls.DataModel

        if data_cls is not None:
            if issubclass(data_cls, BaseAttrsDataModel):
                return USEntrySerializerMarshmallow()

        raise ValueError(f"Unknown type for US entry data: {entry_cls.DataModel}")

    # TODO FIX: Replace with on-import collecting of USEntry inheritors type/scope
    @classmethod
    def _get_entry_class(
        cls, *, us_scope: str, us_type: str, entry_key: Optional[EntryLocation] = None
    ) -> type[USEntry]:
        err_msg = (
            f"Unknown combination of scope/type: {us_scope}/{us_type}"
            f" key={'not provided' if entry_key is None else entry_key.to_short_string()}"
        )

        try:
            if us_scope == "dataset":
                return Dataset

            elif us_scope == "connection":
                conn_type: ConnectionType
                try:
                    conn_type = ConnectionType(us_type)
                except ValueError:
                    conn_type = ConnectionType.unknown
                return get_connection_class(conn_type)

        except Exception as e:
            raise TypeError(err_msg) from e

        else:
            raise exc.UnexpectedUSEntryType()

    def get_sensitive_fields_key_info(self, entry: USEntry) -> dict[str, CryptoKeyInfo]:
        if isinstance(entry, USMigrationEntry):
            us_resp = entry._us_resp
            assert us_resp is not None
            entry_cls = self._get_entry_class(
                us_type=us_resp["type"],
                us_scope=us_resp["scope"],
                entry_key=entry.entry_key,
            )
            data_model_cls = entry_cls.DataModel
        else:
            entry_cls = type(entry)
            data_model_cls = entry.DataModel

        if data_model_cls is not None and issubclass(data_model_cls, BaseAttrsDataModel):
            serializer = self.get_us_entry_serializer(entry_cls)
            sensitive_fields = serializer.get_secret_keys(entry_cls)
            us_resp = entry._us_resp
            assert us_resp is not None
            fields_info_addressable = AddressableData({})
            unversioned_addressable = AddressableData(us_resp["unversionedData"])

            for field_key in sensitive_fields:
                if unversioned_addressable.contains(field_key):
                    field_value = unversioned_addressable.get(field_key)
                    if field_value is None:
                        key_id = None
                        key_kind = None
                    else:
                        key_id = field_value["key_id"]
                        key_kind = field_value["key_kind"]
                    fields_info_addressable.set(field_key, CryptoKeyInfo(key_id=key_id, key_kind=key_kind))

                else:
                    fields_info_addressable.set(field_key, None)
            return fields_info_addressable.data
        return {}

    def actualize_crypto_keys(self, entry: USMigrationEntry) -> None:
        us_resp = entry._us_resp
        assert us_resp is not None

        entry_cls = self._get_entry_class(
            us_type=us_resp["type"],
            us_scope=us_resp["scope"],
            entry_key=entry.entry_key,
        )

        serializer = self.get_us_entry_serializer(entry_cls)
        secret_keys = serializer.get_secret_keys(entry_cls)
        for key in secret_keys:
            secret_addressable = AddressableData(entry.unversioned_data)
            decrypted_value = self._crypto_controller.decrypt(secret_addressable.get(key))
            encrypted_value = self._crypto_controller.encrypt_with_actual_key(decrypted_value)
            secret_addressable.set(key, encrypted_value)

    def _entry_dict_to_obj(self, us_resp: dict, expected_type: Optional[type[USEntry]] = None) -> USEntry:
        """
        Deserialize US entry dict.
        :param us_resp: US response as-is
        :param expected_type: If not none - type/scope will be checked to that actual class is subclass of expected one.
         If check fails or if there is no mapping for type/scope `TypeError` will be thrown.
         `USEntry` is special value. Actual class will not be determined and base `USEntry` will be constructed
        :return: Deserialized US entry
        """

        entry_loc: EntryLocation
        raw_entry_key = us_resp["key"]
        assert isinstance(raw_entry_key, str), f"Got non-string US entry key: {raw_entry_key!r}"

        if us_resp.get("workbookId") is not None:
            entry_name = raw_entry_key.split("/")[-1]
            entry_loc = WorkbookEntryLocation(
                workbook_id=us_resp["workbookId"],
                entry_name=entry_name,
            )
        else:
            entry_loc = PathEntryLocation(path=raw_entry_key)

        entry_cls: type[USEntry]
        if expected_type == USMigrationEntry:
            entry_cls = USMigrationEntry
        else:
            entry_cls = self._get_entry_class(
                us_type=us_resp["type"],
                us_scope=us_resp["scope"],
                entry_key=entry_loc,
            )
            if expected_type is not None and not issubclass(entry_cls, expected_type):
                raise exc.UnexpectedUSEntryType(
                    f"Unexpected type of US entry fetched:"
                    f" {entry_cls.__qualname__} instead of {expected_type.__qualname__}"
                )

        common_properties: dict[str, Any] = dict(
            entry_key=entry_loc,
            type_=us_resp["type"],
            meta=us_resp["meta"],
            is_locked=us_resp.get("isLocked"),
            is_favorite=us_resp.get("isFavorite"),
            permissions=us_resp.get("permissions") or {},
            links=us_resp.get("links") or {},
            hidden=us_resp["hidden"],
            migration_status=MigrationStatus(us_resp.get("migration_status", MigrationStatus.non_migrated.value)),
        )

        entry: USEntry
        if entry_cls == USMigrationEntry:
            entry = USMigrationEntry(
                uuid=us_resp["entryId"],
                us_manager=self,
                data=us_resp.get("data"),
                unversioned_data=us_resp.get("unversionedData"),
                data_strict=False,
                **common_properties,
            )
        else:
            data = us_resp.get("data", dict())
            secrets = us_resp.get("unversionedData", dict())

            assert isinstance(data, dict)
            assert isinstance(secrets, dict)
            data_pack = USDataPack(
                data=data,
                secrets=secrets,
            )

            data_pack = copy.deepcopy(data_pack)
            if data_pack.secrets:
                for key, secret in data_pack.secrets.items():
                    assert not isinstance(secret, str)
                    data_pack.secrets[key] = self._crypto_controller.decrypt(secret)

            serializer = self.get_us_entry_serializer(entry_cls)
            entry = serializer.deserialize(
                entry_cls,
                data_pack,
                us_resp["entryId"],
                us_manager=self,
                common_properties=common_properties,
                data_strict=False,
            )

        entry.stored_in_db = True
        entry._us_resp = us_resp

        return entry

    def clone_entry_instance(self, entry: USEntry) -> USEntry:
        if not entry.uuid:
            raise ValueError("Only saved entries are supported here (must have an .uuid)")
        entry_data = self._get_entry_save_params(entry)
        entry_loc = entry.entry_key

        assert entry_loc is not None, "Could not clone US entry without location"

        entry_data.update(
            entryId=entry.uuid,
            unversionedData=entry_data.pop("unversioned_data"),
            migration_status=entry.migration_status,
            **entry_loc.to_us_resp_api_params(entry.raw_us_key),
        )
        return self._entry_dict_to_obj(entry_data)

    @staticmethod
    def _get_us_type_for_entry(entry: USEntry) -> Optional[str]:
        if isinstance(entry, ConnectionBase):
            us_type = entry.conn_type.name
        else:
            us_type = entry.type_  # type: ignore  # TODO: fix

        if us_type is not None and not isinstance(us_type, str):
            raise ValueError(f"Unexpected US type ({us_type}) for entry {entry}")

        return us_type

    def dump_data(self, entry: USEntry) -> dict[str, Any]:
        entry_cls = type(entry)
        if entry_cls.DataModel is not None and issubclass(entry_cls.DataModel, BaseAttrsDataModel):
            serializer = self.get_us_entry_serializer(entry_cls)
            data_dict = serializer.serialize_raw(entry)
        else:
            data_dict = entry.data
        return data_dict

    # TODO CONSIDER: Use DTO instead of dict
    def _get_entry_save_params(self, entry: USEntry) -> dict:
        us_type = self._get_us_type_for_entry(entry)

        # Validating type on update
        if entry._us_resp:
            prev_type = entry._us_resp.get("type")
            if prev_type == "" and us_type is None:
                pass
            elif prev_type != us_type:
                raise ValueError(f"Type mismatch on US entry update: was '{prev_type}'; become '{us_type}'")

        data_dict = self.dump_data(entry)

        save_params: dict[str, Any] = {}

        if not entry.stored_in_db:
            if entry.permissions_mode is not None:
                save_params.update(permissionsMode=entry.permissions_mode)
            if entry.initial_permissions is not None:
                save_params.update(initialPermissions=entry.initial_permissions)

        if isinstance(entry, USMigrationEntry):
            save_params.update(
                data=data_dict,
                unversioned_data=entry.unversioned_data,
            )
        else:
            entry_cls = type(entry)
            if entry_cls.DataModel is not None:
                serializer = self.get_us_entry_serializer(type(entry))
                data_pack = serializer.serialize(entry)
            else:
                data_pack = USDataPack(data=data_dict)

            for key, secret in data_pack.secrets.items():
                assert secret is None or isinstance(secret, str)
                data_pack.secrets[key] = self._crypto_controller.encrypt_with_actual_key(secret)

            save_params.update(
                data=data_pack.data,
                unversioned_data=data_pack.secrets,
            )

        save_params.update(
            meta=entry.meta,
            hidden=entry.hidden,
            links=entry.links,
            scope=entry.scope,
            type=us_type,
        )

        return save_params

    def _prepare_update_entry_params(self, entry: USEntry, update_revision: Optional[bool] = None) -> dict:
        assert entry.uuid is not None
        save_params = self._get_entry_save_params(entry)
        assert "data" in save_params and "unversioned_data" in save_params

        save_params.pop("scope")
        save_params.pop("type")
        save_params["update_revision"] = update_revision

        return save_params

    def copy_entry(self, source: _ENTRY_TV, key: Optional[EntryLocation] = None) -> _ENTRY_TV:
        if not isinstance(source, Dataset):
            raise ValueError("Only dataset can be copied at this time")

        # TODO FIX: Find another way to copy data to avoid issues with unversioned data
        source_save_params = self._get_entry_save_params(source)

        copied_entry = type(source)(
            uuid=None,
            data=source_save_params["data"],
            entry_key=key,
            type_=source.type_,
            is_locked=source.is_locked,
            is_favorite=source.is_favorite,
            # initial_permissions=???,
            # Links will be collected on pre-save
            # links={},
            hidden=source.hidden,
            us_manager=self,
            data_strict=False,
        )

        self.get_lifecycle_manager(entry=copied_entry).post_copy_hook()

        return copied_entry  # type: ignore  # TODO: fix

    @staticmethod
    @contextmanager
    def _enrich_us_exception(  # type: ignore  # TODO: fix
        entry_id: Optional[str] = None,
        entry_scope: Optional[str] = None,
    ):
        try:
            yield
        except exc.USReqException as err:
            err.details.update(
                entry_id=entry_id,
                scope=entry_scope,
            )
            raise

    def get_loaded_us_connection(self, identity: Union[str, ConnectionRef]) -> ConnectionBase:
        if isinstance(identity, str):
            identity = DefaultConnectionRef(conn_id=identity)

        assert isinstance(identity, ConnectionRef)
        entry = self._loaded_entries.get_entry(identity)

        if not isinstance(entry, ConnectionBase):
            raise ValueError(f"Entry {identity} is not a connection: {type(identity).__qualname__}")

        return entry

    def _get_entry_links(self, entry: Optional[USEntry]) -> set[ConnectionRef]:
        if isinstance(entry, Dataset):
            lifecycle_manager = self.get_lifecycle_manager(entry=entry)
            linked_entries_refs: set[ConnectionRef] = {
                DefaultConnectionRef(conn_id=conn_id) for conn_id in lifecycle_manager.collect_links().values()
            }

            return linked_entries_refs
        elif isinstance(entry, ConnectionBase):
            pass
        return set()

    def get_services_registry(self) -> ServicesRegistry:
        if self._services_registry is not None:
            return self._services_registry
        raise ValueError("Services registry was not passed to US manager")
