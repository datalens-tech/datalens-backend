from __future__ import annotations

import abc
from collections import ChainMap
import copy
from functools import reduce
import json
import logging
from typing import (
    ClassVar,
    Optional,
    Type,
)
from typing import (
    TYPE_CHECKING,
    Any,
)
from typing import ChainMap as ChainMapGeneric

import attr
import marshmallow

from dl_core import exc
from dl_core.base_models import BaseAttrsDataModel
from dl_core.us_dataset import Dataset
from dl_core.us_entry import USEntry
from dl_core.us_manager.crypto.main import EncryptedData
from dl_core.us_manager.storage_schemas.connection_schema_registry import MAP_TYPE_TO_SCHEMA_MAP_TYPE_TO_SCHEMA
from dl_core.us_manager.storage_schemas.dataset import DatasetStorageSchema
from dl_utils.utils import (
    AddressableData,
    DataKey,
)


if TYPE_CHECKING:
    from dl_core.us_manager.us_manager import USManagerBase


LOGGER = logging.getLogger(__name__)


@attr.s()
class USDataPack:
    data: dict[str, Any] = attr.ib()
    secrets: dict[str, Optional[str | EncryptedData]] = attr.ib(repr=False, factory=dict)


class USEntrySerializer(abc.ABC):
    _MAP_TYPE_TO_SCHEMA: ClassVar[ChainMapGeneric[Type[BaseAttrsDataModel], Type[marshmallow.Schema]]] = ChainMap(
        MAP_TYPE_TO_SCHEMA_MAP_TYPE_TO_SCHEMA,  # type: ignore  # 2024-01-30 # TODO: Argument 1 to "ChainMap" has incompatible type "dict[type[DataModel], type[Schema]]"; expected "MutableMapping[type[BaseAttrsDataModel], type[Schema]]"  [arg-type]
        {
            Dataset.DataModel: DatasetStorageSchema,
        },
    )

    @classmethod
    def get_load_storage_schema(cls, data_cls: Type[BaseAttrsDataModel]) -> marshmallow.Schema:
        schema_cls = cls._MAP_TYPE_TO_SCHEMA[data_cls]
        return schema_cls()

    @classmethod
    def get_dump_storage_schema(cls, data_cls: Type[BaseAttrsDataModel]) -> marshmallow.Schema:
        schema_cls = cls._MAP_TYPE_TO_SCHEMA[data_cls]
        return schema_cls()

    @abc.abstractmethod
    def serialize_raw(self, entry: USEntry) -> dict[str, Any]:
        """Returns raw entry data dict"""

        raise NotImplementedError()

    @abc.abstractmethod
    def deserialize_raw(
        self,
        cls: Type[USEntry],
        raw_data: dict[str, Any],
        entry_id: str,
        us_manager: USManagerBase,
        common_properties: dict[str, Any],
        data_strict: bool = True,
    ) -> USEntry:
        """Initializes USEntry with data provided in raw_data"""

        raise NotImplementedError()

    @abc.abstractmethod
    def get_secret_keys(self, cls: Type[USEntry]) -> set[DataKey]:
        raise NotImplementedError()

    @abc.abstractmethod
    def set_data_attr(self, entry: USEntry, key: DataKey, value: Any) -> None:
        raise NotImplementedError()

    @abc.abstractmethod
    def get_data_attr(self, entry: USEntry, key: DataKey) -> Any:
        raise NotImplementedError()

    def serialize(self, obj: USEntry) -> USDataPack:
        raw_dict = self.serialize_raw(obj)

        secret_keys = self.get_secret_keys(type(obj))

        raw_addressable = AddressableData(raw_dict)
        secrets_addressable = AddressableData({})

        for sec_key in secret_keys:
            sec_val = raw_addressable.pop(sec_key)
            secrets_addressable.set(sec_key, sec_val)

        return USDataPack(
            data=raw_addressable.data,
            secrets=secrets_addressable.data,
        )

    def deserialize(
        self,
        cls: Type[USEntry],
        data_pack: USDataPack,
        entry_id: str,
        us_manager: USManagerBase,
        common_properties: dict[str, Any],
        data_strict: bool = True,
    ) -> USEntry:
        # Assumed that data_pack's dicts was decoupled with initial US response
        data_pack = copy.deepcopy(data_pack)
        secret_source_addressable = AddressableData(data_pack.secrets)
        raw_addressable = AddressableData(data_pack.data)

        declared_secret_keys = self.get_secret_keys(cls)

        for secret_key in declared_secret_keys:
            if secret_source_addressable.contains(secret_key):
                sec_val = secret_source_addressable.pop(secret_key)
                sec_val_str = json.dumps(sec_val)
                raw_addressable.set(secret_key, sec_val_str)

        if secret_source_addressable.data:
            LOGGER.warning("Undeclared secrets found")

        return self.deserialize_raw(cls, raw_addressable.data, entry_id, us_manager, common_properties, data_strict)


class USEntrySerializerMarshmallow(USEntrySerializer):
    def serialize_raw(self, entry: USEntry) -> dict[str, Any]:
        dump_schema = self.get_dump_storage_schema(type(entry.data))
        data_dict = dump_schema.dump(entry.data)

        # Preventing saving what we can not load
        load_schema = self.get_load_storage_schema(type(entry.data))
        try:
            load_schema.load(data_dict)
            # TODO FIX: Validate that data is equal!!!
            # reloaded_data = load_schema.load(data_dict)
            # assert reloaded_data == entry.data
        except Exception as err:
            LOGGER.exception("Deserialization of US serialized data failed. Preventing save...")
            # TODO FIX: Custom exception
            raise exc.DLBaseException() from err

        return data_dict

    def deserialize_raw(
        self,
        cls: Type[USEntry],
        raw_data: dict[str, Any],
        entry_id: str,
        us_manager: USManagerBase,
        common_properties: dict[str, Any],
        data_strict: bool = True,
    ) -> USEntry:
        data_cls = cls.DataModel
        assert data_cls is not None and issubclass(data_cls, BaseAttrsDataModel)
        schema = self.get_load_storage_schema(data_cls)

        data = schema.load(raw_data)
        entry = cls(
            uuid=entry_id,
            us_manager=us_manager,
            data=data,
            data_strict=data_strict,
            **common_properties,
        )

        return entry

    def get_secret_keys(self, cls: Type[USEntry]) -> set[DataKey]:
        data_cls = cls.DataModel
        assert data_cls is not None and issubclass(data_cls, BaseAttrsDataModel)
        return data_cls.get_secret_keys()

    def set_data_attr(self, entry: USEntry, key: DataKey, value: Any) -> None:
        key_head = DataKey(parts=key.parts[:-1])
        obj = self.get_data_attr(entry, key_head)
        obj.__setattr__(key.parts[-1], value)

    def get_data_attr(self, entry: USEntry, key: DataKey) -> Any:
        return reduce(lambda obj, attribute: obj.__getattribute__(attribute), key.parts, entry.data)
