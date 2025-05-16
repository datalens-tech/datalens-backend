from __future__ import annotations

import abc
from functools import reduce
import json
import logging
from typing import (
    Any,
    Callable,
    ClassVar,
    Optional,
    Type,
    TypeVar,
)
import uuid

import attr
import marshmallow as ma

from dl_api_commons.base_models import RequestContextInfo
from dl_configs.crypto_keys import CryptoKeysConfig
from dl_core.us_manager.crypto.main import CryptoController
import dl_file_uploader_lib.utils.s3_utils as s3_utils
from dl_file_uploader_lib.utils.s3_utils import (
    S3NoSuchKey,
    S3Object,
)
from dl_s3.s3_service import S3Service
from dl_utils.utils import (
    AddressableData,
    DataKey,
)


LOGGER = logging.getLogger(__name__)


class S3ModelException(Exception):
    pass


class S3ModelAccessDenied(S3ModelException):
    pass


class S3ModelNotFound(S3ModelException):
    pass


class SecretContainingMixin:
    def get_secret_keys(self) -> set[DataKey]:
        return set()


TS3Model = TypeVar("TS3Model", bound="S3Model")


@attr.s(init=True, kw_only=True)
class S3Model(SecretContainingMixin, metaclass=abc.ABCMeta):
    ID_PREFIX: ClassVar[str]
    _manager: Optional[S3ModelManager] = attr.ib(default=None)

    id: str = attr.ib(factory=lambda: str(uuid.uuid4()))

    @classmethod
    def generate_key_prefix(cls, manager: S3ModelManager) -> str:
        assert cls.ID_PREFIX

        return "_".join((manager._tenant_id, cls.ID_PREFIX))

    @classmethod
    def _generate_key_by_id(cls, manager: S3ModelManager, obj_id: str) -> str:
        assert cls.ID_PREFIX

        return "_".join((manager._tenant_id, cls.ID_PREFIX, obj_id))

    def generate_key(self) -> str:
        assert self._manager is not None

        return self._generate_key_by_id(self._manager, self.id)

    @classmethod
    async def get(cls: Type[TS3Model], manager: S3ModelManager, obj_id: str) -> TS3Model:
        key = cls._generate_key_by_id(manager=manager, obj_id=obj_id)
        return await manager.get(key=key, target_cls=cls)  # type: ignore  # 2025-04-25 # TODO: Incompatible return value type (got "S3Model", expected "TS3Model")  [return-value]

    async def save(self, persistent: Optional[bool] = False) -> None:
        assert self._manager
        await self._manager.save(self, persistent)

    async def delete(self) -> None:
        assert self._manager
        await self._manager.delete(self)


@attr.s
class S3ModelManager:
    _s3_service: S3Service = attr.ib()
    _tenant_id: str = attr.ib()
    _crypto_keys_config: CryptoKeysConfig = attr.ib()
    _crypto_controller: CryptoController = attr.ib(
        init=False,
        default=attr.Factory(
            lambda self: CryptoController(self._crypto_keys_config),
            takes_self=True,
        ),
    )
    rci: Optional[RequestContextInfo] = attr.ib(default=None)

    def _set_obj_attr(self, obj: S3Model, key: DataKey, value: Any) -> None:
        key_head = DataKey(parts=key.parts[:-1])
        obj = self._get_obj_attr(obj, key_head)
        obj.__setattr__(key.parts[-1], value)

    def _get_obj_attr(self, obj: S3Model, key: DataKey) -> Any:
        return reduce(lambda _obj, attribute: _obj.__getattribute__(attribute), key.parts, obj)

    def _serialize_object(self, obj: S3Model) -> str:
        schema_cls = _get_model_schema_class(type(obj))
        schema = schema_cls()
        json_data_dict = schema.dump(obj)

        addressable_data = AddressableData(json_data_dict)
        secret_keys = obj.get_secret_keys()
        for key in secret_keys:
            plain_text = addressable_data.get(key)
            encrypted = self._crypto_controller.encrypt_with_actual_key(plain_text)
            addressable_data.set(key, json.dumps(encrypted))

        json_data = json.dumps(json_data_dict)
        return json_data

    def _deserialize_object(self, json_data: str, target_cls: Type[S3Model]) -> S3Model:
        schema_cls = _get_model_schema_class(target_cls)
        schema = schema_cls()
        obj = schema.loads(json_data)

        assert isinstance(obj, S3Model)
        secret_keys = obj.get_secret_keys()
        for key in secret_keys:
            encrypted = json.loads(self._get_obj_attr(obj, key))
            decrypted = self._crypto_controller.decrypt(encrypted)
            self._set_obj_attr(obj, key, decrypted)

        return obj

    async def get(self, key: str, target_cls: Type[S3Model], post_load: Optional[Callable] = None) -> S3Model:
        sync_s3_client = self._s3_service.get_sync_client()

        # Try from persistent
        try:
            json_data = s3_utils.read_json_from_s3(
                s3_sync_cli=sync_s3_client,
                file=S3Object(self._s3_service.persistent_bucket_name, key),
            )
        except S3NoSuchKey:
            LOGGER.debug(f"S3Model object not found in persistent bucket: {key}")

            # Try from tmp
            try:
                json_data = s3_utils.read_json_from_s3(
                    s3_sync_cli=sync_s3_client,
                    file=S3Object(self._s3_service.tmp_bucket_name, key),
                )
            except S3NoSuchKey:
                LOGGER.debug(f"S3Model object not found in tmp bucket: {key}")
                LOGGER.info(f"S3Model object not found: {key}")
                raise S3ModelNotFound()

        obj = self._deserialize_object(json_data, target_cls)
        obj._manager = self

        if post_load is not None:
            post_load(obj)

        return obj

    async def save(self, obj: S3Model, persistent: Optional[bool] = False) -> None:
        obj_key = obj.generate_key()
        data_json = self._serialize_object(obj)
        sync_s3_client = self._s3_service.get_sync_client()

        if persistent:
            # Delete from tmp, save in persistent
            try:
                s3_utils.delete_json_from_s3(
                    s3_sync_cli=sync_s3_client,
                    file=S3Object(self._s3_service.tmp_bucket_name, obj_key),
                )
            except S3NoSuchKey:
                pass

            s3_utils.write_json_to_s3(
                s3_sync_cli=sync_s3_client,
                file=S3Object(self._s3_service.persistent_bucket_name, obj_key),
                json_data=data_json,
            )
        else:
            # Delete from persistent, save in tmp
            try:
                s3_utils.delete_json_from_s3(
                    s3_sync_cli=sync_s3_client,
                    file=S3Object(self._s3_service.persistent_bucket_name, obj_key),
                )
            except S3NoSuchKey:
                pass

            s3_utils.write_json_to_s3(
                s3_sync_cli=sync_s3_client,
                file=S3Object(self._s3_service.tmp_bucket_name, obj_key),
                json_data=data_json,
            )

    async def delete(self, obj: S3Model) -> None:
        obj_key = obj.generate_key()
        sync_s3_client = self._s3_service.get_sync_client()

        # Remove from tmp
        try:
            s3_utils.delete_json_from_s3(
                s3_sync_cli=sync_s3_client,
                file=S3Object(self._s3_service.tmp_bucket_name, obj_key),
            )
        except S3NoSuchKey:
            pass

        # Remove from persistent
        try:
            s3_utils.delete_json_from_s3(
                s3_sync_cli=sync_s3_client,
                file=S3Object(self._s3_service.persistent_bucket_name, obj_key),
            )
        except S3NoSuchKey:
            pass


class S3BaseSchema(ma.Schema):
    class Meta:
        unknown = ma.EXCLUDE

        target: Type

    @ma.post_load(pass_many=False)
    def to_object(self, data: dict[str, Any], **kwargs: Any) -> Any:
        return self.Meta.target(**data)


class S3BaseModelSchema(S3BaseSchema):
    id = ma.fields.String()
    user_id = ma.fields.String()


_MODEL_CLASS_TO_STORAGE_SCHEMA_MAP: dict[Type[S3Model], Type[S3BaseModelSchema]] = dict()


def _get_model_schema_class(model_cls: Type[S3Model]) -> Type[S3BaseModelSchema]:
    return _MODEL_CLASS_TO_STORAGE_SCHEMA_MAP[model_cls]


def register_s3_model_storage_schema(model_cls: Type[S3Model], schema_cls: Type[S3BaseModelSchema]) -> None:
    _MODEL_CLASS_TO_STORAGE_SCHEMA_MAP[model_cls] = schema_cls
