from __future__ import annotations

import abc
from datetime import timedelta
from functools import reduce
import json
import logging
from typing import (
    Any,
    AsyncIterator,
    Awaitable,
    Callable,
    ClassVar,
    Optional,
    Type,
    TypeVar,
    Union,
)
import uuid

import attr
import marshmallow as ma
import redis.asyncio

from dl_api_commons.base_models import RequestContextInfo
from dl_configs.crypto_keys import CryptoKeysConfig
from dl_core.us_manager.crypto.main import CryptoController
from dl_utils.utils import (
    AddressableData,
    DataKey,
)


LOGGER = logging.getLogger(__name__)


class RedisModelException(Exception):
    pass


class RedisModelAccessDenied(RedisModelException):
    pass


class RedisModelNotFound(RedisModelException):
    pass


class SecretContainingMixin:
    def get_secret_keys(self) -> set[DataKey]:
        return set()


TRedisModel = TypeVar("TRedisModel", bound="RedisModel")


@attr.s(init=True, kw_only=True)
class RedisModel(SecretContainingMixin, metaclass=abc.ABCMeta):
    KEY_PREFIX: ClassVar[str]
    DEFAULT_TTL: ClassVar[Optional[int]] = None  # in seconds
    _manager: Optional[RedisModelManager] = attr.ib(default=None)

    id: str = attr.ib(factory=lambda: str(uuid.uuid4()))

    @classmethod
    def _generate_key_by_id(cls, obj_id: str) -> str:
        assert cls.KEY_PREFIX
        return "/".join((cls.KEY_PREFIX, obj_id))

    def generate_key(self) -> str:
        return self._generate_key_by_id(self.id)

    @classmethod
    async def get(cls: Type[TRedisModel], manager: RedisModelManager, obj_id: str) -> TRedisModel:
        key = cls._generate_key_by_id(obj_id)
        return await manager.get(key=key, target_cls=cls)  # type: ignore  # 2024-01-30 # TODO: Incompatible return value type (got "RedisModel", expected "TRedisModel")  [return-value]

    async def save(self, ttl: Union[int, timedelta, None] = None) -> None:
        assert self._manager
        await self._manager.save(self, ttl=ttl)

    async def delete(self) -> None:
        assert self._manager
        await self._manager.delete(self)

    async def is_persistent(self) -> bool:
        assert self._manager
        return await self._manager.is_persistent(self)


@attr.s(init=True, kw_only=True)
class RedisModelUserIdAuth(RedisModel, metaclass=abc.ABCMeta):
    system_user_id: ClassVar[str] = "system"

    user_id: Optional[str] = attr.ib(default=None)

    def __attrs_post_init__(self) -> None:
        if self.user_id is None:
            assert self._manager is not None and self._manager.rci is not None
            user_id = self._manager.rci.user_id
            assert user_id is not None
            self.user_id = user_id

    @classmethod
    async def get_authorized(cls: Type[TRedisModel], manager: RedisModelManager, obj_id: str) -> TRedisModel:
        """Load entry with authorization check by user id"""
        key = cls._generate_key_by_id(obj_id)
        obj = await manager.get(key=key, target_cls=cls, post_load=_check_obj_auth)
        assert isinstance(obj, RedisModelUserIdAuth)
        return obj  # type: ignore  # 2024-01-30 # TODO: Incompatible return value type (got "RedisModelUserIdAuth", expected "TRedisModel")  [return-value]


def _check_obj_auth(obj: RedisModelUserIdAuth) -> None:
    if obj.user_id == obj.system_user_id:
        LOGGER.info("Attempt to get and object owned by system with authorization")
        raise RedisModelAccessDenied()

    manager = obj._manager  # noqa
    assert manager is not None and manager.rci is not None
    current_user_id = manager.rci.user_id

    if current_user_id is None:
        LOGGER.info("Current user_id not set. Unable to check authorization,")
        raise RedisModelAccessDenied()
    if obj.user_id != current_user_id:
        LOGGER.info(f"Current user_id and loaded object user_id don't match: {obj.user_id} != {current_user_id}")
        raise RedisModelAccessDenied()


@attr.s(init=True, kw_only=True)
class RedisSetManager(metaclass=abc.ABCMeta):
    KEY_PREFIX: ClassVar[str]
    _redis: redis.asyncio.Redis = attr.ib()

    id: str = attr.ib()

    @classmethod
    def _generate_key_by_id(cls, obj_id: str) -> str:
        return "/".join((cls.KEY_PREFIX, obj_id))

    @property
    def key(self) -> str:
        return self._generate_key_by_id(self.id)

    async def add(self, value: str) -> None:
        cmd = self._redis.sadd(self.key, value)
        assert isinstance(cmd, Awaitable)
        await cmd

    async def remove(self, value: str) -> None:
        cmd = self._redis.srem(self.key, value)
        assert isinstance(cmd, Awaitable)
        await cmd

    async def _sscan_iter_str(self) -> AsyncIterator:
        async for value in self._redis.sscan_iter(name=self.key):
            yield value.decode("utf-8")

    def sscan_iter(self) -> AsyncIterator:
        """
        Note: according to redis docs (https://redis.io/commands/scan/#scan-guarantees),
        may return some entries more than once, the client should handle this behaviour on its side.
        """

        return self._sscan_iter_str()

    async def delete(self) -> None:
        cmd = self._redis.delete(self.key)
        assert isinstance(cmd, Awaitable)
        await cmd

    async def size(self) -> int:
        cmd = self._redis.scard(self.key)
        assert isinstance(cmd, Awaitable)
        return await cmd

    async def sunion_by_id(self, *ids: str) -> None:
        if not ids:
            raise ValueError("ids can't be empty")
        keys = [self._generate_key_by_id(_id) for _id in ids]
        cmd = self._redis.sunionstore(self.key, keys)
        assert isinstance(cmd, Awaitable)
        await cmd


@attr.s
class RedisModelManager:
    _redis: redis.asyncio.Redis = attr.ib()
    _crypto_keys_config: CryptoKeysConfig = attr.ib()
    _crypto_controller: CryptoController = attr.ib(
        init=False,
        default=attr.Factory(
            lambda self: CryptoController(self._crypto_keys_config),
            takes_self=True,
        ),
    )
    rci: Optional[RequestContextInfo] = attr.ib(default=None)

    def _set_obj_attr(self, obj: RedisModel, key: DataKey, value: Any) -> None:
        key_head = DataKey(parts=key.parts[:-1])
        obj = self._get_obj_attr(obj, key_head)
        obj.__setattr__(key.parts[-1], value)

    def _get_obj_attr(self, obj: RedisModel, key: DataKey) -> Any:
        return reduce(lambda _obj, attribute: _obj.__getattribute__(attribute), key.parts, obj)

    def _serialize_object(self, obj: RedisModel) -> str:
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

    def _deserialize_object(self, json_data: str, target_cls: Type[RedisModel]) -> RedisModel:
        schema_cls = _get_model_schema_class(target_cls)
        schema = schema_cls()
        obj = schema.loads(json_data)

        assert isinstance(obj, RedisModel)
        secret_keys = obj.get_secret_keys()
        for key in secret_keys:
            encrypted = json.loads(self._get_obj_attr(obj, key))
            decrypted = self._crypto_controller.decrypt(encrypted)
            self._set_obj_attr(obj, key, decrypted)

        return obj

    async def get(self, key: str, target_cls: Type[RedisModel], post_load: Optional[Callable] = None) -> RedisModel:
        json_data = await self._redis.get(key)
        if json_data is None:
            LOGGER.info(f"RedisModel object not found: {key}")
            raise RedisModelNotFound()

        obj = self._deserialize_object(json_data, target_cls)
        obj._manager = self

        if post_load is not None:
            post_load(obj)

        return obj

    async def save(self, obj: RedisModel, ttl: Union[int, timedelta, None] = None) -> None:
        obj_key = obj.generate_key()
        data_json = self._serialize_object(obj)

        if ttl is None and obj.DEFAULT_TTL is not None:
            ttl = obj.DEFAULT_TTL

        if ttl:
            await self._redis.setex(obj_key, ttl, data_json)
        else:
            await self._redis.set(obj_key, data_json)

    async def is_persistent(self, obj: RedisModel) -> bool:
        """Check if object in redis has infinite ttl"""

        obj_key = obj.generate_key()

        ttl = await self._redis.ttl(obj_key)

        if ttl == -2:
            LOGGER.info(f"RedisModel object not found: {obj_key}")
            raise RedisModelNotFound()

        return ttl == -1

    async def delete(self, obj: RedisModel) -> None:
        obj_key = obj.generate_key()
        await self._redis.delete(obj_key)


class BaseSchema(ma.Schema):
    class Meta:
        unknown = ma.EXCLUDE

        target: Type

    @ma.post_load(pass_many=False)
    def to_object(self, data: dict[str, Any], **kwargs: Any) -> Any:
        return self.Meta.target(**data)


class BaseModelSchema(BaseSchema):
    id = ma.fields.String()
    user_id = ma.fields.String()


_MODEL_CLASS_TO_STORAGE_SCHEMA_MAP: dict[Type[RedisModel], Type[BaseModelSchema]] = dict()


def _get_model_schema_class(model_cls: Type[RedisModel]) -> Type[BaseModelSchema]:
    return _MODEL_CLASS_TO_STORAGE_SCHEMA_MAP[model_cls]


def register_redis_model_storage_schema(model_cls: Type[RedisModel], schema_cls: Type[BaseModelSchema]) -> None:
    _MODEL_CLASS_TO_STORAGE_SCHEMA_MAP[model_cls] = schema_cls
