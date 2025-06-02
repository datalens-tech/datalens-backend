from enum import Enum
import json
from typing import ClassVar
import uuid

import attr
from marshmallow import fields
import pytest
import redis.asyncio

from dl_api_commons.base_models import RequestContextInfo
from dl_configs.crypto_keys import get_dummy_crypto_keys_config
from dl_file_uploader_lib.redis_model.base import (
    BaseModelSchema,
    BaseSchema,
    RedisModel,
    RedisModelAccessDenied,
    RedisModelManager,
    RedisModelNotFound,
    RedisModelUserIdAuth,
    RedisSetManager,
    SecretContainingMixin,
    register_redis_model_storage_schema,
)
from dl_utils.utils import DataKey


class SomeEnum(Enum):
    val1 = "val1"
    val2 = "val2"


@attr.s
class SomeSubModel:
    title: str = attr.ib()
    value: int = attr.ib()


@attr.s
class SomeSubModelWithSecret(SecretContainingMixin):
    secret: str = attr.ib()

    def get_secret_keys(self) -> set[DataKey]:
        return {DataKey(parts=("secret",))}


@attr.s
class SomeModel(RedisModel):
    title: str = attr.ib()
    value: int = attr.ib()
    some_list: list[str] = attr.ib()
    enum_field: SomeEnum = attr.ib()
    sub_model: SomeSubModel = attr.ib()
    sub_model_with_secret: SomeSubModelWithSecret = attr.ib()

    KEY_PREFIX: ClassVar[str] = "some"

    def get_secret_keys(self) -> set[DataKey]:
        lower_level_keys = self.sub_model_with_secret.get_secret_keys()
        return {DataKey(parts=("sub_model_with_secret",) + ll_key.parts) for ll_key in lower_level_keys}


@attr.s
class SomeAuthorizedModel(RedisModelUserIdAuth):
    title: str = attr.ib()
    value: int = attr.ib()

    KEY_PREFIX: ClassVar[str] = "some_auth"


class SomeSubModelSchema(BaseSchema):
    class Meta(BaseSchema.Meta):
        target = SomeSubModel

    title = fields.String()
    value = fields.Integer()


class SomeSubModelWithSecretSchema(BaseSchema):
    class Meta(BaseSchema.Meta):
        target = SomeSubModelWithSecret

    secret = fields.String()


class SomeModelSchema(BaseModelSchema):
    class Meta(BaseModelSchema.Meta):
        target = SomeModel

    title = fields.String()
    value = fields.Integer()
    some_list = fields.List(fields.String)
    enum_field = fields.Enum(SomeEnum)
    sub_model = fields.Nested(SomeSubModelSchema)
    sub_model_with_secret = fields.Nested(SomeSubModelWithSecretSchema)


class SomeAuthorizedModelSchema(BaseModelSchema):
    class Meta(BaseModelSchema.Meta):
        target = SomeAuthorizedModel

    title = fields.String()
    value = fields.Integer()


register_redis_model_storage_schema(SomeModel, SomeModelSchema)
register_redis_model_storage_schema(SomeAuthorizedModel, SomeAuthorizedModelSchema)


@pytest.mark.asyncio
async def test_redis_model(redis_cli: redis.asyncio.Redis) -> None:
    rmm = RedisModelManager(redis=redis_cli, crypto_keys_config=get_dummy_crypto_keys_config())

    with pytest.raises(RedisModelNotFound):
        await SomeModel.get(manager=rmm, obj_id="qwerty")

    obj_sub = SomeSubModel(title="qwe", value=12)
    obj_sub_w_secret = SomeSubModelWithSecret(secret="very very secret")
    obj = SomeModel(
        manager=rmm,
        title="some model instance",
        value=42,
        some_list=[str(i) for i in range(10)],
        enum_field=SomeEnum.val1,
        sub_model=obj_sub,
        sub_model_with_secret=obj_sub_w_secret,
    )
    assert obj.id

    await obj.save()

    redis_data = await redis_cli.get(f"some/{obj.id}")
    assert isinstance(redis_data, (bytes, bytearray))

    decoded_data = json.loads(redis_data.decode())
    assert decoded_data.pop("id")
    assert "cypher_text" in json.loads(decoded_data.pop("sub_model_with_secret")["secret"])
    assert decoded_data == {
        "some_list": ["0", "1", "2", "3", "4", "5", "6", "7", "8", "9"],
        "enum_field": "val1",
        "sub_model": {"value": 12, "title": "qwe"},
        "value": 42,
        "title": "some model instance",
    }
    obj1 = await SomeModel.get(manager=rmm, obj_id=obj.id)
    assert obj1.title == "some model instance"
    assert obj1.sub_model_with_secret.secret == "very very secret"
    assert obj == obj1

    await obj.delete()
    with pytest.raises(RedisModelNotFound):
        await SomeModel.get(manager=rmm, obj_id=obj.id)


@pytest.mark.asyncio
async def test_authorized_redis_model(redis_cli: redis.asyncio.Redis) -> None:
    crypto_keys_config = get_dummy_crypto_keys_config()
    rmm_no_auth = RedisModelManager(redis=redis_cli, crypto_keys_config=crypto_keys_config)
    rmm_auth_1 = RedisModelManager(
        redis=redis_cli, rci=RequestContextInfo(user_id="123"), crypto_keys_config=crypto_keys_config
    )
    rmm_auth_2 = RedisModelManager(
        redis=redis_cli, rci=RequestContextInfo(user_id="456"), crypto_keys_config=crypto_keys_config
    )

    obj = SomeAuthorizedModel(manager=rmm_auth_1, title="qwe", value=12)
    initial_obj_id = obj.id
    assert obj.user_id == "123"

    await obj.save()

    obj1 = await SomeAuthorizedModel.get(manager=rmm_no_auth, obj_id=initial_obj_id)
    assert obj1.user_id == "123"

    obj1 = await SomeAuthorizedModel.get(manager=rmm_auth_1, obj_id=initial_obj_id)
    assert obj1.user_id == "123"

    obj1 = await SomeAuthorizedModel.get_authorized(manager=rmm_auth_1, obj_id=initial_obj_id)
    assert obj1.user_id == "123"

    with pytest.raises(RedisModelAccessDenied):
        await SomeAuthorizedModel.get_authorized(manager=rmm_auth_2, obj_id=initial_obj_id)


class SomeRedisSet(RedisSetManager):
    KEY_PREFIX = "some_set_prefix"


@pytest.mark.asyncio
async def test_redis_set(redis_cli: redis.asyncio.Redis) -> None:
    some_set = SomeRedisSet(redis=redis_cli, id=str(uuid.uuid4()))

    expected_values = {"a", "b", "c"}

    for value in expected_values:
        await some_set.add(value)
    set_size = await some_set.size()
    assert set_size == len(expected_values)

    actual_values = set()
    async for value in some_set.sscan_iter():
        actual_values.add(value)

    assert actual_values == expected_values

    await some_set.delete()
    c = 0
    async for _ in some_set.sscan_iter():
        c += 1
    assert c == 0
