from enum import Enum
import json

import attr
from marshmallow import fields
import pytest

from dl_configs.crypto_keys import get_dummy_crypto_keys_config
from dl_file_uploader_lib.s3_model.base import (
    S3BaseModelSchema,
    S3BaseSchema,
    S3Model,
    S3ModelManager,
    S3ModelNotFound,
    SecretContainingMixin,
    register_s3_model_storage_schema,
)
from dl_s3.s3_service import S3Service
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
class SomeModel(S3Model):
    title: str = attr.ib()
    value: int = attr.ib()
    some_list: list[str] = attr.ib()
    enum_field: SomeEnum = attr.ib()
    sub_model: SomeSubModel = attr.ib()
    sub_model_with_secret: SomeSubModelWithSecret = attr.ib()

    def get_secret_keys(self) -> set[DataKey]:
        lower_level_keys = self.sub_model_with_secret.get_secret_keys()
        return {DataKey(parts=("sub_model_with_secret",) + ll_key.parts) for ll_key in lower_level_keys}


class SomeSubModelSchema(S3BaseSchema):
    class Meta(S3BaseSchema.Meta):
        target = SomeSubModel

    title = fields.String()
    value = fields.Integer()


class SomeSubModelWithSecretSchema(S3BaseSchema):
    class Meta(S3BaseSchema.Meta):
        target = SomeSubModelWithSecret

    secret = fields.String()


class SomeModelSchema(S3BaseModelSchema):
    class Meta(S3BaseModelSchema.Meta):
        target = SomeModel

    title = fields.String()
    value = fields.Integer()
    some_list = fields.List(fields.String)
    enum_field = fields.Enum(SomeEnum)
    sub_model = fields.Nested(SomeSubModelSchema)
    sub_model_with_secret = fields.Nested(SomeSubModelWithSecretSchema)


register_s3_model_storage_schema(SomeModel, SomeModelSchema)


@pytest.mark.parametrize(
    "persistent",
    (False, True),
)
@pytest.mark.asyncio
async def test_s3_model(s3_service: S3Service, persistent: bool) -> None:
    s3mm = S3ModelManager(
        s3_service=s3_service,
        tenant_id="some-tenant",
        crypto_keys_config=get_dummy_crypto_keys_config(),
    )
    s3_client = s3_service.get_sync_client()

    with pytest.raises(S3ModelNotFound):
        await SomeModel.get(manager=s3mm, obj_id="qwerty")

    obj_sub = SomeSubModel(title="qwe", value=12)
    obj_sub_w_secret = SomeSubModelWithSecret(secret="very very secret")
    obj = SomeModel(
        manager=s3mm,
        title="some model instance",
        value=42,
        some_list=[str(i) for i in range(10)],
        enum_field=SomeEnum.val1,
        sub_model=obj_sub,
        sub_model_with_secret=obj_sub_w_secret,
    )
    assert obj.id

    await obj.save(persistent=persistent)
    bucket_name = s3_service.persistent_bucket_name if persistent else s3_service.tmp_bucket_name

    s3_data = s3_client.get_object(
        Bucket=bucket_name,
        Key=obj.generate_key(),
    )["Body"].read()
    assert isinstance(s3_data, (bytes, bytearray))

    decoded_data = json.loads(s3_data.decode())
    assert decoded_data.pop("id")
    assert "cypher_text" in json.loads(decoded_data.pop("sub_model_with_secret")["secret"])
    assert decoded_data == {
        "some_list": ["0", "1", "2", "3", "4", "5", "6", "7", "8", "9"],
        "enum_field": "val1",
        "sub_model": {"value": 12, "title": "qwe"},
        "value": 42,
        "title": "some model instance",
    }
    obj1 = await SomeModel.get(manager=s3mm, obj_id=obj.id)
    assert obj1.title == "some model instance"
    assert obj1.sub_model_with_secret.secret == "very very secret"
    assert obj == obj1

    await obj.delete()
    with pytest.raises(S3ModelNotFound):
        await SomeModel.get(manager=s3mm, obj_id=obj.id)
