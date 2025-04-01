from __future__ import annotations

from typing import (
    Any,
    ClassVar,
    Generic,
    TypeVar,
)

import attr
from marshmallow import (
    EXCLUDE,
    Schema,
    post_load,
)


class BaseSchema(Schema):
    class Meta:
        unknown = EXCLUDE


_TARGET_OBJECT_TV = TypeVar("_TARGET_OBJECT_TV")


class DefaultSchema(BaseSchema, Generic[_TARGET_OBJECT_TV]):
    TARGET_CLS: ClassVar[type[_TARGET_OBJECT_TV]]  # type: ignore  # 2024-01-24 # TODO: ClassVar cannot contain type variables  [misc]

    @classmethod
    def get_target_cls(cls) -> type[_TARGET_OBJECT_TV]:
        return cls.TARGET_CLS

    def to_object(self, data: dict) -> _TARGET_OBJECT_TV:
        return self.get_target_cls()(**data)

    @post_load(pass_many=False)
    def post_load(self, data: dict[str, Any], **_: Any) -> _TARGET_OBJECT_TV:
        obj = self.to_object(data)
        return obj


_TARGET_ATTRS_OBJECT_TV = TypeVar("_TARGET_ATTRS_OBJECT_TV", bound=attr.AttrsInstance)


class DefaultValidateSchema(DefaultSchema[_TARGET_ATTRS_OBJECT_TV], Generic[_TARGET_ATTRS_OBJECT_TV]):
    @post_load(pass_many=False)
    def post_load(self, data: dict[str, Any], **_: Any) -> _TARGET_ATTRS_OBJECT_TV:
        data_with_orig = data.copy()
        obj = self.to_object(data_with_orig)
        as_dict = attr.asdict(obj, recurse=False)
        as_dict = {k: v for k, v in as_dict.items() if v is not None}
        data = {k: v for k, v in data.items() if v is not None}
        assert data == as_dict
        return obj
