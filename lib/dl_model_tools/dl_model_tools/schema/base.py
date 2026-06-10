from __future__ import annotations

from typing import (
    Any,
    ClassVar,
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


class DefaultSchema[TARGET_OBJECT_TV](BaseSchema):
    TARGET_CLS: ClassVar[type[TARGET_OBJECT_TV]]

    @classmethod
    def get_target_cls(cls) -> type[TARGET_OBJECT_TV]:
        return cls.TARGET_CLS

    def to_object(self, data: dict) -> TARGET_OBJECT_TV:
        return self.get_target_cls()(**data)

    @post_load(pass_many=False)
    def post_load(self, data: dict[str, Any], **_: Any) -> TARGET_OBJECT_TV:
        obj = self.to_object(data)
        return obj


class DefaultValidateSchema[TARGET_ATTRS_OBJECT_TV: attr.AttrsInstance](DefaultSchema[TARGET_ATTRS_OBJECT_TV]):
    @post_load(pass_many=False)
    def post_load(self, data: dict[str, Any], **_: Any) -> TARGET_ATTRS_OBJECT_TV:
        data_with_orig = data.copy()
        obj = self.to_object(data_with_orig)
        as_dict = attr.asdict(obj, recurse=False)
        as_dict = {k: v for k, v in as_dict.items() if v is not None}
        data = {k: v for k, v in data.items() if v is not None}
        assert data == as_dict
        return obj
