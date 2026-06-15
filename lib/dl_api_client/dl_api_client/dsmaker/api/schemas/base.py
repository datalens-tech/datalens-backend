from typing import (
    Any,
    ClassVar,
)

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
    def post_load(self, data: dict, **_: Any) -> TARGET_OBJECT_TV:
        return self.to_object(data)
