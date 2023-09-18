from typing import (
    ClassVar,
    Generic,
    Type,
    TypeVar,
)

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
    TARGET_CLS: ClassVar[Type[_TARGET_OBJECT_TV]]

    @classmethod
    def get_target_cls(cls) -> Type[_TARGET_OBJECT_TV]:
        return cls.TARGET_CLS

    def to_object(self, data: dict) -> _TARGET_OBJECT_TV:
        return self.get_target_cls()(**data)  # type: ignore

    @post_load(pass_many=False)
    def post_load(self, data, **_) -> _TARGET_OBJECT_TV:  # type: ignore
        obj = self.to_object(data)
        return obj
