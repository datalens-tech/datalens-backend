from __future__ import annotations

from typing import Generic, ClassVar, Type, TypeVar, Dict, Tuple, Set

from marshmallow import Schema, post_load
from marshmallow_oneofschema import OneOfSchema

_TARGET_OBJECT_TV = TypeVar('_TARGET_OBJECT_TV')


class BaseSchema(Schema, Generic[_TARGET_OBJECT_TV]):
    TARGET_CLS: ClassVar[Type[_TARGET_OBJECT_TV]]

    @classmethod
    def get_target_cls(cls) -> Type[_TARGET_OBJECT_TV]:
        return cls.TARGET_CLS

    def to_object(self, data: dict) -> _TARGET_OBJECT_TV:
        return self.get_target_cls()(**data)  # type: ignore  # TODO: fix

    @post_load(pass_many=False)
    def post_load(self, data, **_) -> _TARGET_OBJECT_TV:  # type: ignore  # TODO: fix
        obj = self.to_object(data)
        return obj


class GenericSchemaBase(OneOfSchema):
    sub_schemas: Dict[str, Tuple[BaseSchema, bool]]
    map_cls_type_disc: Dict[Type, str]

    @classmethod
    def __init_subclass__(cls, **kwargs):  # type: ignore  # TODO: fix
        if not cls.__name__.endswith('Base'):
            cls.type_schemas = {  # type: ignore  # TODO: fix
                type_disc: schema_cls
                for type_disc, (schema_cls, _) in cls.sub_schemas.items()
            }

            map_cls_type_disc_df_set: Dict[Type, Set[Tuple[str, bool]]] = {}

            for type_disc, (schema_cls, is_default) in cls.sub_schemas.items():
                target_cls = schema_cls.TARGET_CLS

                type_disc_set = map_cls_type_disc_df_set.setdefault(target_cls, set())
                type_disc_set.add((type_disc, is_default))

            map_cls_type_disc: Dict[Type, str] = {}

            for target_cls, type_disc_df_flag_set in map_cls_type_disc_df_set.items():
                default_type_disc_set = {type_disc for type_disc, is_default in type_disc_df_flag_set if is_default}
                if len(default_type_disc_set) != 1:
                    raise AssertionError(
                        f"Got non single type discriminator for class {target_cls.__name__}: {default_type_disc_set!r}"
                    )

                map_cls_type_disc[target_cls] = default_type_disc_set.pop()

            cls.map_cls_type_disc = map_cls_type_disc

    def get_obj_type(self, obj):  # type: ignore  # TODO: fix
        return self.map_cls_type_disc.get(type(obj))
