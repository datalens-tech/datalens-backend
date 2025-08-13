import abc
from copy import deepcopy
import enum
from typing import (
    ClassVar,
    Optional,
)

import attr
import marshmallow
from marshmallow import fields
import pytest

from dl_attrs_model_mapper.base import (
    AttribDescriptor,
    ModelDescriptor,
)
from dl_attrs_model_mapper.marshmallow import ModelMapperMarshmallow
from dl_attrs_model_mapper.marshmallow_fields import FrozenStrMappingField
from dl_attrs_model_mapper.structs.mappings import (
    FrozenMappingStrToStrOrStrSeq,
    FrozenStrMapping,
)


class BioKind(enum.Enum):
    cat = "cat"
    dog = "dog"
    salmon = "salmon"


class MeowKind(enum.Enum):
    cute = enum.auto()
    aggressive = enum.auto()


@ModelDescriptor(
    is_abstract=True,
    children_type_discriminator_attr_name="kind",
    children_type_discriminator_aliases_attr_name="kind_aliases",
)
@attr.s
class Bio(metaclass=abc.ABCMeta):
    kind: ClassVar[BioKind]

    max_weight: int = attr.ib()


@ModelDescriptor(is_abstract=True)
@attr.s
class Animal(Bio, metaclass=abc.ABCMeta):
    pass
    # lungs_volume: int = attr.ib()


@ModelDescriptor(is_abstract=True)
@attr.s
class Fish(Bio, metaclass=abc.ABCMeta):
    max_depth: int = attr.ib()


@ModelDescriptor()
@attr.s
class Cat(Animal):
    kind = BioKind.cat

    meow_kind: MeowKind = attr.ib()


@ModelDescriptor()
@attr.s
class Dog(Animal):
    kind = BioKind.dog

    bark_level: int = attr.ib()


@ModelDescriptor()
@attr.s
class Salmon(Fish):
    kind = BioKind.salmon
    kind_aliases = ("Losos obecný",)

    max_caviar_volume: int = attr.ib()


@ModelDescriptor()
@attr.s
class Flat:
    aquarium: list[Fish] = attr.ib()
    guard_animal: Animal = attr.ib()
    owner: Bio = attr.ib()


def test_round_trip():
    mapper = ModelMapperMarshmallow()
    mapper.register_models([Flat, Dog, Cat, Salmon])

    schema_cls = mapper.get_or_create_schema_for_attrs_class(Flat)
    flat = Flat(
        aquarium=[
            Salmon(max_depth=-1, max_weight=100, max_caviar_volume=0),
        ],
        guard_animal=Cat(
            meow_kind=MeowKind.aggressive,
            max_weight=10,
        ),
        owner=Dog(
            max_weight=90,
            bark_level=100500,
        ),
    )

    schema = schema_cls()

    serialized = schema.dump(flat)
    restored_flat = schema.load(serialized)
    assert restored_flat == flat


@ModelDescriptor()
@attr.s
class UnsafeFlat:
    guard_animal: Optional[Animal] = attr.ib(metadata=AttribDescriptor(skip_none_on_dump=True).to_meta())
    owner: Bio = attr.ib()


def test_dump_with_null_fields():
    mapper = ModelMapperMarshmallow()
    mapper.register_models(
        [
            UnsafeFlat,
            Dog,
        ]
    )
    schema_cls = mapper.get_or_create_schema_for_attrs_class(UnsafeFlat)
    flat = UnsafeFlat(
        guard_animal=None,
        owner=Dog(
            max_weight=90,
            bark_level=100500,
        ),
    )

    schema = schema_cls()
    serialized = schema.dump(flat)
    assert serialized == {"owner": {"bark_level": 100500, "kind": "dog", "max_weight": 90}}


def test_kind_alias():
    mapper = ModelMapperMarshmallow()
    mapper.register_models(
        [
            Flat,
            Dog,
            Cat,
            Salmon,
        ]
    )
    schema_cls = mapper.get_or_create_schema_for_attrs_class(Flat)
    flat = Flat(
        aquarium=[
            Salmon(max_depth=-1, max_weight=100, max_caviar_volume=0),
        ],
        guard_animal=Cat(
            meow_kind=MeowKind.aggressive,
            max_weight=10,
        ),
        owner=Dog(
            max_weight=90,
            bark_level=100500,
        ),
    )
    schema = schema_cls()

    serialized = schema.dump(flat)

    adjusted = deepcopy(serialized)
    adjusted["aquarium"][0]["type"] = "Losos obecný"

    restored_flat = schema.load(adjusted)
    assert restored_flat == flat


def test_enum_by_value():
    class EnumByValue(enum.Enum):
        x = "the-x"
        y = "the-y"

    class EnumByName(enum.Enum):
        a = "the-a"
        b = "the-b"

    @ModelDescriptor()
    @attr.s
    class Target:
        axis: EnumByValue = attr.ib(metadata=AttribDescriptor(enum_by_value=True).to_meta())
        ab: EnumByName = attr.ib()

    mapper = ModelMapperMarshmallow()
    mapper.register_models(
        [
            Target,
        ]
    )

    schema_cls = mapper.get_or_create_schema_for_attrs_class(Target)

    target = Target(axis=EnumByValue.x, ab=EnumByName.b)

    serialized = schema_cls().dump(target)

    assert serialized == dict(axis=target.axis.value, ab=target.ab.name)

    restored_target = schema_cls().load(serialized)
    assert restored_target == target


def test_nested_containers():
    @ModelDescriptor()
    @attr.s(auto_attribs=True)
    class Point:
        x: int
        y: int

    @ModelDescriptor()
    @attr.s
    class Target:
        list_of_lists_of_ints: list[list[int]] = attr.ib()
        list_of_lists_of_lists_of_points: list[list[list[Point]]] = attr.ib()

    mapper = ModelMapperMarshmallow()
    mapper.register_models(
        [
            Point,
            Target,
        ]
    )

    schema_cls = mapper.get_or_create_schema_for_attrs_class(Target)

    target = Target(
        list_of_lists_of_ints=[[1, 2, 3], [6, 7, 8]],
        list_of_lists_of_lists_of_points=[[[Point(1, 1)], [Point(2, 2)]], [[Point(3, 3), Point(4, 4)]]],
    )

    serialized = schema_cls().dump(target)

    assert serialized == dict(
        list_of_lists_of_ints=target.list_of_lists_of_ints,
        list_of_lists_of_lists_of_points=[
            [[attr.asdict(point) for point in l2] for l2 in l1] for l1 in target.list_of_lists_of_lists_of_points
        ],
    )

    restored_target = schema_cls().load(serialized)
    assert restored_target == target


def test_FrozenMappingStrToStrOrStrSeqField():
    @ModelDescriptor()
    @attr.s(auto_attribs=True)
    class Container:
        m: FrozenMappingStrToStrOrStrSeq

    mapper = ModelMapperMarshmallow()
    mapper.register_models([Container])

    mapping_data = {
        "de_havilland": ["Trident", "Comet"],
        "boeing": "Clipper",
        "antonov": "AN-124",
    }

    target = Container(m=FrozenMappingStrToStrOrStrSeq(mapping_data))

    schema_cls = mapper.get_or_create_schema_for_attrs_class(Container)

    # Serialization
    serialized = schema_cls().dump(target)
    assert serialized == dict(
        m=mapping_data,
    )

    # Deserialization
    deserialized = schema_cls().load(serialized)
    assert deserialized == target


def test_frozen_str_mapping():
    @ModelDescriptor()
    @attr.s(auto_attribs=True)
    class ContainerStr:
        m: FrozenStrMapping[str]

    mapper = ModelMapperMarshmallow()
    mapper.register_models([ContainerStr])

    mapping_data = {
        "de_havilland": "Comet",
        "boeing": "Clipper",
        "antonov": "AN-124",
    }

    target = ContainerStr(m=FrozenStrMapping(mapping_data))

    schema_cls = mapper.get_or_create_schema_for_attrs_class(ContainerStr)

    # Serialization
    serialized = schema_cls().dump(target)
    assert serialized == dict(
        m=mapping_data,
    )

    # Deserialization
    deserialized = schema_cls().load(serialized)
    assert deserialized == target

    # Deserialization errors
    with pytest.raises(marshmallow.ValidationError) as exc_pack:
        schema_cls().load(dict(m={"de_havilland": None}))

    assert exc_pack.value.messages == {
        "m": {"de_havilland": {"value": ["Field may not be null."]}},
    }


def test_serialization_key():
    @ModelDescriptor()
    @attr.s(auto_attribs=True)
    class Point:
        x: int = attr.ib(metadata=AttribDescriptor(serialization_key="the_x").to_meta())
        y: int

    @ModelDescriptor()
    @attr.s
    class Target:
        list_of_points: list[Point] = attr.ib(metadata=AttribDescriptor(serialization_key="lop").to_meta())
        list_of_ints: list[int] = attr.ib(metadata=AttribDescriptor(serialization_key="loint").to_meta())
        some_long_attr_name: int = attr.ib(metadata=AttribDescriptor(serialization_key="slan").to_meta())
        no_serialization_key: str = attr.ib()
        point: Point = attr.ib(metadata=AttribDescriptor(serialization_key="p").to_meta())

    mapper = ModelMapperMarshmallow()
    mapper.register_models([Target])
    schema_cls = mapper.get_or_create_schema_for_attrs_class(Target)

    target = Target(
        list_of_points=[Point(x=0, y=0)],
        list_of_ints=[1, 2, 3],
        some_long_attr_name=1984,
        no_serialization_key="no_serialization_key_value",
        point=Point(x=-1, y=-1),
    )

    serialized = schema_cls().dump(target)

    assert serialized == dict(
        lop=[dict(the_x=0, y=0)],
        loint=[1, 2, 3],
        slan=1984,
        no_serialization_key="no_serialization_key_value",
        p=dict(the_x=-1, y=-1),
    )

    restored = schema_cls().load(serialized)

    assert restored == target


@attr.s(frozen=True)
class MAFieldProjection:
    MA_TYPE: ClassVar[type[fields.Field]]
    MAP_MA_FIELD_CLS_PROJECTION_CLS: ClassVar[dict[type[fields.Field], type["MAFieldProjection"]]] = {}

    allow_none: bool = attr.ib()
    attribute: str = attr.ib()
    required: bool = attr.ib()

    def __init_subclass__(cls, **kwargs):
        cls.MAP_MA_FIELD_CLS_PROJECTION_CLS[cls.MA_TYPE] = cls

    @classmethod
    def get_default_kwargs(cls, ma_field: fields.Field) -> dict:
        return dict(
            allow_none=ma_field.allow_none,
            attribute=ma_field.attribute,
            required=ma_field.required,
        )

    @classmethod
    def project(cls, ma_field: fields.Field) -> "MAFieldProjection":
        return cls(**cls.get_default_kwargs(ma_field))

    @classmethod
    def project_generic(cls, ma_field: fields.Field) -> "MAFieldProjection":
        return cls.MAP_MA_FIELD_CLS_PROJECTION_CLS[type(ma_field)].project(ma_field)


@attr.s(frozen=True)
class MAIntProjection(MAFieldProjection):
    MA_TYPE = fields.Integer


@attr.s()
class MAStringProjection(MAFieldProjection):
    MA_TYPE = fields.String


@attr.s()
class MABoolProjection(MAFieldProjection):
    MA_TYPE = fields.Boolean


@attr.s()
class MAListFieldProjection(MAFieldProjection):
    MA_TYPE = fields.List

    item: MAFieldProjection = attr.ib()

    @classmethod
    def project(cls, ma_field: fields.List) -> "MAFieldProjection":
        return cls(item=cls.project_generic(ma_field.inner), **cls.get_default_kwargs(ma_field))


@attr.s()
class MAFrozenMappingProjection(MAFieldProjection):
    MA_TYPE = FrozenStrMappingField

    key_field: MAFieldProjection = attr.ib()
    value_field: MAFieldProjection = attr.ib()

    @classmethod
    def project(cls, ma_field: FrozenStrMappingField) -> "MAFieldProjection":
        return cls(
            key_field=cls.project_generic(ma_field.key_field),
            value_field=cls.project_generic(ma_field.value_field),
            **cls.get_default_kwargs(ma_field),
        )


def project_schema(schema: marshmallow.Schema) -> dict[str, MAFieldProjection]:
    return {name: MAFieldProjection.project_generic(field) for name, field in schema.fields.items()}


@ModelDescriptor()
@attr.s(auto_attribs=True, kw_only=True)
class Target1:
    class ExpectedSchema(marshmallow.Schema):
        a = fields.List(fields.Integer(), attribute="a", required=True)
        optional_str = fields.String(attribute="optional_str", allow_none=True, required=True)
        defaulted_optional_str = fields.String(attribute="defaulted_optional_str", allow_none=True, required=False)
        strict_bool = fields.Boolean(attribute="strict_bool", required=True)
        list_of_lists_of_str = fields.List(
            fields.List(fields.String()), attribute="list_of_lists_of_str", required=True
        )
        optional_list_of_str = fields.List(
            fields.String(), attribute="optional_list_of_str", allow_none=True, required=True
        )
        list_of_optional_str = fields.List(
            fields.String(allow_none=True), attribute="list_of_optional_str", required=True
        )
        defaulted_list = fields.List(fields.String(allow_none=True), attribute="defaulted_list", required=False)

    a: list[int]
    optional_str: Optional[str]
    defaulted_optional_str: Optional[str] = attr.ib(default=None)
    strict_bool: bool
    list_of_lists_of_str: list[list[str]]
    optional_list_of_str: Optional[list[str]]
    list_of_optional_str: list[Optional[str]]
    defaulted_list: list[Optional[str]] = attr.ib(factory=lambda: [None])


@ModelDescriptor()
@attr.s(auto_attribs=True, kw_only=True)
class TargetVariousMappings:
    class ExpectedSchema(marshmallow.Schema):
        map_str_str = FrozenStrMappingField(
            keys=fields.String(),
            values=fields.String(allow_none=False),
            attribute="map_str_str",
            required=True,
        )
        map_str_optional_str = FrozenStrMappingField(
            keys=fields.String(),
            values=fields.String(allow_none=True),
            attribute="map_str_optional_str",
            required=True,
        )

    map_str_str: FrozenStrMapping[str]
    map_str_optional_str: FrozenStrMapping[Optional[str]]


@pytest.mark.parametrize(
    "main_cls,extra_cls_list",
    [
        [Target1, []],
        [TargetVariousMappings, []],
    ],
)
def test_schema_generation(main_cls: type, extra_cls_list: list[type]):
    mapper = ModelMapperMarshmallow()
    mapper.register_models(extra_cls_list)
    mapper.register_models([main_cls])

    generated_schema_cls = mapper.get_or_create_schema_for_attrs_class(main_cls)

    generated_schema = generated_schema_cls()
    expected_schema = main_cls.ExpectedSchema()

    assert project_schema(generated_schema) == project_schema(expected_schema)
