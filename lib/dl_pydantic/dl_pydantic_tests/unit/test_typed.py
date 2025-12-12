import typing

import pydantic
import pytest

import dl_pydantic


def test_default() -> None:
    class Base(dl_pydantic.TypedBaseModel):
        ...

    class Child(Base):
        ...

    class Child2(Base):
        ...

    Base.register("child", Child)
    Base.register("child2", Child2)

    assert isinstance(Base.factory({"type": "child"}), Child)
    assert isinstance(Base.factory({"type": "child2"}), Child2)


def test_type_field_name_alias() -> None:
    class Base(dl_pydantic.TypedBaseModel):
        type: str = pydantic.Field(alias="test_type_field_name")

    class Child(Base):
        ...

    Base.register("child", Child)

    assert isinstance(Base.factory({"test_type_field_name": "child"}), Child)


def test_already_deserialized() -> None:
    class Base(dl_pydantic.TypedBaseModel):
        ...

    class Child(Base):
        ...

    Base.register("child", Child)
    child = Child.model_validate({"type": "child"})

    assert isinstance(Base.factory(child), Child)


def test_not_a_dict_data() -> None:
    class Base(dl_pydantic.TypedBaseModel):
        ...

    class Child(Base):
        ...

    Base.register("child", Child)

    class Root(dl_pydantic.BaseModel):
        child: Child

    with pytest.raises(ValueError):
        Root.model_validate({"child": ""})


def test_already_registered() -> None:
    class Base(dl_pydantic.TypedBaseModel):
        ...

    class Child(Base):
        ...

    Base.register("child", Child)
    with pytest.raises(ValueError):
        Base.register("child", Child)


def test_not_subclass() -> None:
    class Base(dl_pydantic.TypedBaseModel):
        ...

    class Base2(dl_pydantic.TypedBaseModel):
        ...

    class Child(Base2):
        ...

    with pytest.raises(ValueError):
        Base.register("child", Child)


def test_unknown_type() -> None:
    class Base(dl_pydantic.TypedBaseModel):
        ...

    with pytest.raises(ValueError):
        Base.factory({"type": "child"})


def test_multiple_bases() -> None:
    class Base(dl_pydantic.TypedBaseModel):
        ...

    class Base2(dl_pydantic.TypedBaseModel):
        ...

    class Child(Base):
        ...

    class Child2(Base2):
        ...

    Base.register("child", Child)
    Base2.register("child", Child2)

    assert isinstance(Base.factory({"type": "child"}), Child)
    assert isinstance(Base2.factory({"type": "child"}), Child2)


def test_list_factory_default() -> None:
    class Base(dl_pydantic.TypedBaseModel):
        ...

    class Child(Base):
        ...

    Base.register("child", Child)

    children = Base.list_factory([{"type": "child"}])

    assert isinstance(children[0], Child)


def test_list_factory_not_sequence() -> None:
    class Base(dl_pydantic.TypedBaseModel):
        ...

    class Child(Base):
        ...

    Base.register("child", Child)

    with pytest.raises(ValueError):
        Base.list_factory(typing.cast(list, "test"))


def test_dict_factory_default() -> None:
    class Base(dl_pydantic.TypedBaseModel):
        ...

    class Child(Base):
        ...

    Base.register("child", Child)

    children = Base.dict_factory({"child": {"type": "child"}})

    assert isinstance(children["child"], Child)


def test_dict_factory_not_dict() -> None:
    class Base(dl_pydantic.TypedBaseModel):
        ...

    class Child(Base):
        ...

    Base.register("child", Child)

    with pytest.raises(ValueError):
        Base.dict_factory(typing.cast(dict, "test"))


def test_annotation() -> None:
    class Base(dl_pydantic.TypedBaseModel):
        ...

    class Child(Base):
        ...

    Base.register("child", Child)

    class Root(dl_pydantic.BaseModel):
        child: dl_pydantic.TypedAnnotation[Base]

    root = Root.model_validate({"child": {"type": "child"}})

    assert isinstance(root.child, Child)


def test_optional_annotation() -> None:
    class Base(dl_pydantic.TypedBaseModel):
        ...

    class Child(Base):
        ...

    Base.register("child", Child)

    class Root(dl_pydantic.BaseModel):
        child: typing.Optional[dl_pydantic.TypedAnnotation[Base]] = None

    root = Root.model_validate({"child": {"type": "child"}})
    assert isinstance(root.child, Child)

    root = Root.model_validate({"child": None})
    assert root.child is None

    root = Root.model_validate({})
    assert root.child is None


def test_list_annotation() -> None:
    class Base(dl_pydantic.TypedBaseModel):
        ...

    class Child(Base):
        ...

    Base.register("child", Child)

    class Root(dl_pydantic.BaseModel):
        children: dl_pydantic.TypedListAnnotation[Base] = pydantic.Field(default_factory=list)

    root = Root.model_validate({"children": [{"type": "child"}]})

    assert isinstance(root.children[0], Child)


def test_dict_annotation() -> None:
    class Base(dl_pydantic.TypedBaseModel):
        ...

    class Child(Base):
        ...

    Base.register("child", Child)

    class Root(dl_pydantic.BaseModel):
        children: dl_pydantic.TypedDictAnnotation[Base] = pydantic.Field(default_factory=dict)

    root = Root.model_validate({"children": {"child": {"type": "child"}}})

    assert isinstance(root.children["child"], Child)


def test_model_json_schema() -> None:
    class Base(dl_pydantic.TypedBaseModel):
        base: str

    class Child(Base):
        text: str

    class Child2(Base):
        number: int

    Base.register("text_child", Child)
    Base.register("int_child", Child2)

    class Root(dl_pydantic.BaseModel):
        children: dl_pydantic.TypedAnnotation[Base]

    schema = Root.model_json_schema()

    expected_schema = {
        "properties": {
            "children": {"$ref": "#/$defs/Base"},
        },
        "required": ["children"],
        "title": "Root",
        "type": "object",
        "$defs": {
            "Base": {
                "oneOf": [
                    {
                        "properties": {
                            "base": {"title": "Base", "type": "string"},
                            "type": {"title": "Type", "type": "string", "enum": ["int_child"]},
                            "number": {"title": "Number", "type": "integer"},
                        },
                        "required": ["type", "base", "number"],
                        "title": "Child2",
                        "type": "object",
                    },
                    {
                        "properties": {
                            "base": {"title": "Base", "type": "string"},
                            "type": {"title": "Type", "type": "string", "enum": ["text_child"]},
                            "text": {"title": "Text", "type": "string"},
                        },
                        "required": ["type", "base", "text"],
                        "title": "Child",
                        "type": "object",
                    },
                ]
            },
        },
    }

    assert schema == expected_schema


def test_model_json_schema_with_nested_models() -> None:
    class Child(dl_pydantic.BaseModel):
        value: int

    class Base(dl_pydantic.TypedBaseModel):
        type: str = pydantic.Field(alias="test_type")

    class NestedChild(Base):
        value: str
        child: Child

    Base.register("nested_child", NestedChild)

    class Root(dl_pydantic.BaseModel):
        name: str
        child: dl_pydantic.TypedAnnotation[Base]

    schema = Root.model_json_schema()

    expected_schema = {
        "properties": {
            "name": {"title": "Name", "type": "string"},
            "child": {"$ref": "#/$defs/Base"},
        },
        "required": ["name", "child"],
        "title": "Root",
        "type": "object",
        "$defs": {
            "Child": {
                "properties": {"value": {"title": "Value", "type": "integer"}},
                "required": ["value"],
                "title": "Child",
                "type": "object",
            },
            "Base": {
                "oneOf": [
                    {
                        "properties": {
                            "test_type": {"title": "Test Type", "type": "string", "enum": ["nested_child"]},
                            "value": {"title": "Value", "type": "string"},
                            "child": {"$ref": "#/$defs/Child"},
                        },
                        "required": ["test_type", "value", "child"],
                        "title": "NestedChild",
                        "type": "object",
                    }
                ]
            },
        },
    }

    assert schema == expected_schema


def test_model_json_schema_not_subclass() -> None:
    class Base(dl_pydantic.TypedBaseModel):
        ...

    with pytest.raises(ValueError):
        Base.model_json_schema()


def test_dict_annotation_with_type_key() -> None:
    class Base(dl_pydantic.TypedBaseModel):
        ...

    class Child(Base):
        ...

    Base.register("child", Child)

    class Root(dl_pydantic.BaseModel):
        children: dl_pydantic.TypedDictWithTypeKeyAnnotation[Base] = pydantic.Field(default_factory=dict)

    root = Root.model_validate({"children": {"child": {}}})

    assert isinstance(root.children["child"], Child)
