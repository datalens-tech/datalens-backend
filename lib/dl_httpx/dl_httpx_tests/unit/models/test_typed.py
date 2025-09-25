import typing

import pydantic
import pytest

import dl_httpx


def test_default() -> None:
    class Base(dl_httpx.TypedBaseSchema):
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
    class Base(dl_httpx.TypedBaseSchema):
        type: str = pydantic.Field(alias="test_type_field_name")

    class Child(Base):
        ...

    Base.register("child", Child)

    assert isinstance(Base.factory({"test_type_field_name": "child"}), Child)


def test_already_deserialized() -> None:
    class Base(dl_httpx.TypedBaseSchema):
        ...

    class Child(Base):
        ...

    Base.register("child", Child)
    child = Child.model_validate({"type": "child"})

    assert isinstance(Base.factory(child), Child)


def test_not_a_dict_data() -> None:
    class Base(dl_httpx.TypedBaseSchema):
        ...

    class Child(Base):
        ...

    Base.register("child", Child)

    class Root(dl_httpx.BaseResponseSchema):
        child: Child

    with pytest.raises(ValueError):
        Root.model_validate({"child": ""})


def test_already_registered() -> None:
    class Base(dl_httpx.TypedBaseSchema):
        ...

    class Child(Base):
        ...

    Base.register("child", Child)
    with pytest.raises(ValueError):
        Base.register("child", Child)


def test_not_subclass() -> None:
    class Base(dl_httpx.TypedBaseSchema):
        ...

    class Base2(dl_httpx.TypedBaseSchema):
        ...

    class Child(Base2):
        ...

    with pytest.raises(ValueError):
        Base.register("child", Child)


def test_unknown_type() -> None:
    class Base(dl_httpx.TypedBaseSchema):
        ...

    with pytest.raises(ValueError):
        Base.factory({"type": "child"})


def test_multiple_bases() -> None:
    class Base(dl_httpx.TypedBaseSchema):
        ...

    class Base2(dl_httpx.TypedBaseSchema):
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
    class Base(dl_httpx.TypedBaseSchema):
        ...

    class Child(Base):
        ...

    Base.register("child", Child)

    children = Base.list_factory([{"type": "child"}])

    assert isinstance(children[0], Child)


def test_list_factory_not_sequence() -> None:
    class Base(dl_httpx.TypedBaseSchema):
        ...

    class Child(Base):
        ...

    Base.register("child", Child)

    with pytest.raises(ValueError):
        Base.list_factory(typing.cast(list, "test"))


def test_dict_factory_default() -> None:
    class Base(dl_httpx.TypedBaseSchema):
        ...

    class Child(Base):
        ...

    Base.register("child", Child)

    children = Base.dict_factory({"child": {"type": "child"}})

    assert isinstance(children["child"], Child)


def test_dict_factory_not_dict() -> None:
    class Base(dl_httpx.TypedBaseSchema):
        ...

    class Child(Base):
        ...

    Base.register("child", Child)

    with pytest.raises(ValueError):
        Base.dict_factory(typing.cast(dict, "test"))


def test_annotation() -> None:
    class Base(dl_httpx.TypedBaseSchema):
        ...

    class Child(Base):
        ...

    Base.register("child", Child)

    class Root(dl_httpx.BaseResponseSchema):
        child: dl_httpx.TypedSchemaAnnotation[Base]

    root = Root.model_validate({"child": {"type": "child"}})

    assert isinstance(root.child, Child)


def test_optional_annotation() -> None:
    class Base(dl_httpx.TypedBaseSchema):
        ...

    class Child(Base):
        ...

    Base.register("child", Child)

    class Root(dl_httpx.BaseResponseSchema):
        child: typing.Optional[dl_httpx.TypedSchemaAnnotation[Base]] = None

    root = Root.model_validate({"child": {"type": "child"}})
    assert isinstance(root.child, Child)

    root = Root.model_validate({"child": None})
    assert root.child is None

    root = Root.model_validate({})
    assert root.child is None


def test_list_annotation() -> None:
    class Base(dl_httpx.TypedBaseSchema):
        ...

    class Child(Base):
        ...

    Base.register("child", Child)

    class Root(dl_httpx.BaseResponseSchema):
        children: dl_httpx.TypedSchemaListAnnotation[Base] = pydantic.Field(default_factory=list)

    root = Root.model_validate({"children": [{"type": "child"}]})

    assert isinstance(root.children[0], Child)


def test_dict_annotation() -> None:
    class Base(dl_httpx.TypedBaseSchema):
        ...

    class Child(Base):
        ...

    Base.register("child", Child)

    class Root(dl_httpx.BaseResponseSchema):
        children: dl_httpx.TypedSchemaDictAnnotation[Base] = pydantic.Field(default_factory=dict)

    root = Root.model_validate({"children": {"child": {"type": "child"}}})

    assert isinstance(root.children["child"], Child)
