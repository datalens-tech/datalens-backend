import pickle

from dynamic_enum import (
    AutoEnumValue,
    DynamicEnum,
)
import pytest


def test_auto_value():
    class Color(DynamicEnum):
        blue = AutoEnumValue()

    color = Color.blue
    assert color.value == "blue"


def test_deduplication():
    class Weight(DynamicEnum):
        heavy = AutoEnumValue()

    assert Weight("heavy") is Weight.heavy


def test_declaration():
    class Size(DynamicEnum):
        pass

    with pytest.raises(ValueError):
        Size("medium")  # value 'medium' has not been declared yet

    medium_size = Size.declare("medium")
    assert medium_size.value == "medium"

    # Now we can call the constructor directly, since the value has already been declared
    medium_size = Size("medium")
    assert medium_size.value == "medium"

    with pytest.raises(ValueError):
        Size.declare("medium")  # value 'medium' has already been declared


def test_subclassing():
    class Something(DynamicEnum):
        smth = AutoEnumValue()

    with pytest.raises(TypeError):

        class SpecificSomething(Something):
            pass


def test_comparison():
    class ThisWorld(DynamicEnum):
        thing = AutoEnumValue()

    class OtherWorld(DynamicEnum):
        thing = AutoEnumValue()

    assert ThisWorld.thing == ThisWorld.thing
    assert ThisWorld.thing is ThisWorld.thing

    assert OtherWorld.thing != ThisWorld.thing
    assert OtherWorld.thing is not ThisWorld.thing


def test_stringification():
    class What(DynamicEnum):
        that = AutoEnumValue()

    assert str(What("that")) == "What('that')"
    assert repr(What("that")) == "What('that')"


def test_getitem():
    class What(DynamicEnum):
        pass

    What.declare("that")

    assert What["that"] == What("that")


def test_containment():
    class Galaxy(DynamicEnum):
        pass

    Galaxy.declare("milky_way")
    Galaxy.declare("andromeda")

    assert "milky_way" in Galaxy
    assert "andromeda" in Galaxy
    assert "far_far_away" not in Galaxy


def test_iter():
    class Everything(DynamicEnum):
        pass

    Everything.declare("this")
    Everything.declare("that")

    assert set(Everything) == {Everything("this"), Everything("that")}


def test_no_instance_dict():
    class It(DynamicEnum):
        pass

    It.declare("thing")
    assert not hasattr(It("thing"), "__dict__")


def test_custom_slots():
    with pytest.raises(TypeError):

        class ExtendedEnum(DynamicEnum):
            __slots__ = ("this_other thing",)


class Picklable(DynamicEnum):
    thing = AutoEnumValue()


def test_pickle():
    data = pickle.dumps(Picklable.thing)
    loaded = pickle.loads(data)

    assert loaded is Picklable.thing
