"""
Intended as a replacement for `Enum`-based constants that are being spread out
across different packages and require distributed definition of values.

Usage examples.
Enum-like definition of values for compatibility with existing code:

    class Color(DynamicEnum):
        blue = AutoEnumValue()
        green = AutoEnumValue()

    Color.blue.value  # == 'blue'


Definition of values outside the class
(the case where values need to be defined in different packages):

    Color.declare('white')  # .value  == 'white'


After a value has been explicitly declared once (and only once)
via the `declare` classmethod or `AutoEnumValue` descriptor within the class itself,
the constant can be instantiated simply by using the constructor:

    Color('white')
    Color('green')

Calling the constructor on an undeclared value of declaring a value more than once
will raise a `ValueError`. This helps prevent the creation of invalid constant values
in arbitrary places of the code or when deserializing data.

Also, for better compatibility with enums, value deduplication is implemented
so that constants of the same class with the same value are always the same instance,
which makes possible comparisons via the `is` operator:

    Color('blue') is Color.blue  # == True


See tests for more specifics.
"""

from __future__ import annotations

from types import MappingProxyType
from typing import (
    Any,
    Callable,
    ClassVar,
    Generator,
    Optional,
    Sequence,
    Type,
    TypeVar,
)

_ANY_TV = TypeVar("_ANY_TV")
_DYNAMIC_ENUM_TV = TypeVar("_DYNAMIC_ENUM_TV", bound="DynamicEnum")


class AutoEnumValue:
    """
    Descriptor that enables to define `DynamicEnum` subclass instances
    within the class itself `Enum`-style.
    """

    __slots__ = ("__name",)

    __name: Optional[str]

    def __init__(self) -> None:
        self.__name = None

    def __set_name__(self, owner: DynamicEnum, name: str) -> None:
        self.__name = name
        owner.declare(name)

    def __get__(self, instance: Optional[DynamicEnum], owner: Type[_DYNAMIC_ENUM_TV]) -> _DYNAMIC_ENUM_TV:
        if self.__name is None:  # has not been set yet
            raise RuntimeError("Property is not bound to any class")

        assert self.__name is not None
        return owner(self.__name)

    def __set__(self, instance: Optional[DynamicEnum], value: Any) -> None:
        raise AttributeError


def _get_value_from_dyn_enum_args(args: Sequence[Any], kwargs: dict[str, Any]) -> str:
    """Auxiliary function used by `DynamicEnum` and its metaclass"""

    if args:
        value = args[0]
    elif "value" in kwargs:
        value = kwargs["value"]
    else:
        raise TypeError("name argument not received")

    return value


class DynamicEnumMetaclass(type):
    """
    This metaclass is used to:
    1. Override instance creation (i.e. avoid creating a new instance
       when one with the same value already exists).
    2. Implement magic methods for the `DynamicEnum` class. They cannot be defined
       as classmethods on the class, they must be defined in the metaclass.
    3. Enforce `__slots__` for all subclasses.
    """

    __instances: ClassVar[dict[DynamicEnumMetaclass, dict[str, Any]]] = {}  # For deduplication

    def __new__(mcs, name: str, bases: Any, attrs: Any) -> DynamicEnumMetaclass:
        if len(bases) > 1:
            raise TypeError("Cannot have multiple bases for subclasses")

        # Enforce slots
        slots = attrs.get("__slots__")
        if not bases:
            # This must be the base class
            assert slots == ("__value",)
        elif slots is not None:
            if slots != ():
                raise TypeError(
                    f"Custom instance attributes are not supported for class {name}. " f"Got slots: {slots}"
                )
        else:
            # A subclass. Enforce slots without any additional instance attributes
            attrs = dict(attrs, __slots__=())

        return super().__new__(mcs, name, bases, attrs)  # type: ignore

    def __call__(cls: Type[_ANY_TV], *args: Any, **kwargs: Any) -> _ANY_TV:
        value = _get_value_from_dyn_enum_args(args, kwargs)

        # Deduplicate instances with the same value
        if cls not in DynamicEnumMetaclass.__instances:
            DynamicEnumMetaclass.__instances[cls] = {}  # type: ignore
        cls_instances = DynamicEnumMetaclass.__instances[cls]  # type: ignore
        if value not in cls_instances:
            cls_instances[value] = super().__call__(*args, **kwargs)  # type: ignore

        return cls_instances[value]  # type: ignore

    def __getitem__(cls: Type[_ANY_TV], item: str) -> _ANY_TV:
        return cls(item)  # type: ignore

    def __contains__(cls: DynamicEnumMetaclass, item: str) -> bool:
        return cls.is_declared(item)  # type: ignore

    def __iter__(cls: Type[_ANY_TV]) -> Generator[_ANY_TV, None, None]:
        yield from cls.iter_items()  # type: ignore

    @property
    def __members__(cls) -> MappingProxyType:  # For compatibility with Enum
        return MappingProxyType({elem.name: elem.value for elem in cls})  # type: ignore


class DynamicEnum(metaclass=DynamicEnumMetaclass):
    __slots__ = ("__value",)
    __value: str

    __declared_values: ClassVar[dict[Type[DynamicEnum], set[str]]] = {}
    __subclassable: ClassVar[bool] = True

    def __init_subclass__(cls) -> None:
        """
        Prevent the subclassing of subclasses.
        """

        if not cls.__subclassable:
            raise TypeError(f"Cannot subclass {cls.__name__}")

        # All subclasses of DynamicEnum are not subclassable
        cls.__subclassable = False

        # This way we avoid situations like:
        # MyString.my_value != MyStringSubclass.my_value

    def __new__(cls: Type[_DYNAMIC_ENUM_TV], *args: Any, **kwargs: Any) -> _DYNAMIC_ENUM_TV:
        """
        Validates creation of instances:
        1. Forbids direct instantiation of `DynamicEnum`.
        2. Forbids instantiation with undeclared values.
        """

        if cls is DynamicEnum:
            raise TypeError("Cannot instantiate DynamicEnum base class")

        value = _get_value_from_dyn_enum_args(args, kwargs)

        if not cls.is_declared(value):
            raise ValueError(f"Cannot instantiate {cls.__name__} with undeclared value {value!r}")

        return super().__new__(cls)

    def __init__(self, value: str):
        if type(value) is not str:
            raise TypeError(f"Invalid value type {type(value)}")

        value_attr_mangled_name = f"_{DynamicEnum.__name__}__value"  # See "name mangling in python"
        super().__setattr__(value_attr_mangled_name, value)  # Because direct usage of __setattr__ is forbidden

    @classmethod
    def is_declared(cls, value: str) -> bool:
        return cls in cls.__declared_values and value in cls.__declared_values[cls]

    @classmethod
    def iter_items(cls: Type[_DYNAMIC_ENUM_TV]) -> Generator[_DYNAMIC_ENUM_TV, None, None]:
        if cls not in cls.__declared_values:
            yield from ()
        for value in cls.__declared_values[cls]:
            yield cls(value)

    @classmethod
    def declare(cls: Type[_DYNAMIC_ENUM_TV], value: str) -> _DYNAMIC_ENUM_TV:
        if cls not in cls.__declared_values:
            cls.__declared_values[cls] = set()

        cls_declared_values = cls.__declared_values[cls]
        if value not in cls_declared_values:
            cls_declared_values.add(value)
        else:
            raise ValueError(f"{cls.__name__} value {value!r} has already been declared")

        return cls(value)

    @property
    def value(self) -> str:
        return self.__value

    @property
    def name(self) -> str:  # Same as value, for compatibility with Enum
        return self.__value

    def __setattr__(self, key: str, value: Any) -> None:
        raise AttributeError(f"{self.__class__.__name__} is immutable")

    def __eq__(self, other: Any) -> bool:
        if type(other) != type(self):
            return False
        return other.__value == self.__value

    def __hash__(self) -> int:
        return hash(self.__value)

    def __str__(self) -> str:
        return repr(self)

    def __repr__(self) -> str:
        return f"{type(self).__name__}({self.__value!r})"

    def __reduce__(self: _DYNAMIC_ENUM_TV) -> tuple[Callable[[], _DYNAMIC_ENUM_TV], tuple[str]]:
        return self.__class__, (self.__value,)
