from __future__ import annotations

import abc
import enum
import inspect
from typing import (
    Any,
    ClassVar,
    List,
    Optional,
    Sequence,
    Type,
    TypeVar,
    Union,
    final,
)

import attr
from dynamic_enum import DynamicEnum

from bi_external_api.attrs_model_mapper.utils import MText
from bi_external_api.enums import ExtAPIType


_CLS_T = TypeVar("_CLS_T", bound=type)
_DESCRIPTOR_T = TypeVar("_DESCRIPTOR_T", bound="BaseClassDescriptor")


@attr.s(kw_only=True)
class BaseClassDescriptor(metaclass=abc.ABCMeta):
    _REGISTRY: ClassVar[dict[Type, BaseClassDescriptor]]

    _registered: bool = attr.ib(init=False, default=False)
    _target_cls: Optional[Type] = attr.ib(init=False, default=None)

    def __init_subclass__(cls, **kwargs: Any) -> None:
        # Override registry for each subclass
        cls._REGISTRY = {}

    @classmethod
    def has(cls, the_type: Type) -> bool:
        return the_type in cls._REGISTRY

    @classmethod
    def get_for_type(cls: Type[_DESCRIPTOR_T], the_type: Type) -> _DESCRIPTOR_T:
        if not cls.has(the_type):
            raise AssertionError(f"Class {the_type!r} has no a {cls.__name__}")
        ret = cls._REGISTRY[the_type]
        assert isinstance(ret, cls)
        return ret

    @classmethod
    def _register(cls, descriptor: _DESCRIPTOR_T) -> None:
        the_type = descriptor._target_cls

        assert not descriptor._registered, f"Attempt to reuse {descriptor}"
        assert the_type is not None
        assert the_type not in cls._REGISTRY, f"Class {the_type!r} already associated with {cls.__name__}"

        cls._REGISTRY[the_type] = descriptor

    @abc.abstractmethod
    def pre_registration_hook(self, cls: Type) -> None:
        raise NotImplementedError()

    @final
    def __call__(self, cls: _CLS_T) -> _CLS_T:
        assert not self._registered, "Attempt to reuse model descriptor"
        assert attr.has(cls)
        self._target_cls = cls
        self.pre_registration_hook(cls)
        self._register(self)
        return cls


@attr.s(kw_only=True)
class ModelDescriptor(BaseClassDescriptor):
    # TODO FIX: Add on_setattr=attr.setters.frozen after MyPy upgrade in Arcadia
    #  At this moment:
    #  bi_external_api/attrs_model_mapper/base.py:16: error: Module has no attribute "setters"
    """
    api_types: optional list of ExtAPIType where model should be used, if no tag is provided model is used
        for all environments
    api_types_exclude: optional list of ExtAPIType where model should NOT be used, if no tag is provided model is used
        for all environments
    currently we need specify env_tags, api_types_exclude explicitly,
     to avoid registering two sub-models with same kind

    """
    is_abstract: bool = attr.ib(default=False)
    type_discriminator: Optional[str] = attr.ib(default=None)
    children_type_discriminator_attr_name: Optional[str] = attr.ib(default=None)
    children_type_discriminator_aliases_attr_name: Optional[str] = attr.ib(default=None)
    api_types: List[ExtAPIType] = attr.ib(factory=list)
    api_types_exclude: List[ExtAPIType] = attr.ib(factory=list)
    description: Optional[MText] = attr.ib(default=None)

    # Next fields will be filled during
    _effective_type_discriminator: Optional[str] = attr.ib(init=False, default=None)
    _effective_type_discriminator_aliases: Optional[tuple[str, ...]] = attr.ib(init=False, default=None)

    @property
    def effective_type_discriminator(self) -> str:
        ret = self._effective_type_discriminator
        assert ret is not None, f"No type discriminator defined for {self._target_cls!r}"
        return ret

    @property
    def effective_type_discriminator_aliases(self) -> tuple[str, ...]:
        ret = self._effective_type_discriminator_aliases
        assert ret is not None, f"No type discriminator aliases defined for {self._target_cls!r}"
        return ret

    @classmethod
    def get_registered_parents_for(cls, the_type: Type) -> Sequence[Type]:
        return [parent_cls for parent_cls in inspect.getmro(the_type) if parent_cls in cls._REGISTRY]

    @classmethod
    def resolve_type_discriminator_attr_name(cls, model_cls: Type) -> Optional[str]:
        registered_parents_mro = cls.get_registered_parents_for(model_cls)
        parent_model_descriptors_with_children_type_discriminator_attr_name = [
            ModelDescriptor.get_for_type(parent_model_cls)
            for parent_model_cls in registered_parents_mro
            if ModelDescriptor.get_for_type(parent_model_cls).children_type_discriminator_attr_name is not None
        ]

        assert len(parent_model_descriptors_with_children_type_discriminator_attr_name) < 2, (
            f"type_discriminator_attr_name for {cls} is set in more than one parent:"
            f" {parent_model_descriptors_with_children_type_discriminator_attr_name}"
        )

        return next(
            (
                md.children_type_discriminator_attr_name
                for md in parent_model_descriptors_with_children_type_discriminator_attr_name
            ),
            None,
        )

    @classmethod
    def resolve_type_discriminator_aliases_attr_name(cls, model_cls: Type) -> Optional[str]:
        registered_parents_mro = cls.get_registered_parents_for(model_cls)
        parent_model_descriptors_with_children_type_discriminator_attr_name = [
            ModelDescriptor.get_for_type(parent_model_cls)
            for parent_model_cls in registered_parents_mro
            if ModelDescriptor.get_for_type(parent_model_cls).children_type_discriminator_aliases_attr_name is not None
        ]

        assert len(parent_model_descriptors_with_children_type_discriminator_attr_name) < 2, (
            f"type_discriminator_attr_name for {cls} is set in more than one parent:"
            f" {parent_model_descriptors_with_children_type_discriminator_attr_name}"
        )

        return next(
            (
                md.children_type_discriminator_aliases_attr_name
                for md in parent_model_descriptors_with_children_type_discriminator_attr_name
            ),
            None,
        )

    def pre_registration_hook(self, cls: Type) -> None:
        if self.is_abstract:
            pass

        else:
            self.set_effective_type_discriminator(cls)
            self.set_effective_type_discriminator_aliases(cls)

    def set_effective_type_discriminator(self, cls: Type) -> None:
        assert self.children_type_discriminator_attr_name is None
        children_type_discriminator_attr_name = self.resolve_type_discriminator_attr_name(cls)
        if children_type_discriminator_attr_name is not None:
            assert self.type_discriminator is None, (
                f"Type discriminators should not be set for {cls!r} manually"
                f" due to type_discriminator_attr_name set {children_type_discriminator_attr_name!r} in one of parents"
            )

            type_discriminator_candidate = getattr(cls, children_type_discriminator_attr_name)

            if isinstance(type_discriminator_candidate, enum.Enum):
                self._effective_type_discriminator = type_discriminator_candidate.name
            elif isinstance(type_discriminator_candidate, DynamicEnum):
                self._effective_type_discriminator = type_discriminator_candidate.name
            elif isinstance(type_discriminator_candidate, str):
                self._effective_type_discriminator = type_discriminator_candidate
            else:
                raise AssertionError(f"Unknown type of type discriminator for {cls}: {type_discriminator_candidate}")

    def set_effective_type_discriminator_aliases(self, cls: Type) -> None:
        ret: List[str] = list()
        children_type_discriminator_aliases_attr_name = self.resolve_type_discriminator_aliases_attr_name(cls)
        if children_type_discriminator_aliases_attr_name is not None:
            type_discriminator_alias_candidate = (
                getattr(
                    cls,
                    children_type_discriminator_aliases_attr_name,
                    None,
                )
                or tuple()
            )

            for alias in type_discriminator_alias_candidate:
                if isinstance(alias, str):
                    ret.append(alias)
                elif isinstance(alias, enum.Enum):
                    ret.append(alias.name)
                else:
                    raise AssertionError(f"Unknown type of type discriminator alias for {cls}: {alias}")

        self._effective_type_discriminator_aliases = tuple(ret)


@attr.s(kw_only=True, frozen=True)
class AttribDescriptor:
    METADATA_KEY = "ATTRS_MODEL_MAPPER_META_KEY"

    enum_by_value: bool = attr.ib(default=False)
    serialization_key: Optional[str] = attr.ib(default=None)
    tags: frozenset[enum.Enum] = attr.ib(default=frozenset())
    load_only: bool = attr.ib(default=False)
    skip_none_on_dump: bool = attr.ib(default=False)
    _description: Union[str, MText, None] = attr.ib(default=None)

    def to_meta(self) -> dict:
        return {self.METADATA_KEY: self}

    @classmethod
    def from_attrib(cls, attr_ib: attr.Attribute) -> Optional["AttribDescriptor"]:
        meta = attr_ib.metadata
        if cls.METADATA_KEY in meta:
            may_be_attrib_descriptor = meta[cls.METADATA_KEY]
            assert isinstance(may_be_attrib_descriptor, AttribDescriptor)
            return may_be_attrib_descriptor

        return None

    @property
    def description(self) -> Optional[MText]:
        d = self._description

        if isinstance(d, str):
            return MText(ru=d, en=None)
        return d


# TODO FIX: Consider to do not use subclassing in favor of some flags in model descriptor
class MapperBaseModel(metaclass=abc.ABCMeta):
    @classmethod
    def pre_load(cls, data: dict[str, Any]) -> Optional[dict[str, Any]]:
        return None

    @classmethod
    def post_dump(cls, data: dict[str, Any]) -> Optional[dict[str, Any]]:
        return None
