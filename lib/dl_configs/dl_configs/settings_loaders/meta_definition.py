from __future__ import annotations

from typing import (
    Any,
    Callable,
    ClassVar,
    Optional,
    Type,
    TypeVar,
    cast,
)

import attr

from dl_configs.settings_loaders.common import FallbackFactory


class ReservedKeys:
    JSON_VALUE: ClassVar[str] = "JSON_VALUE"


@attr.s(frozen=True)
class SMeta:
    attrs_meta_key: ClassVar[str] = "s_meta"

    name: Optional[str] = attr.ib()
    fallback_cfg_key: Optional[str] = attr.ib(default=None)
    fallback_factory: Optional[Callable[[Any], Any]] = attr.ib(default=None)
    env_var_converter: Optional[Callable[[str], Any]] = attr.ib(default=None)
    json_converter: Optional[Callable[[Any], Any]] = attr.ib(default=None)
    enabled_key_name: Optional[str] = attr.ib(default=None)
    is_app_cfg_type: bool = attr.ib(default=False)

    def __attrs_post_init__(self):  # type: ignore  # 2024-01-24 # TODO: Function is missing a return type annotation  [no-untyped-def]
        assert not (
            self.fallback_cfg_key is not None and self.fallback_factory is not None
        ), "fallback_factory and fallback_cfg_key can not be defined together"

    def to_dict(self):  # type: ignore  # 2024-01-24 # TODO: Function is missing a return type annotation  [no-untyped-def]
        return {self.attrs_meta_key: self}

    @classmethod
    def from_attrib(cls, attrib: Any) -> Optional[SMeta]:
        return attrib.metadata.get(cls.attrs_meta_key)


def s_attrib(
    key_name: Optional[str],
    type_class: object = None,
    fallback_cfg_key: Optional[str] = None,
    fallback_factory: Optional[FallbackFactory] = None,
    env_var_converter: Optional[Callable[[str], Any]] = None,
    json_converter: Optional[Callable[[Any], Any]] = None,
    missing: Any = attr.NOTHING,
    missing_factory: Any = None,
    sensitive: bool = False,
    enabled_key_name: Optional[str] = None,
    is_app_cfg_type: bool = False,
) -> attr.Attribute:
    if is_app_cfg_type:
        assert fallback_cfg_key is None
        assert fallback_factory is None
        assert missing_factory is None
        assert missing is attr.NOTHING

    s_meta = SMeta(
        name=key_name,
        fallback_cfg_key=fallback_cfg_key,
        fallback_factory=fallback_factory,  # type: ignore  # 2024-01-24 # TODO: Argument "fallback_factory" to "SMeta" has incompatible type "Callable[[Any, Any], Any] | Callable[[Any], Any] | None"; expected "Callable[[Any], Any] | None"  [arg-type]
        env_var_converter=env_var_converter,
        json_converter=json_converter,
        enabled_key_name=enabled_key_name,
        is_app_cfg_type=is_app_cfg_type,
    )

    return attr.ib(
        type=type_class,
        default=missing,
        factory=missing_factory,
        repr=not sensitive,
        kw_only=True,
        metadata=s_meta.to_dict(),
    )


_REQUIRED_TV = TypeVar("_REQUIRED_TV")


@attr.s(frozen=True)
class RequiredValue:
    message: Optional[str] = attr.ib(default=None)


def required(t: Type[_REQUIRED_TV]) -> _REQUIRED_TV:
    """
    Usage: set constructor args of attrs objects in fallback_config_factory.
    Fields with `REQUIRED_VALUE` value must be replaced by settings loader or exception must be thrown
    Typing and cast are only to trick MyPy
    """
    return cast(t, RequiredValue())  # type: ignore  # 2024-01-24 # TODO: Variable "t" is not valid as a type  [valid-type]
