from __future__ import annotations

import os
from typing import (
    Callable,
    Generic,
    Literal,
    NamedTuple,
    Optional,
    TypeVar,
    Union,
    overload,
)


_RET_TV = TypeVar("_RET_TV")
_FACTORY_RET_TV = TypeVar("_FACTORY_RET_TV")


class SecretStr(str):
    def __repr__(self) -> str:
        return "sec_str<***>"

    def __str__(self) -> str:
        return "sec_str<***>"


class Required(NamedTuple):
    message: Optional[str] = None


class Factory(Generic[_FACTORY_RET_TV]):
    def __init__(self, factory: Callable[[], _FACTORY_RET_TV]):
        self._factory = factory

    def build(self) -> _FACTORY_RET_TV:
        return self._factory()


class EnvError(Exception):
    pass


@overload
def get_from_env(env_key: str, converter: Callable[[str], _RET_TV], default: Literal[None]) -> Optional[_RET_TV]:
    pass


@overload  # noqa
def get_from_env(env_key: str, converter: Callable[[str], _RET_TV], default: Required) -> _RET_TV:
    pass


@overload  # noqa
def get_from_env(
    env_key: str, converter: Callable[[str], _RET_TV], default: Union[_RET_TV, Factory[_RET_TV]]
) -> _RET_TV:
    pass


@overload  # noqa
def get_from_env(env_key: str, converter: Callable[[str], _RET_TV]) -> _RET_TV:
    pass


def get_from_env(  # noqa
    env_key: str,
    converter: Callable[[str], _RET_TV],
    default: Union[_RET_TV, Factory[_RET_TV], Required, None] = Required(),  # noqa: B008
) -> Optional[_RET_TV]:
    if env_key in os.environ:
        try:
            return converter(os.environ[env_key])
        except Exception as err:
            raise EnvError(f"Could not convert env var {env_key}") from err

    if isinstance(default, Required):
        err_msg = f"Env missing '{env_key}' but required"
        if default.message is not None:
            err_msg = f"{err_msg}: {default.message}"
        raise EnvError(err_msg, env_key)

    if isinstance(default, Factory):
        return default.build()

    return default
