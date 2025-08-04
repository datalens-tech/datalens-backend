"""
DTOs representing connection targets for SA adapters.
Should be constructed by connection executors based on static connection data stored in connections storage
and possibly augmented with data calculated in runtime or fetched from cache e.g. DB server version
"""

from __future__ import annotations

import abc
from typing import (
    TYPE_CHECKING,
    Any,
    ClassVar,
    Optional,
)

import attr
from typing_extensions import (
    Self,
    final,
)

from dl_core.utils import secrepr


if TYPE_CHECKING:
    from dl_constants.types import TJSONLike


@attr.s(frozen=True)
class ConnTargetDTO(metaclass=abc.ABCMeta):
    conn_id: Optional[str] = attr.ib()

    pass_db_messages_to_user: bool = attr.ib(default=False, kw_only=True)
    pass_db_query_to_user: bool = attr.ib(default=False, kw_only=True)

    @abc.abstractmethod
    def get_effective_host(self) -> Optional[str]:
        pass

    _MAP_CLASS_NAME_CLASS: ClassVar[dict[str, type[ConnTargetDTO]]] = {}

    def __init_subclass__(cls, **kwargs: Any) -> None:
        cls._MAP_CLASS_NAME_CLASS[cls.__qualname__] = cls

    @classmethod
    @final
    def from_polymorphic_jsonable_dict(cls, data: dict) -> ConnTargetDTO:
        data = {**data}
        cls_name = data.pop("cls_name")
        target_cls = cls._MAP_CLASS_NAME_CLASS[cls_name]
        return target_cls.from_jsonable_dict(data)

    @classmethod
    @final
    def from_jsonable_dict(cls, data: dict) -> Self:
        if "cls_name" in data:
            raise ValueError("Property 'cls_name' in data dict. Use from_polymorphic_jsonable_dict() to unmarshal it.")
        return cls._from_jsonable_dict(data)

    @classmethod
    def _from_jsonable_dict(cls, data: dict) -> Self:
        return cls(**data)

    def to_jsonable_dict(self) -> dict[str, TJSONLike]:
        return dict(
            **attr.asdict(self),
            cls_name=type(self).__qualname__,
        )

    def clone(self, **kwargs: Any) -> Self:
        return attr.evolve(self, **kwargs)


@attr.s(frozen=True)
class BaseSQLConnTargetDTO(ConnTargetDTO):
    host: str = attr.ib()
    port: int = attr.ib()
    username: str = attr.ib()
    password: str = attr.ib(repr=secrepr)
    db_name: Optional[str] = attr.ib()

    def get_effective_host(self) -> Optional[str]:
        return self.host


@attr.s(frozen=True)
class BaseAiohttpConnTargetDTO(ConnTargetDTO, metaclass=abc.ABCMeta):
    ca_data: str = attr.ib(repr=secrepr)
