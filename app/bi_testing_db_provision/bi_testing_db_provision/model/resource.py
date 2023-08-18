from __future__ import annotations

import abc
from typing import ClassVar, TypeVar, Any, Optional, Generic, Tuple, Dict, Type
from uuid import UUID

import attr

from bi_testing_db_provision.model.base import BaseStoredModel
from bi_testing_db_provision.model.enums import ResourceKind, ResourceState, ResourceType
from bi_testing_db_provision.model.request_base import ResourceRequest

_RESOURCE_TV = TypeVar('_RESOURCE_TV', bound='ResourceBase')
_RESOURCE_REQUEST_TV = TypeVar('_RESOURCE_REQUEST_TV', bound=ResourceRequest)


@attr.s
class ResourceInternalData:
    pass


@attr.s
class ResourceExternalData:
    pass


@attr.s(frozen=True)
class ResourceBase(BaseStoredModel, Generic[_RESOURCE_REQUEST_TV], metaclass=abc.ABCMeta):
    kind: ClassVar[ResourceKind]
    type: ClassVar[ResourceType]

    _map_kind_type_cls: ClassVar[Dict[Tuple[ResourceKind, ResourceType], Type[ResourceBase]]] = {}

    state: ResourceState = attr.ib()
    request: _RESOURCE_REQUEST_TV = attr.ib()
    internal_data: ResourceInternalData = attr.ib()
    external_data: ResourceExternalData = attr.ib()
    session_id: Optional[UUID] = attr.ib()

    def __attrs_post_init__(self):  # type: ignore  # TODO: fix
        # TODO FIX: Implement type validation for request/data
        pass

    @classmethod
    def __init_subclass__(cls, **kwargs):  # type: ignore
        if not cls.__name__.endswith('Base'):
            key = (cls.kind, cls.type)
            cls_map = cls._map_kind_type_cls
            assert key not in cls._map_kind_type_cls, f"Resource with {key} already registered: {cls_map[key]}"
            cls_map[key] = cls
            # TODO FIX: Validate than all types are defined

    @classmethod
    def get_resource_cls(cls, r_kind: ResourceKind, r_type: ResourceType) -> Type[ResourceBase]:
        return cls._map_kind_type_cls[(r_kind, r_type)]

    @classmethod
    def create(
            cls: Type[_RESOURCE_TV],
            resource_id: Optional[UUID],
            state: ResourceState,
            request: ResourceRequest,
            internal_data: Optional[ResourceInternalData] = None,
            external_data: Optional[ResourceExternalData] = None,
            session_id: Optional[UUID] = None,
    ) -> _RESOURCE_TV:
        return cls(
            id=resource_id,
            state=state,
            request=request,
            internal_data=internal_data,  # type: ignore  # TODO: fix
            external_data=external_data,  # type: ignore  # TODO: fix
            session_id=session_id,
        )

    def clone(self: _RESOURCE_TV, **updates: Any) -> _RESOURCE_TV:
        return attr.evolve(self, **updates)
