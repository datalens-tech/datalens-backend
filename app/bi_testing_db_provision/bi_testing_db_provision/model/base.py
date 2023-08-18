from typing import Optional, TypeVar, Any
from uuid import UUID

import attr

_MODEL_TV = TypeVar('_MODEL_TV', bound='ResourceBase')  # type: ignore  # TODO: fix


@attr.s
class BaseStoredModel:
    id: Optional[UUID] = attr.ib()

    def clone(self: _MODEL_TV, **updated: Any) -> _MODEL_TV:
        return attr.evolve(self, **updated)
