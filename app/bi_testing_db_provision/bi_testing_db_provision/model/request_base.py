import json
from typing import Any, TypeVar

import attr
import xxhash

_RESOURCE_RQ_TV = TypeVar('_RESOURCE_RQ_TV', bound='ResourceRequest')


@attr.s(frozen=True)
class ResourceRequest:
    def clone(self: _RESOURCE_RQ_TV, **updates: Any) -> _RESOURCE_RQ_TV:
        return attr.evolve(self, **updates)

    def get_hash(self) -> bytes:
        return xxhash.xxh64_digest(
            # TODO FIX: Find more adequate way
            json.dumps(attr.asdict(self))
        )
