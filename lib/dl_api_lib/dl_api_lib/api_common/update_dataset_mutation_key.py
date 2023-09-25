from __future__ import annotations

import hashlib
import json
from typing import (
    Any,
    List,
)

import attr

from dl_api_lib.request_model.data import FieldAction
from dl_core.serialization import RedisDatalensDataJSONEncoder
from dl_core.us_manager.mutation_cache.mutation_key_base import MutationKey


class MutationKeySerializationError(ValueError):
    pass


@attr.s(auto_attribs=True, kw_only=True, frozen=True)
class UpdateDatasetMutationKey(MutationKey):
    _dumped: str
    _hash: str

    def get_collision_tier_breaker(self) -> Any:
        return self._dumped

    def get_hash(self) -> str:
        return self._hash

    @classmethod
    def create(cls, dataset_revision_id: str, updates: List[FieldAction]) -> UpdateDatasetMutationKey:
        try:
            serialized = [upd.serialized for upd in updates]
        except Exception:
            raise MutationKeySerializationError()
        serialized.sort(key=lambda x: json.dumps(x, indent=None, sort_keys=True, cls=RedisDatalensDataJSONEncoder))
        dumped = json.dumps(
            dict(ds_rev=dataset_revision_id, mutation=serialized),
            sort_keys=True,
            indent=None,
            separators=(",", ":"),
            cls=RedisDatalensDataJSONEncoder,
        )
        hashed = hashlib.sha256(dumped.encode()).hexdigest()
        return UpdateDatasetMutationKey(dumped=dumped, hash=hashed)
