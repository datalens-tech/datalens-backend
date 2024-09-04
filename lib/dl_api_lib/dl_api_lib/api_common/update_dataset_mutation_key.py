from __future__ import annotations

import hashlib

import attr

from dl_api_lib.request_model.data import FieldAction
from dl_constants.types import TJSONExt
from dl_core.us_manager.mutation_cache.mutation_key_base import MutationKey
from dl_model_tools.serialization import hashable_dumps


class MutationKeySerializationError(ValueError):
    pass


@attr.s(auto_attribs=True, kw_only=True, frozen=True)
class UpdateDatasetMutationKey(MutationKey):
    _dumped: str
    _hash: str

    def get_collision_tier_breaker(self) -> str:
        return self._dumped

    def get_hash(self) -> str:
        return self._hash

    @staticmethod
    def _dumps(value: TJSONExt) -> str:
        return hashable_dumps(value, sort_keys=True, ensure_ascii=True, check_circular=True)

    @classmethod
    def create(cls, dataset_revision_id: str, updates: list[FieldAction]) -> UpdateDatasetMutationKey:
        try:
            serialized = [upd.serialized for upd in updates]
        except Exception as e:
            raise MutationKeySerializationError() from e
        serialized.sort(key=cls._dumps)
        dumped = cls._dumps(dict(ds_rev=dataset_revision_id, mutation=serialized))
        hashed = hashlib.sha256(dumped.encode()).hexdigest()
        return UpdateDatasetMutationKey(dumped=dumped, hash=hashed)
