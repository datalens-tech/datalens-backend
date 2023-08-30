from __future__ import annotations

import logging
from typing import Collection, Dict, Optional, Set, Tuple

import attr

from bi_constants.enums import ManagedBy

import bi_core.exc as exc
from bi_core.components.ids import AvatarId, RelationId
from bi_core.components.accessor import DatasetComponentAccessor
from bi_core.components.dependencies.avatar_tree_base import AvatarTreeResolverBase
from bi_core.components.dependencies.relation_avatar_base import RelationAvatarDependencyManagerBase
from bi_core.us_dataset import Dataset


LOGGER = logging.getLogger(__name__)


@attr.s
class AvatarTreeResolver(AvatarTreeResolverBase):
    _dataset: Dataset = attr.ib(kw_only=True)
    _relation_avatar_dep_mgr: RelationAvatarDependencyManagerBase = attr.ib(kw_only=True)
    _ds_accessor: DatasetComponentAccessor = attr.ib(init=False)

    @_ds_accessor.default
    def _make_ds_accessor(self) -> DatasetComponentAccessor:
        return DatasetComponentAccessor(dataset=self._dataset)

    def rank_avatars(self) -> Dict[AvatarId, int]:
        def populate_recursively(avatar_id: str, rank: int) -> None:
            ranks[avatar_id] = rank
            for relation in self._ds_accessor.get_avatar_relation_list(left_avatar_id=avatar_id):
                if relation.managed_by == ManagedBy.feature:
                    continue
                elif relation.managed_by == ManagedBy.user:
                    populate_recursively(avatar_id=relation.right_avatar_id, rank=rank+1)
                else:
                    raise ValueError(f'Unsupported managed_by value in relation: {relation.managed_by}')

        root_avatar = self._ds_accessor.get_root_avatar_strict()
        ranks: Dict[str, int] = {}
        populate_recursively(avatar_id=root_avatar.id, rank=0)
        return ranks

    def expand_required_avatar_ids(
            self, required_avatar_ids: Collection[str]
    ) -> Tuple[Optional[AvatarId], Set[AvatarId], Set[RelationId]]:

        if len(required_avatar_ids) == 1:
            # Single avatar -> nothing to resolve
            return next(iter(required_avatar_ids)), set(required_avatar_ids), set()

        required_avatar_ids = set(required_avatar_ids)
        ranks = self.rank_avatars()
        if len(required_avatar_ids) > 1:
            LOGGER.info(f'Got avatar ranks: {ranks}')

        required_relation_ids: Set[RelationId] = set()

        # first add avatars used by required feature-managed relations
        for iteration in range(10):
            updated_required_avatar_ids = required_avatar_ids.copy()
            for relation in self._ds_accessor.get_avatar_relation_list():
                if relation.right_avatar_id in required_avatar_ids and relation.managed_by == ManagedBy.feature:
                    updated_required_avatar_ids |= self._relation_avatar_dep_mgr.get_relation_avatar_references(
                        relation_id=relation.id)
                    required_relation_ids.add(relation.id)
            if updated_required_avatar_ids == required_avatar_ids:
                LOGGER.info(f'Finished resolving feature-managed avatars on iteration {iteration}')
                break
            else:
                LOGGER.info(
                    'Found additional avatars in required feature-managed relations: '
                    f'{updated_required_avatar_ids - required_avatar_ids}')
            required_avatar_ids = updated_required_avatar_ids
        else:
            # It means that for every iteration we are still getting new avatars
            raise RuntimeError('Failed to resolve required avatars')  # Should not happen

        user_avatar_ids = {
            avatar_id for avatar_id in required_avatar_ids
            if self._ds_accessor.get_avatar_strict(avatar_id=avatar_id).managed_by == ManagedBy.user
        }
        assert user_avatar_ids, 'Must have at least one user-managed source'

        def get_relation_and_parent_id(avatar_id: AvatarId) -> Tuple[RelationId, AvatarId]:
            relations = self._ds_accessor.get_avatar_relation_list(right_avatar_id=avatar_id)
            assert len(relations) == 1
            relation = relations[0]
            return relation.id, relation.left_avatar_id

        try:
            min_rank = min(rank for avatar_id, rank in ranks.items() if avatar_id in user_avatar_ids)
        except ValueError:
            raise exc.UnboundAvatarError(
                'Detected the usage of an unbound avatar. '
                'Dataset is configured incorrectly.'
            )

        # add parent_ids until single common ancestor remains
        common_root_ids = user_avatar_ids
        while len(common_root_ids) > 1:
            new_common_root_ids = set()
            for avatar_id in common_root_ids:
                # now fill in the gaps so that all `new_common_root_ids` are at the same level in the avatar tree
                while ranks[avatar_id] > min_rank:
                    new_relation_id, new_avatar_id = get_relation_and_parent_id(avatar_id)
                    # add this parent to required IDs
                    if new_avatar_id not in required_avatar_ids:
                        LOGGER.info(
                            f'Implicitly adding avatar {new_avatar_id} '
                            f'so that avatar {avatar_id} can be joined to')
                        required_avatar_ids.add(new_avatar_id)
                    avatar_id = new_avatar_id
                    required_relation_ids.add(new_relation_id)

                new_common_root_ids.add(avatar_id)

            common_root_ids = new_common_root_ids
            if len(common_root_ids) > 1:
                min_rank -= 1

        return next(iter(common_root_ids)), required_avatar_ids, required_relation_ids
