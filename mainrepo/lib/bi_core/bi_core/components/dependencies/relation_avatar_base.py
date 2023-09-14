from __future__ import annotations

import abc
from typing import AbstractSet

from bi_core.components.ids import (
    AvatarId,
    RelationId,
)


class RelationAvatarDependencyManagerBase:
    @abc.abstractmethod
    def get_relation_avatar_references(self, relation_id: RelationId) -> AbstractSet[AvatarId]:
        """
        Return a set of all avatars referenced directly or indirectly by the given relation.
        """
        raise NotImplementedError
