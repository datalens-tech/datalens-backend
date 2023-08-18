from __future__ import annotations

import abc
from typing import AbstractSet

from bi_core.components.ids import AvatarId, FieldId


class FieldAvatarDependencyManagerBase:

    @abc.abstractmethod
    def get_field_avatar_references(self, dep_field_id: FieldId) -> AbstractSet[AvatarId]:
        """
        Return a set of all avatars referenced directly or indirectly by the given field.
        """
        raise NotImplementedError
