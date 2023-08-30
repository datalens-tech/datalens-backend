from __future__ import annotations

import abc
from typing import Collection, Dict, Optional, Set, Tuple

from bi_core.components.ids import AvatarId, RelationId


class AvatarTreeResolverBase(abc.ABC):

    @abc.abstractmethod
    def rank_avatars(self) -> Dict[AvatarId, int]:
        """
        Return mapping: ``{avatar_id: avatar_rank}``,
        where:
        - the root avatar has rank 0,
        - avatars joined directly to the root have rank 1,
        - and so on.

        Feature-managed avatars are excluded because their formal rank is always 1,
        but in reality their relation expressions may require non-root avatars.
        """
        raise NotImplementedError

    @abc.abstractmethod
    def expand_required_avatar_ids(
            self, required_avatar_ids: Collection[str]
    ) -> Tuple[Optional[AvatarId], Set[AvatarId], Set[RelationId]]:
        """
        Complement required avatars wit ones that are required to apply JOINs of the given avatars

        :param required_avatar_ids: avatars that must be used in the query
        :param relation_expressions: compiled relation expressions that contain info
            about avatars required by each JOIN
        :return: root avatar ID, the complemented set of avatar IDs, required relation IDs

        Illustration:
        ::

            Avatar tree:

             A0 -->  A1  -->  A2  --> [A4]
               |         --> [A3]
               |         -->  A5
               |
                --> [F1]  # feature-managed avatar (joined to via an expression that uses a field from A5)

            Parameters:
                required_avatar_ids: {A3, A4, F1}  (emphasized with brackets in the diagram)
                relation_expressions (req avatars by each relation):
                    {
                        # user-managed:
                        A0-A1: {A0, A1},
                        A1-A2: {A1, A2},
                        A1-A3: {A1, A3},
                        A1-A5: {A1, A5},
                        A2-A4: {A2, A4},
                        # feature-managed:
                        A0-F1: {A5, F1},  # via result_field, so it might not really need A0
                    }

        1. First we inspect feature-managed relations that are needed by required avatars (``A0-F1`` in this case)
        and add avatars required by their expressions to ``required_avatar_ids``.

        After this ``required_avatar_ids`` becomes ``{A3, A4, F1, A5}``;
        ``required_relation_ids``: ``{A0-F1}``.

        2. Now, we exclude feature managed avatars (``F1``): ``user_avatar_ids = {A3, A4, A5}``,
        traverse the tree in search of their latest common ancestor (LUCA)
        and add all avatars we encounter on the way (``A2``, ``A1``)

        Finally ``required_avatar_ids = {A3, A4, F1, A5, A2, A1}``;
        ``required_relation_ids``: ``{A0-F1, A1-A2, A1-A3, A1-A5, A2-A4}``.

        3. Return LUCA's ID (``A1``), updated ``required_avatar_ids`` and ``required_relation_ids``.
        """
        raise NotImplementedError
