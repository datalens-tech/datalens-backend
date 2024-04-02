import logging

from dl_constants.enums import ManagedBy
from dl_core.components.accessor import DatasetComponentAccessor
from dl_core.components.ids import AvatarId
from dl_core.us_dataset import Dataset
import dl_query_processing.exc


LOGGER = logging.getLogger(__name__)


def normalize_explicit_avatar_ids(dataset: Dataset, required_avatar_ids: set[AvatarId]) -> set[AvatarId]:
    user_managed_required_avatar_ids = set()
    accessor = DatasetComponentAccessor(dataset=dataset)

    for avatar_id in required_avatar_ids:
        avatar = accessor.get_avatar_strict(avatar_id=avatar_id)
        if avatar.managed_by == ManagedBy.user:
            user_managed_required_avatar_ids.add(avatar_id)

    if not user_managed_required_avatar_ids:
        # In this case only one non-user-managed avatar can be handled
        if len(required_avatar_ids) > 1:
            raise dl_query_processing.exc.InvalidQueryStructure("Cannot build query with only feature-managed avatars")

        elif not required_avatar_ids:
            # Attempt to fall back to root avatar
            root_avatar = accessor.get_root_avatar_opt()
            if root_avatar is not None:
                LOGGER.warning(
                    f"Requested to build source without avatars. "
                    f"Should only happen when selecting constants. "
                    f"Falling back to root avatar {root_avatar.id}"
                )
                required_avatar_ids = required_avatar_ids | {root_avatar.id}

    return required_avatar_ids
