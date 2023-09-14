from __future__ import annotations

import abc
from typing import Optional

from bi_core.components.ids import AvatarId
from bi_core.data_processing.prepared_components.primitives import PreparedSingleFromInfo


class PreparedComponentManagerBase(abc.ABC):
    @abc.abstractmethod
    def get_prepared_source(
        self, avatar_id: AvatarId, alias: str, from_subquery: bool, subquery_limit: Optional[int]
    ) -> PreparedSingleFromInfo:
        raise NotImplementedError
