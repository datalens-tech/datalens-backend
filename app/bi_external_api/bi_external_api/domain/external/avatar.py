from typing import Sequence

import attr

from bi_external_api.attrs_model_mapper import ModelDescriptor
from bi_external_api.domain.utils import ensure_tuple


@ModelDescriptor()
@attr.s
class AvatarDef:
    id: str = attr.ib()
    source_id: str = attr.ib()
    title: str = attr.ib()


@ModelDescriptor()
@attr.s
class AvatarJoinCondition:
    pass


@ModelDescriptor()
@attr.s
class AvatarsConfig:
    definitions: Sequence[AvatarDef] = attr.ib(converter=ensure_tuple)
    root: str = attr.ib()
    joins: Sequence[AvatarJoinCondition] = attr.ib(converter=ensure_tuple)

    def get_root_avatar_def(self) -> AvatarDef:
        return next(avatar for avatar in self.definitions if avatar.id == self.root)
