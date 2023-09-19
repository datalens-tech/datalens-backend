from __future__ import annotations

import enum
from typing import TYPE_CHECKING

import attr


if TYPE_CHECKING:
    from dl_core.base_models import ConnectionRef


class BrokenUSLinkErrorKind(enum.Enum):
    NOT_FOUND = enum.auto()
    OTHER = enum.auto()
    ACCESS_DENIED = enum.auto()


@attr.s
class BrokenUSLink:
    error_kind: BrokenUSLinkErrorKind = attr.ib()
    reference: ConnectionRef = attr.ib()
    _referrer_id_set: set[str] = attr.ib(init=False, factory=set)

    def __attrs_post_init__(self):  # type: ignore  # TODO: fix
        self._referrer_id_set = set(self._referrer_id_set)

    @property
    def referrer_id_set(self) -> frozenset[str]:
        return frozenset(self._referrer_id_set)

    def add_referrer_id(self, referrer_id: str) -> None:
        self._referrer_id_set.add(referrer_id)
