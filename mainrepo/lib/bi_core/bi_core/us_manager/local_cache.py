from __future__ import annotations

from typing import (
    TYPE_CHECKING,
    Iterable,
    Union,
)

import attr

from bi_core.base_models import (
    ConnectionRef,
    DefaultConnectionRef,
)
import bi_core.exc as exc
from bi_core.us_manager.broken_link import (
    BrokenUSLink,
    BrokenUSLinkErrorKind,
)

if TYPE_CHECKING:
    from bi_core.us_entry import USEntry


@attr.s
class USEntryBuffer:
    _data: dict[ConnectionRef, Union[USEntry, BrokenUSLink]] = attr.ib(factory=dict)

    def get_entry(self, entry_id: ConnectionRef) -> USEntry:
        entry = self._data.get(entry_id)

        if isinstance(entry, BrokenUSLink):
            if isinstance(entry.reference, DefaultConnectionRef):
                if entry.error_kind == BrokenUSLinkErrorKind.NOT_FOUND:
                    raise exc.ReferencedUSEntryNotFound(f"Referenced connection {entry.reference.conn_id} was deleted")
                elif entry.error_kind == BrokenUSLinkErrorKind.ACCESS_DENIED:
                    raise exc.ReferencedUSEntryAccessDenied(
                        f"Referenced connection {entry.reference.conn_id} cannot be loaded: access denied",
                        details=dict(scope="connection", entry_id=entry.reference.conn_id),
                    )
                else:
                    raise ValueError(f"Referenced connection {entry.reference} cannot be loaded: {entry.error_kind}")
            else:
                raise ValueError(f"Requested referenced US entry {entry_id} is broken: {entry}", entry)

        if entry is None:
            raise ValueError(f"Connection {entry_id} is not loaded")

        return entry

    def set_entry(self, entry_id: ConnectionRef, entry: Union[USEntry, BrokenUSLink]) -> None:
        self._data[entry_id] = entry

    def __getitem__(self, entry_id: ConnectionRef) -> USEntry:
        return self.get_entry(entry_id)

    def __setitem__(self, entry_id: ConnectionRef, entry: Union[USEntry, BrokenUSLink]) -> None:
        self.set_entry(entry_id, entry)

    def __contains__(self, entry_id: ConnectionRef) -> bool:
        return entry_id in self._data

    def keys(self) -> Iterable[ConnectionRef]:
        return self._data.keys()

    def clear(self) -> None:
        self._data.clear()
