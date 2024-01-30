from __future__ import annotations

import attr

from dl_pivot.base.sorter import PivotSorter


@attr.s
class EmptyPivotSorter(PivotSorter):
    def sort(self) -> None:
        pass
