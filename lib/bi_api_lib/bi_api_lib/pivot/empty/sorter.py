from __future__ import annotations

import attr

from bi_api_lib.pivot.base.sorter import PivotSorter


@attr.s
class EmptyPivotSorter(PivotSorter):
    def sort(self) -> None:
        pass
