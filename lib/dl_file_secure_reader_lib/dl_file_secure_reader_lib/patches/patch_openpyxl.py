from typing import Any

from openpyxl.descriptors import NoneSet
from openpyxl.styles.alignment import (
    Alignment,
    horizontal_alignments,
)


HORIZONTAL_ALIGNMENTS_CORRECTION_MAP = {
    "start": "left",
}


class HorizontalAlignmentSelfCorrectionSet(NoneSet):
    def __set__(self, instance: Any, value: Any) -> None:
        if value in HORIZONTAL_ALIGNMENTS_CORRECTION_MAP:
            value = HORIZONTAL_ALIGNMENTS_CORRECTION_MAP[value]
        super(HorizontalAlignmentSelfCorrectionSet, self).__set__(instance, value)


def patch_openpyxl() -> None:
    Alignment.horizontal = HorizontalAlignmentSelfCorrectionSet(values=horizontal_alignments)
