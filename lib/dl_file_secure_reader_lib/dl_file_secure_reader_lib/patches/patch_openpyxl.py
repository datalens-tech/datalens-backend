import logging
from typing import Any

from openpyxl.descriptors import NoneSet
from openpyxl.styles.alignment import (
    Alignment,
    horizontal_alignments,
)


LOGGER = logging.getLogger(__name__)


HORIZONTAL_ALIGNMENTS_CORRECTION_MAP = {
    "start": "left",
}


class HorizontalAlignmentSelfCorrectionSet(NoneSet):
    def __set__(self, instance: Any, value: Any) -> None:
        LOGGER.debug("Setting horizontal alignment value to %s", value)
        if value in HORIZONTAL_ALIGNMENTS_CORRECTION_MAP:
            LOGGER.debug(
                "Patching horizontal alignment value %s to %s", value, HORIZONTAL_ALIGNMENTS_CORRECTION_MAP[value]
            )
            value = HORIZONTAL_ALIGNMENTS_CORRECTION_MAP[value]
        super(HorizontalAlignmentSelfCorrectionSet, self).__set__(instance, value)


def patch_openpyxl() -> None:
    LOGGER.debug("Patching openpyxl library")
    Alignment.horizontal = HorizontalAlignmentSelfCorrectionSet(values=horizontal_alignments)
