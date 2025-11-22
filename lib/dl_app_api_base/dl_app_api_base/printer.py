import logging
import sys
from typing import (
    Any,
    TextIO,
)


AIOHTTP_LOGGER = logging.getLogger("aiohttp")


class PrintLogger:
    def __call__(
        self,
        *args: Any,
        sep: str = " ",
        end: str = "\n",
        file: TextIO | None = None,
    ) -> None:
        if file == sys.stderr:
            AIOHTTP_LOGGER.error(sep.join([str(arg) for arg in args]))

        if file is None or file == sys.stdout:
            AIOHTTP_LOGGER.info(sep.join([str(arg) for arg in args]))
