from __future__ import annotations

import logging

log = logging.getLogger()


def setup_basic_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format='[%(asctime)s] {%(filename)s:%(lineno)d} %(levelname)s - %(message)s',
        datefmt='%H:%M:%S'
    )
    log.debug("Logger initiated")
