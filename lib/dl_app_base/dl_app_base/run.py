import asyncio
import logging
import os
import sys
from typing import (
    Any,
    Coroutine,
)

import dl_app_base.exceptions


def run_async_app(
    run_coroutine: Coroutine[Any, Any, Any],
    logger: logging.Logger,
) -> None:
    try:
        asyncio.run(run_coroutine)
    except SystemExit:
        logger.info("Exited with system exit")
        sys.exit(os.EX_OK)
    except dl_app_base.exceptions.ApplicationError:
        logger.exception("Application error occurred, exiting application")
        sys.exit(os.EX_SOFTWARE)
    except KeyboardInterrupt:
        logger.info("Exited with keyboard interruption")
        sys.exit(os.EX_OK)
    except BaseException:
        logger.exception("FATAL error occurred, exiting application")
        sys.exit(os.EX_SOFTWARE)
    finally:
        logger.info("Flushing log handlers")
        # flush all handlers to ensure all messages are written to the log file before exiting
        for handler in logger.handlers:
            handler.flush()
