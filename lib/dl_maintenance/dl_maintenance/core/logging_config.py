from __future__ import annotations

from typing import Optional

import shortuuid

from dl_app_tools import log
from dl_core.logging_config import (
    configure_logging,
    logcfg_process_stream_human_readable,
)


def configure_logging_for_shell(app_name: str = "ad_hoc_operation", for_development: Optional[bool] = None) -> None:
    if "request_id" not in log.context.get_log_context():
        log.context.put_to_context("request_id", shortuuid.uuid())

    configure_logging(
        for_development=for_development, app_name=app_name, logcfg_processors=(logcfg_process_stream_human_readable,)
    )
