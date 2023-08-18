from __future__ import annotations

import shortuuid

from bi_app_tools import ylog
from bi_core.logging_config import configure_logging, logcfg_process_stream_human_readable


def configure_logging_for_shell(app_name: str = 'ad_hoc_operation', env: str = None):  # type: ignore  # TODO: fix
    if 'request_id' not in ylog.context.get_log_context():
        ylog.context.put_to_context('request_id', shortuuid.uuid())

    configure_logging(
        env=env,
        app_name=app_name,
        logcfg_processors=(
            logcfg_process_stream_human_readable,
        )
    )
