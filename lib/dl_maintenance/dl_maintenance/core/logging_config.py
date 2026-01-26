import shortuuid

import dl_logging


def configure_logging_for_shell(app_name: str = "ad_hoc_operation", for_development: bool | None = None) -> None:
    if "request_id" not in dl_logging.get_log_context():
        dl_logging.put_to_context("request_id", shortuuid.uuid())

    dl_logging.configure_logging(
        for_development=for_development,
        app_name=app_name,
        logcfg_processors=(dl_logging.logcfg_process_stream_human_readable,),
    )
