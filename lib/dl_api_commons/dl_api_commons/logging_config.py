from logging import LogRecord

import dl_app_tools.log.context as log_context


def add_log_context(record: LogRecord) -> None:
    context = log_context.get_log_context()
    for key, value in context.items():
        setattr(record, key, value)
