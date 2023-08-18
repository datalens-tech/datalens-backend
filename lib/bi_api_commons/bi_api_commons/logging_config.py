from logging import LogRecord

import bi_app_tools.ylog.context as ylog_context


def add_ylog_context(record: LogRecord) -> None:
    context = ylog_context.get_log_context()
    for key, value in context.items():
        setattr(record, key, value)
