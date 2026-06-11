import logging


class FastLogsFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        event_code = getattr(record, "event_code", None)

        if not event_code:
            return False

        return not isinstance(event_code, str) or not event_code.startswith("_")
