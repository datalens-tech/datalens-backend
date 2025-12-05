import logging


class FastLogsFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        event_code = getattr(record, "event_code", None)

        if not event_code:
            return False

        if isinstance(event_code, str) and event_code.startswith("_"):
            return False

        return True
