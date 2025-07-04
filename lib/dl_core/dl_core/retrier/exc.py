class RetrierError(Exception):
    ...


class RetrierTimeoutError(RetrierError):
    ...


class RetrierUnretryableError(RetrierError):
    ...
