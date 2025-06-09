from __future__ import annotations


class RetrierError(Exception):
    ...


class RetrierTimeoutError(RetrierError):
    ...


class RetrierUnretryableError(RetrierError):
    ...
