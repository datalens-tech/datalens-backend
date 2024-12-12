import attr


@attr.s(frozen=True)
class BaseError(Exception):
    message: str = attr.ib()


__all__ = ["BaseError"]
