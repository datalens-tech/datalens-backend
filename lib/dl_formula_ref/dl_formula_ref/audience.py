import attr


@attr.s(frozen=True)
class Audience:
    name: str = attr.ib(kw_only=True, default="")
    default: bool = attr.ib(kw_only=True, default=False)


DEFAULT_AUDIENCE = Audience(default=True)
