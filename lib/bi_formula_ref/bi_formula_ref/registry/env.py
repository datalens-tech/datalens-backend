import attr


@attr.s(frozen=True)
class GenerationEnvironment:
    scopes: int = attr.ib(kw_only=True)
