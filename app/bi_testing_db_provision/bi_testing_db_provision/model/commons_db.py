import attr


@attr.s(frozen=True)
class DBCreds:
    username: str = attr.ib()
    password: str = attr.ib(repr=False)
