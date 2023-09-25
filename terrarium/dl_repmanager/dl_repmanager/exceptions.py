class PackageMetaCliError(Exception):
    pass


class InconsistentStateError(PackageMetaCliError):
    pass


class MypyStubsOutOfSyncError(InconsistentStateError):
    pass
