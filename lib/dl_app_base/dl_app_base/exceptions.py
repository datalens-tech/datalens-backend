class ApplicationError(Exception):
    ...


class StartupError(ApplicationError):
    ...


class ShutdownError(ApplicationError):
    ...


class RunError(ApplicationError):
    ...


class UnexpectedFinishError(RunError):
    ...
