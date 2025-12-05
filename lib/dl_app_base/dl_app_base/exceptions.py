class ApplicationError(Exception):
    ...


class ConfigurationError(ApplicationError):
    ...


class StartupError(ApplicationError):
    ...


class ShutdownError(ApplicationError):
    ...


class RunError(ApplicationError):
    ...


class UnexpectedFinishError(RunError):
    ...
