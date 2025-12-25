import enum


@enum.unique
class ObfuscationContext(enum.Enum):
    LOGS = "logs"
    SENTRY = "sentry"
    TRACING = "tracing"
    USAGE_TRACKING = "usage_tracking"
    INSPECTOR = "inspector"
