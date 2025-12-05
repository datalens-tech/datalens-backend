from typing import Literal

import pydantic

import dl_settings


LogLevel = Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]


class LoggerSettings(dl_settings.BaseSettings):
    LEVEL: LogLevel


class LoggingSettings(dl_settings.BaseSettings):
    APP_NAME: str
    IS_DEVELOPMENT: bool = False
    LEVEL: LogLevel | None = None

    LOGGERS: dict[str, LoggerSettings] = pydantic.Field(default_factory=dict)

    @property
    def logger_levels(self) -> dict[str, LogLevel]:
        return {logger_name: logger_settings.LEVEL for logger_name, logger_settings in self.LOGGERS.items()}


class Settings(dl_settings.BaseRootSettings):
    LOGGING: LoggingSettings = NotImplemented
