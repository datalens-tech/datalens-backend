import logging
import os
from typing import (
    Any,
    Callable,
    Mapping,
    Sequence,
)

import jaeger_client
from opentracing.scope_managers.contextvars import ContextVarsScopeManager
import statcommons.log_config
import statcommons.logs

import dl_logging.context
import dl_logging.settings


LOGGER = logging.getLogger(__name__)


_CUSTOM_LOGGER_LEVELS = {
    "jaeger_tracing": "WARNING",
    "asyncio": "INFO",
    "kikimr": "WARNING",
    "botocore": "INFO",
    "ydb": "WARNING",
    "httpcore": "INFO",
    "httpx": "WARNING",
}


def add_log_context(record: logging.LogRecord) -> None:
    context_data = dl_logging.context.get_log_context()
    for key, value in context_data.items():
        setattr(record, key, value)


LOGMUTATORS = statcommons.logs.LOGMUTATORS


def _make_logging_config(
    for_development: bool,
    logcfg_processors: Sequence[Callable] | None = None,
    log_level: str | None = None,
    custom_logger_levels: Mapping[str, str] | None = None,
) -> dict:
    """
    ...

    `logcfg_processors`: see description in `configure_logging`.

    `for_development`: if True - will apply simple plain text stderr/stdout config without any integrations.
      Should be used only for running tests

    `custom_logger_levels`: used to override default logger levels for custom loggers.
    """
    if for_development:
        base = statcommons.log_config.DEV_LOGGING_CONFIG
        common_handlers = []
        logging_config = {
            **base,
            "loggers": {
                **base.get("loggers", {}),
                "dl_core": {
                    "handlers": ["stream"],
                    "level": "INFO",
                    "propagate": False,
                },
            },
            "root": {
                **base.get("root", {}),
                "level": log_level or "INFO",
            },
        }

    else:
        base = statcommons.log_config.BASE_LOGGING_CONFIG
        # Handlers for root and for every non-propagated logger
        common_handlers = base["root"]["handlers"]
        # ^ ['debug_log', 'fast_log', 'event_log']

        default_handlers = ["stream"] + common_handlers  # everything to stdout

        logger_levels = _CUSTOM_LOGGER_LEVELS.copy()
        if custom_logger_levels:
            logger_levels.update(custom_logger_levels)

        logging_config = {
            **base,
            "filters": {
                **base.get("filters", {}),
                "fast_logs": {"()": "dl_logging.FastLogsFilter"},
            },
            "formatters": {
                **base.get("formatters", {}),
                "json": {"()": "dl_logging.StdoutFormatter"},
            },
            "handlers": {
                **(base.get("handlers") or {}),
            },
            "loggers": {
                **(base.get("loggers") or {}),
                **{
                    logger_name: {
                        "handlers": default_handlers,
                        "level": level,
                        "propagate": False,
                    }
                    for logger_name, level in logger_levels.items()
                },
            },
            "root": {
                **(base.get("root") or {}),
                "handlers": default_handlers,
                "level": log_level or "DEBUG",
            },
        }

    for logcfg_process in logcfg_processors or ():
        logging_config = logcfg_process(
            logging_config,
            common_handlers=common_handlers,
        )

    return logging_config


# TODO FIX: Remove after all tests will be refactored to use unscoped log context
def add_log_context_scoped(record: logging.LogRecord) -> None:
    context_data = dl_logging.context.get_log_context()
    record.log_context = context_data


def update_tags(record: logging.LogRecord) -> None:
    current = getattr(record, "tags", None)
    if current is None:
        current = {}
        record.tags = current
    # NOTE: this will add a copy of request_id in the other logs (stdout / files);
    # TODO: transition to sentry_sdk, make a better common solution for pulling
    # data from record into sentry tags.
    current["request_id"] = getattr(record, "request_id", "unkn")


def logcfg_process_enable_handler(logger_name: str, handler_name: str = "stream_info") -> Callable:
    """
    Make a `logcfg_process` mapper
    that enables a specified handler for the specified logger
    (in addition to the common handlers).

    Useful for enabling INFO or DEBUG logs.
    """

    def logcfg_process(cfg: dict, common_handlers: list[str], **kwargs: Any) -> dict:
        if handler_name in cfg["handlers"]:
            cfg["loggers"][logger_name] = dict(
                handlers=[handler_name] + common_handlers,
                propagate=False,
                level="DEBUG",
            )
        return cfg

    return logcfg_process


def logcfg_process_stream_human_readable(cfg: dict, common_handlers: list[str], **kwargs: Any) -> dict:
    """
    Set human-readable logs to stdout/stderr.
    For maintenance procedures/migrations that are launched manually from shell.
    """
    if "verbose" in cfg.get("formatters", {}):
        for handler_name, handler in cfg.get("handlers", {}).items():
            if handler_name.startswith("stream_") or handler_name == "stream":
                handler.update(
                    formatter="verbose",
                    level="INFO",
                )

    return cfg


def setup_jaeger_client(service_name: str) -> None:
    config = jaeger_client.Config(
        config={  # usually read from some yaml config
            "sampler": {
                "type": "const",
                "param": 1,
            },
            "logging": True,
        },
        service_name=service_name,
        scope_manager=ContextVarsScopeManager(),
        validate=True,
    )
    config.initialize_tracer()


def configure_logging(
    app_name: str,
    for_development: bool | None = None,
    app_prefix: str | None = None,
    logcfg_processors: Sequence[Callable] | None = None,
    use_jaeger_tracer: bool = False,
    jaeger_service_name: str | None = None,
    log_level: dl_logging.settings.LogLevel | None = None,
    custom_logger_levels: Mapping[str, str] | None = None,
) -> None:
    """
    Make sure the global logging state is configured.

    Mostly idempotent but does some checks to ensure the configuration has not changed.

    `app_prefix` is not currently used; see `request_id_app_prefix` instead.

    `logcfg_processors`: convenient (but dangerous) processing of the logging config.
    Iterable of Callables `(log_cfg, **context) -> log_cfg`.
    Context includes `common_handlers`.
    Example: see `logcfg_process_enable_handler`.
    """
    if for_development is None:
        for_development = os.environ.get("DEV_LOGGING", "0").lower() in ("1", "true")

    logging_config = _make_logging_config(
        for_development=for_development,
        logcfg_processors=logcfg_processors,
        log_level=log_level,
        custom_logger_levels=custom_logger_levels,
    )
    statcommons.log_config.configure_logging(app_name=app_name, cfg=logging_config)
    LOGMUTATORS.add_mutator("log_context", add_log_context)
    LOGMUTATORS.add_mutator("update_tags", update_tags)

    if use_jaeger_tracer:
        effective_service_name = app_name if jaeger_service_name is None else jaeger_service_name
        setup_jaeger_client(effective_service_name)

    LOGGER.info("Logging configured with config: %s", logging_config)


def configure_logging_from_settings() -> None:
    settings = dl_logging.settings.Settings()
    logging_settings = settings.LOGGING
    configure_logging(
        app_name=logging_settings.APP_NAME,
        for_development=logging_settings.IS_DEVELOPMENT,
        log_level=logging_settings.LEVEL,
        custom_logger_levels=logging_settings.logger_levels,
    )

    LOGGER.info("Logging configured with settings: %s", logging_settings)
