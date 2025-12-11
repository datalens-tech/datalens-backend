import logging
import os
from typing import (
    Any,
    Callable,
    Optional,
    Sequence,
)

import jaeger_client
from opentracing.scope_managers.contextvars import ContextVarsScopeManager
import statcommons.log_config
import statcommons.logs

import dl_logging.context as context
import dl_logging.format as format


def add_log_context(record: logging.LogRecord) -> None:
    context_data = context.get_log_context()
    for key, value in context_data.items():
        setattr(record, key, value)


LOGMUTATORS = statcommons.logs.LOGMUTATORS
HOSTNAME = statcommons.log_config.HOSTNAME
ENV_CONTEXT = statcommons.log_config.ENV_CONTEXT


class FastLogsFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        event_code = getattr(record, "event_code", None)

        if not event_code:
            return False

        if isinstance(event_code, str) and event_code.startswith("_"):
            return False

        return True


_LOG_FORMATTER = format.JsonFormatter()
_JSON_FORMATTER = statcommons.logs.JsonExtFormatter()


class StdoutFormatter(logging.Formatter):
    """
    Write logs in YDeploy format in YDeploy,
    and as top-level JSON otherwise.
    """

    def format(self, record: logging.LogRecord) -> str:
        if format.is_deploy():
            return _LOG_FORMATTER.format(record)
        return _JSON_FORMATTER.format(record)


def _make_logging_config(
    for_development: bool,
    logcfg_processors: Optional[Sequence[Callable]] = None,
) -> dict:
    """
    ...

    `logcfg_processors`: see description in `configure_logging`.

    `for_development`: if True - will apply simple plain text stderr/stdout config without any integrations.
      Should be used only for running tests
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
                "level": "INFO",
            },
        }

    else:
        base = statcommons.log_config.BASE_LOGGING_CONFIG
        # Handlers for root and for every non-propagated logger
        common_handlers = base["root"]["handlers"]
        # ^ ['debug_log', 'fast_log', 'event_log']

        default_handlers = ["stream"] + common_handlers  # everything to stdout
        logging_config = {
            **base,
            "filters": {
                **base.get("filters", {}),
                "fast_logs": {"()": "dl_logging.config.FastLogsFilter"},
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
                # Set minimal level to some unhelpful libraries' logging:
                "jaeger_tracing": {"level": "WARNING", "propagate": False, "handlers": default_handlers},
                "asyncio": {"level": "INFO", "propagate": False, "handlers": default_handlers},
                "kikimr": {"level": "WARNING", "propagate": False, "handlers": default_handlers},
                "botocore": {"level": "INFO", "propagate": False, "handlers": default_handlers},
                "ydb": {"level": "DEBUG", "propagate": False, "handlers": default_handlers},
            },
            "root": {
                **(base.get("root") or {}),
                "handlers": default_handlers,
                "level": "DEBUG",
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
    context_data = context.get_log_context()
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
    for_development: Optional[bool] = None,
    app_prefix: Optional[str] = None,
    logcfg_processors: Optional[Sequence[Callable]] = None,
    use_jaeger_tracer: bool = False,
    jaeger_service_name: Optional[str] = None,
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

    cfg = _make_logging_config(
        for_development=for_development,
        logcfg_processors=logcfg_processors,
    )
    statcommons.log_config.configure_logging(app_name=app_name, cfg=cfg)
    LOGMUTATORS.add_mutator("log_context", add_log_context)
    LOGMUTATORS.add_mutator("update_tags", update_tags)

    if use_jaeger_tracer:
        effective_service_name = app_name if jaeger_service_name is None else jaeger_service_name
        setup_jaeger_client(effective_service_name)
