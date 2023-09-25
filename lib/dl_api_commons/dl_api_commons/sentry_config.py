import logging
from typing import (
    Any,
    Optional,
    Sequence,
)

import attr
import flask

from dl_api_commons.logging_sentry import cleanup_common_secret_data


@attr.s(frozen=True)
class SentryConfig:
    dsn: str = attr.ib(repr=False)
    release: Optional[str] = attr.ib(default=None)


def configure_sentry(cfg: SentryConfig, extra_integrations: Sequence[Any] = ()) -> None:
    import sentry_sdk
    from sentry_sdk.integrations.argv import ArgvIntegration
    from sentry_sdk.integrations.atexit import AtexitIntegration
    from sentry_sdk.integrations.excepthook import ExcepthookIntegration
    from sentry_sdk.integrations.logging import LoggingIntegration
    from sentry_sdk.integrations.modules import ModulesIntegration
    from sentry_sdk.integrations.stdlib import StdlibIntegration
    from sentry_sdk.integrations.threading import ThreadingIntegration

    # from sentry_sdk.integrations.logging import LoggingIntegration
    sentry_sdk.init(
        dsn=cfg.dsn,
        default_integrations=False,
        before_send=cleanup_common_secret_data,
        integrations=[
            # # Default
            AtexitIntegration(),
            ExcepthookIntegration(),
            StdlibIntegration(),
            ModulesIntegration(),
            ArgvIntegration(),
            LoggingIntegration(event_level=logging.WARNING),
            ThreadingIntegration(),
            #  # Custom
            *extra_integrations,
        ],
    )


def configure_sentry_for_aiohttp(cfg: SentryConfig) -> None:
    from sentry_sdk.integrations.aiohttp import AioHttpIntegration

    configure_sentry(cfg, extra_integrations=(AioHttpIntegration(),))


def configure_sentry_for_flask(cfg: SentryConfig) -> None:
    from sentry_sdk.integrations.flask import FlaskIntegration

    configure_sentry(cfg, extra_integrations=(FlaskIntegration(),))


def hook_configure_configure_sentry_for_flask(app: flask.Flask, cfg: SentryConfig) -> None:
    """
    Try to configure sentry in uwsgi `postfork` if possible,
    but ensure it is configured in `before_first_request` (flask app).
    """
    try:
        import uwsgidecorators  # type: ignore
    except Exception:  # noqa
        pass
    else:

        @uwsgidecorators.postfork
        def _init_logging_in_uwsgi_postfork():  # type: ignore
            configure_sentry_for_flask(cfg)

    @app.before_first_request
    def _init_logging_in_before_first_request():  # type: ignore
        configure_sentry_for_flask(cfg)
