from typing import Optional

import flask
from raven.contrib.flask import Sentry


def configure_raven_for_flask(app: flask.Flask, sentry_dsn: Optional[str], release: Optional[str] = None) -> None:
    if sentry_dsn is None:
        return
    app.config['SENTRY_PROCESSORS'] = ('bi_api_commons.logging_sentry.SecretsCleanupProcessor',)
    if release is not None:
        app.config['SENTRY_RELEASE'] = release
    Sentry(app, dsn=sentry_dsn)
