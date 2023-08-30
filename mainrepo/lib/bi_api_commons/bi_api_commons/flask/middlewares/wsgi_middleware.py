"""
Usage example:

.. code-block:: python
    app = Flask(__name__)
    FlaskWSGIMiddleware().wrap_flask_app(app)
"""
from __future__ import annotations

import abc
from typing import ClassVar, Optional, TYPE_CHECKING

import attr
import flask

if TYPE_CHECKING:
    from bi_api_commons.flask.types import WSGIEnviron, WSGIStartResponse, WSGICallable, WSGIReturn


@attr.s
class FlaskWSGIMiddleware(metaclass=abc.ABCMeta):
    _APP_FLAG_ATTR_NAME: ClassVar[str]

    _app: Optional[flask.Flask] = attr.ib(init=False, default=None)
    _original_wsgi_app: Optional[WSGICallable] = attr.ib(init=False, default=None)

    @property
    def original_wsgi_app(self) -> WSGICallable:
        if self._original_wsgi_app is None:
            raise ValueError(f"Middleware {self} was not bind to application")

        return self._original_wsgi_app

    @abc.abstractmethod
    def wsgi_app(self, environ: WSGIEnviron, start_response: WSGIStartResponse) -> WSGIReturn:
        return self.original_wsgi_app(environ, start_response)

    def wrap_flask_app(self, app: flask.Flask) -> None:
        if self._app is not None:
            raise ValueError("Middleware already wrapping Flask app")

        self._app = app
        self._original_wsgi_app = app.wsgi_app
        app.wsgi_app = self.wsgi_app  # type: ignore
        setattr(app, self._APP_FLAG_ATTR_NAME, True)

    @classmethod
    def is_wrapping_app(cls, app: flask.Flask) -> bool:
        return hasattr(app, cls._APP_FLAG_ATTR_NAME)
