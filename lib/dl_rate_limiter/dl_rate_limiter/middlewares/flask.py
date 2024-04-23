import logging

import attr
import flask

import dl_rate_limiter.request_rate_limiter as request_rate_limiter


logger = logging.getLogger(__name__)


@attr.s(auto_attribs=True)
class FlaskMiddleware:
    _rate_limiter: request_rate_limiter.SyncRequestRateLimiterProtocol

    def process(self) -> None | flask.Response:
        try:
            result = self._rate_limiter.check_limit(
                request=request_rate_limiter.Request(
                    url=str(flask.request.path),
                    method=flask.request.method,
                    headers=flask.request.headers,
                )
            )
            if result is False:
                logger.warning("Request was rate limited")
                return flask.Response(
                    status=429,
                    response={"description": "Too Many Requests"},
                )
        except Exception:
            logger.exception("Failed to check request limit")

        logger.info("No request limit was found")
        return None

    def set_up(self, app: flask.Flask) -> None:
        app.before_request(self.process)
