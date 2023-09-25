from __future__ import annotations

from aiohttp import web
import attr


@attr.s(frozen=True)
class ServerHeader:
    _server_header: str = attr.ib()

    @_server_header.validator
    def validate_server_header(self, _, value):  # type: ignore  # TODO: fix
        if not isinstance(value, str):
            raise TypeError(f"Server header must be a 'str', not '{type(value).__qualname__}'")
        if len(value) < 1:
            raise ValueError("Server header must have non-zero length")

    def add_signal_handlers(self, app: web.Application):  # type: ignore  # TODO: fix
        app.on_response_prepare.append(self.on_response_prepare)

    async def on_response_prepare(self, _: web.Request, response: web.Response):  # type: ignore  # TODO: fix
        response.headers["Server"] = self._server_header
