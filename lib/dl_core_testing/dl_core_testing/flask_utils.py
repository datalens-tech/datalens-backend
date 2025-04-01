from __future__ import annotations

import json
from typing import Optional

from flask.testing import FlaskClient
import werkzeug

from dl_api_commons.tracing import get_current_tracing_headers


class FlaskTestResponse(werkzeug.wrappers.Response):
    def _json(self):  # type: ignore  # TODO: fix
        """Get the result of json.loads if possible."""
        if "json" not in self.mimetype:  # type: ignore  # TODO: fix
            raise AttributeError("Not a JSON response")
        return json.loads(self.data)

    json = werkzeug.utils.cached_property(_json)


class FlaskTestClient(FlaskClient):
    def get_us_home_directory(self) -> str:
        """To split US directories for different users to prevent permissions issues"""
        raise NotImplementedError()

    def get_default_headers(self) -> dict[str, Optional[str]]:
        return {}

    def post_process_response(self, resp) -> None:  # type: ignore  # TODO: fix
        pass

    def open(self, *args, **kw):  # type: ignore  # TODO: fix
        kw["headers"] = {**self.get_default_headers(), **kw.get("headers", {}), **get_current_tracing_headers()}
        kw["headers"] = {key: val for key, val in kw["headers"].items() if val}
        resp = super().open(*args, **kw)
        self.post_process_response(resp)
        return resp
