import typing

import attr
import flask
from werkzeug.exceptions import Forbidden

from dl_api_commons.crypto import get_hmac_hex_digest


@attr.s
class BodySignatureValidator:
    hmac_keys: typing.Sequence[bytes] = attr.ib()
    header: str = attr.ib()

    def validate_request_body(self) -> None:
        if flask.request.method in ("HEAD", "OPTIONS", "GET"):  # no body to validate.
            return

        # For import-test reasons, can't verify this when getting it;
        # but allowing requests when the key is empty is too dangerous.
        if not self.hmac_keys:
            raise Exception("validate_request_body: no hmac_keys passed")

        if any(not key for key in self.hmac_keys):
            raise Exception("validate_request_body: empty hmac_key passed")

        body_bytes = flask.request.get_data()
        signature_str_from_header = flask.request.headers.get(self.header)
        for hmac_key in self.hmac_keys:
            expected_signature = get_hmac_hex_digest(body_bytes, secret_key=hmac_key)

            if expected_signature == signature_str_from_header:
                return

        raise Forbidden("Invalid signature")

    def set_up(self, app: flask.Flask) -> None:
        app.before_request(self.validate_request_body)
