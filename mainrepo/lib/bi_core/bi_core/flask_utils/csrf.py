"""
CSRF token check middleware

Similar frontend JS implementation: https://github.yandex-team.ru/data-ui/core/blob/master/lib/csrf.ts
"""

from __future__ import annotations

import time
import hmac
import logging
from typing import Optional

import flask

from bi_api_commons.flask.middlewares.commit_rci_middleware import ReqCtxInfoMiddleware


LOGGER = logging.getLogger(__name__)


def generate_csrf_token(yandexuid, timestamp):  # type: ignore  # TODO: fix
    msg = bytes('{}:{}'.format(yandexuid, timestamp), encoding='utf-8')
    secret = bytes(flask.current_app.config['CSRF_SECRET'], encoding='utf-8')
    h = hmac.new(key=secret, msg=msg, digestmod='sha1')
    return h.hexdigest()


CSRF_ERROR_RESP = ('CSRF validation failed', 400)


def validate_csrf_token(token_header_value: str, user_token) -> (bool, Optional[tuple]):  # type: ignore  # TODO: fix
    """
    :param token_header_value: CSRF token value to check
    :param user_token: current user id string (IAM user_id or yandexuid))
    :return: (is_valid, error_info_tuple)
    """
    if not token_header_value:
        return False, CSRF_ERROR_RESP

    try:
        token, timestamp = token_header_value.split(':')
        timestamp = int(timestamp)  # type: ignore  # TODO: fix
    except ValueError:
        return False, CSRF_ERROR_RESP

    if not token or timestamp <= 0:  # type: ignore  # TODO: fix
        return False, CSRF_ERROR_RESP

    ts_now = int(time.time())
    if ts_now - timestamp > flask.current_app.config['CSRF_TIME_LIMIT']:  # type: ignore  # TODO: fix
        return False, CSRF_ERROR_RESP

    if not hmac.compare_digest(generate_csrf_token(user_token, timestamp), token):
        return False, CSRF_ERROR_RESP

    return True, None


def csrf_check():  # type: ignore  # TODO: fix
    if flask.request.method not in flask.current_app.config['CSRF_METHODS']:
        return

    if not flask.request.cookies:
        return

    # Frontend in Cloud installation is migrating from "yandexuid" cookie usage
    # for CSRF token generation to IAM user_id.
    # Currently both options possible:
    #   - yandexuid - for Blackbox authenticated users
    #   - user_id - for federation users (authenticated in IAM SessionService).
    #
    # https://st.yandex-team.ru/BI-1757#5f569d3e035fc353df8eec82

    rci = ReqCtxInfoMiddleware.get_last_resort_rci()
    user_tokens = []
    if rci is not None and rci.user_id:
        user_tokens.append(rci.user_id)
    yandexuid = flask.request.cookies.get('yandexuid')
    if yandexuid:
        user_tokens.append(yandexuid)

    if not user_tokens:
        return

    csrf_token = flask.request.headers.get(flask.current_app.config['CSRF_HEADER_NAME'])

    LOGGER.info('Checking CSRF token for user tokens: %s', user_tokens)

    token_is_valid, error_info_tuple = None, None
    for user_token in user_tokens:
        token_is_valid, error_info_tuple = validate_csrf_token(csrf_token, user_token)  # type: ignore  # TODO: fix
        if token_is_valid:
            LOGGER.info('CSRF token is valid for user token %s', user_token)
            break

    if not token_is_valid:
        LOGGER.info('CSRF validation failed.')
        return error_info_tuple


def set_up(app, **kwargs):  # type: ignore  # TODO: fix
    app.before_request(csrf_check)
