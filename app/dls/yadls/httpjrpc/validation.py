# coding: utf8
"""
...
"""


from __future__ import annotations

import os

from openapi_core import create_spec
from openapi_core.testing import MockRequest
try:
    from openapi_core.validators import RequestValidator
except ImportError:
    from openapi_core.validation.request.validators import RequestValidator
from openapi_core.exceptions import OpenAPIError

from yadls.utils import load_schema


__all__ = (
    'OpenAPIError',
    'make_mock_request',
    'validate',
)


HERE = os.path.dirname(__file__)


SCHEMAS = load_schema()
SPEC = create_spec(SCHEMAS)
VALIDATOR = RequestValidator(SPEC)


def make_mock_request(request, body, path_pattern=None, host_url=None):
    if host_url is None:
        host_url = request.host_url
    return MockRequest(
        host_url=host_url,
        method=request.method.lower(),
        path=request.path,
        path_pattern=path_pattern,
        view_args=request.view_args,
        args=request.args.items(),
        headers=request.headers.items(),
        cookies=request.cookies,
        data=body,
    )


def validate(openapi_request):
    return VALIDATOR.validate(openapi_request)
