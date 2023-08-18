# coding: utf8
"""
django as http daemon for unistat handling.
"""

from __future__ import division, absolute_import, print_function, unicode_literals

import os
from collections import OrderedDict

try:
    from django.http import HttpResponse
except Exception:
    HttpResponse = None

from .common import results_to_response
from .uwsgi import uwsgi_unistat


def unistat_django_view(request):  # pylint: disable=unused-argument
    if HttpResponse is None:
        raise Exception("`django` not found")
    results = OrderedDict()
    if os.environ.get('UWSGI_STATS') and os.environ.get('QLOUD_APPLICATION'):
        results.update(uwsgi_unistat())
    data = ''.join(results_to_response(results))
    return HttpResponse(data, content_type='application/json; charset=utf-8')
