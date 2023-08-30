# coding: utf8
"""
Requests for humans that have to deal with lots of stuff.
"""
# pylint: disable=arguments-differ,too-many-arguments,useless-object-inheritance,import-outside-toplevel

from __future__ import division, absolute_import, print_function, unicode_literals

import os
import sys
import logging
import functools

import requests
import requests.adapters
import requests.exceptions
from requests.packages.urllib3.util import Retry  # pylint: disable=import-error


PY3 = sys.version_info >= (3,)
if PY3:
    text_type = str
    import urllib.parse as urllib_parse
else:
    # pylint: disable=undefined-variable
    text_type = unicode  # noqa
    import urlparse as urllib_parse  # pylint: disable=import-error

# Singleton session for connection pooling.
# WARNING: this involves caching; do `SESSION.close()` in a `finally:` block to clean it up.
SESSION = requests.Session()


def to_bytes(value, default=None, encoding='utf-8', errors='strict'):
    if isinstance(value, bytes):
        return value
    if isinstance(value, text_type):
        return value.encode(encoding, errors)
    if default is not None:
        return default(value)
    return value


def to_text(value, default=None, encoding='utf-8', errors='strict'):
    if isinstance(value, text_type):
        return value
    if isinstance(value, bytes):
        return value.decode(encoding, errors)
    if default is not None:
        return default(value)
    return value


if PY3:
    to_str = to_text  # pylint: disable=invalid-name
else:
    to_str = to_bytes  # pylint: disable=invalid-name


class ReprObj(object):
    """ A class for inserting specific text in __repr__ outputs.  """

    def __init__(self, txt):
        self.txt = txt

    def __repr__(self):
        return self.txt


UNDEF = ReprObj('Unspecified')
AUTO = ReprObj('Autoselected')


class LazyRepr(object):
    """
    Alternative of `lazystr` that does not do additional `repr()`ing, i.e. the
    `func` must return a string.
    """

    def __init__(self, func):
        self.func = func

    def __repr__(self):
        return to_str(self.func())

    def __str__(self):
        return to_str(self.func())

    def __unicode__(self):
        return to_text(self.func())


def dict_maybe_items(value):
    items = getattr(value, 'items', None)
    if items is not None:
        return items()
    return value


def split_list(iterable, condition):
    """
    Split list items into `(matching, non_matching)` by `cond(item)` callable.
    """
    matching = []
    non_matching = []
    for item in iterable:
        if condition(item):
            matching.append(item)
        else:
            non_matching.append(item)
    return matching, non_matching


def split_dict(source, condition, cls=dict):
    source = dict_maybe_items(source)
    matching, nonmatching = split_list(
        source,
        lambda item: condition(item[0], item[1]))
    return cls(matching), cls(nonmatching)


def current_frame(depth=1):
    """
    (from logging/__init__.py)
    """
    func = getattr(sys, '_getframe', None)
    if func is not None:
        return func(depth)
    # fallback; probably not relevant anymore.
    try:
        raise Exception
    except Exception:  # pylint: disable=broad-except
        frame = sys.exc_info()[2].tb_frame
        for _ in range(depth):
            frame = frame.f_back


def find_caller(extra_depth=1, skip_packages=()):
    """
    Find the stack frame of the caller so that we can note the source
    file name, line number and function name.

    Mostly a copypaste from `logging`.

    :param skip_packages: ...; example: `[getattr(logging, '_srcfile', None)]`.
    """
    cur_frame = current_frame(depth=2 + extra_depth)  # our caller, i.e. parent frame
    frame = cur_frame
    # On some versions of IronPython, currentframe() returns None if
    # IronPython isn't run with -X:Frames.
    result = "(unknown file)", 0, "(unknown function)"
    while hasattr(frame, "f_code"):
        codeobj = frame.f_code
        filename = os.path.normcase(codeobj.co_filename)
        # Additionally skip
        if any(filename.startswith(pkg) for pkg in skip_packages if pkg):
            frame = frame.f_back
            continue
        result = (codeobj.co_filename, frame.f_lineno, codeobj.co_name)
        break
    return result


def _is_special(value, none=True, undef=True, auto=True):
    """
    Check whether `value` is one of the special values used in this module.
    """
    if none and value is None:
        return True
    if undef and value is UNDEF:
        return True
    if auto and value is AUTO:
        return True
    return False


def configure_session(
        session,
        retries_total=5,
        retries_backoff_factor=0.5,
        retries_status_forcelist=(500, 502, 503, 504, 521),
        retries_method_whitelist=frozenset((
            'HEAD', 'OPTIONS', 'TRACE', 'GET',
            'POST', 'PUT', 'PATCH', 'DELETE')),
        pool_connections=30,
        pool_maxsize=30,
        raise_on_status=False,
        **kwargs):
    """
    Configure session with significant retries.

    WARNING: by default, retries mutating requests too. Use only when
    idempotence is expected (or at-least-once delivery is generally okay).
    """
    retry_conf = Retry(
        total=retries_total,
        backoff_factor=retries_backoff_factor,
        status_forcelist=retries_status_forcelist,
        method_whitelist=retries_method_whitelist,
        # https://stackoverflow.com/a/43496895
        raise_on_status=raise_on_status,
    )

    for prefix in ('http://', 'https://'):
        session.mount(
            prefix,
            requests.adapters.HTTPAdapter(
                max_retries=retry_conf,
                pool_connections=pool_connections,
                pool_maxsize=pool_maxsize,
                **kwargs))

    return session


def _cut(data, length, marker):
    if not length:
        return data
    actual_length = length - len(marker)
    assert actual_length >= 2, "can't cut to/below marker length"
    if len(data) < length:
        return data
    right_length = actual_length // 2
    left_length = actual_length - right_length
    return data[:left_length] + marker + data[right_length:]


SESSION_ZEALOUS = configure_session(requests.Session())


class RequesterBase(object):

    apply_environment = True
    length_cut_marker = b'...'
    _prepare_request_keys = frozenset((
        'method', 'url',
        'headers', 'auth', 'cookies',
        'params',
        'data', 'files', 'json',
        'hooks',
    ))
    send_request_keys = frozenset((
        'proxies', 'cert', 'verify',
        'stream', 'timeout',
        'allow_redirects',
    ))

    # Warning: argument defaults are also duplicated in `Requester.__init__`.
    def __init__(self, session=True):
        if session is True:
            session = SESSION
        elif _is_special(session):
            session = requests.Session()
        self.session = session
        self.logger = logging.getLogger('req')

    # Warning: kwargs defaults are also duplicated in `Requester.request`.
    @staticmethod
    def _preprepare_parameters(kwargs):
        """
        Separated out for easier overriding of `prepare_parameters` by subclasses.
        """
        headers = kwargs.get('headers')
        # For common convenience, commonize the headers:
        if _is_special(headers):
            headers = {}
        else:
            # TODO?: OrderedDict?
            headers = {
                key.lower().replace('_', '-'): value
                for key, value in dict_maybe_items(headers)}
        kwargs['headers'] = headers

        return kwargs

    # Warning: kwargs defaults are also duplicated in `Requester.request`.
    def _prepare_parameters(self, kwargs):  # pylint: disable=no-self-use
        return kwargs

    def request(self, url, **kwargs):
        kwargs['url'] = url
        request, send_kwargs = self._prepare_request(kwargs)
        return self.send_request(request, **send_kwargs)

    # Warning: kwargs defaults are also duplicated in `Requester.request`.
    def _prepare_request(self, kwargs, run_processing=True):
        """
        ...

        Not to be confused with `Session.prepare_request`, which takes a
        request and returns only a request.
        """
        session = kwargs.pop('session', None) or self.session

        if run_processing:
            kwargs = self._preprepare_parameters(kwargs)
            kwargs = self._prepare_parameters(kwargs)

        assert kwargs.get('method')
        assert kwargs.get('url')

        prepare_keys = self._prepare_request_keys
        prepare_kwargs, send_kwargs = split_dict(
            kwargs, lambda key, value: key in prepare_keys)

        send_keys = self.send_request_keys
        send_kwargs, unknown_kwargs = split_dict(
            send_kwargs, lambda key, value: key in send_keys)

        if unknown_kwargs:
            raise ValueError("Unknown request arguments", unknown_kwargs)

        request = requests.Request(**prepare_kwargs)
        request = session.prepare_request(request)
        if self.apply_environment:
            send_kwargs_overrides = send_kwargs
            # http://docs.python-requests.org/en/master/user/advanced/#prepared-requests
            send_kwargs = session.merge_environment_settings(
                request.url, {}, None, None, None)
            send_kwargs.update(send_kwargs_overrides)
        send_kwargs['session'] = session
        return request, send_kwargs

    def send_request(self, request, **kwargs):
        """
        ...

        Essentially a wrap of `self.session.send_request` but the session
        object can be specified in arguments.
        """
        session = kwargs.pop('session', None) or self.session
        return session.send(request, **kwargs)

    def __call__(self, *args, **kwargs):
        """ See `request` method """
        kwargs['call_extra_depth'] = (kwargs.get('call_extra_depth') or 0) + 1
        return self.request(*args, **kwargs)


class RequesterSessionWrap(RequesterBase):

    def __getattr__(self, key):
        return getattr(self.session, key)


class RequesterVerbMethods(RequesterBase):
    """
    Add the http-method-named methods to the class, wrapping `self.request`.
    """

    def get(self, *args, **kwargs):
        """ See `request` method """
        kwargs['call_extra_depth'] = (kwargs.get('call_extra_depth') or 0) + 1
        kwargs['method'] = 'get'
        return self.request(*args, **kwargs)

    def options(self, *args, **kwargs):
        """ See `request` method """
        kwargs['call_extra_depth'] = (kwargs.get('call_extra_depth') or 0) + 1
        kwargs['method'] = 'options'
        return self.request(*args, **kwargs)

    def head(self, *args, **kwargs):
        """ See `request` method """
        kwargs['call_extra_depth'] = (kwargs.get('call_extra_depth') or 0) + 1
        kwargs['method'] = 'head'
        return self.request(*args, **kwargs)

    def post(self, *args, **kwargs):
        """ See `request` method """
        kwargs['call_extra_depth'] = (kwargs.get('call_extra_depth') or 0) + 1
        kwargs['method'] = 'post'
        return self.request(*args, **kwargs)

    def put(self, *args, **kwargs):
        """ See `request` method """
        kwargs['call_extra_depth'] = (kwargs.get('call_extra_depth') or 0) + 1
        kwargs['method'] = 'put'
        return self.request(*args, **kwargs)

    def patch(self, *args, **kwargs):
        """ See `request` method """
        kwargs['call_extra_depth'] = (kwargs.get('call_extra_depth') or 0) + 1
        kwargs['method'] = 'patch'
        return self.request(*args, **kwargs)

    def delete(self, *args, **kwargs):
        """ See `request` method """
        kwargs['call_extra_depth'] = (kwargs.get('call_extra_depth') or 0) + 1
        kwargs['method'] = 'delete'
        return self.request(*args, **kwargs)


class RequesterDefaults(RequesterBase):

    # Warning: argument defaults are also duplicated in `Requester.__init__`.
    def __init__(
            self, default_timeout=5.0, default_write_method='post',
            default_allow_redirects=AUTO,
            **kwargs):
        self.default_timeout = default_timeout
        self.default_write_method = default_write_method
        self.default_allow_redirects = default_allow_redirects
        super(RequesterDefaults, self).__init__(**kwargs)

    # Warning: kwargs defaults are also duplicated in `Requester.request`.
    def _prepare_parameters(self, kwargs):
        # NOTE: will leave explicit `timeout=None` as-is.
        if _is_special(kwargs.get('timeout'), none=False):
            kwargs.update(timeout=self.default_timeout)

        # Determine the method by the presence of `data` / `files`.
        if _is_special(kwargs.get('method')):
            if kwargs.get('data') is not None or kwargs.get('files') is not None:
                kwargs['method'] = self.default_write_method
            else:
                kwargs['method'] = 'get'

        if _is_special(kwargs.get('allow_redirects')):
            if self.default_allow_redirects == 'auto' or self.default_allow_redirects is AUTO:
                # From `requests.get` logic.
                kwargs['allow_redirects'] = kwargs['method'] in ('get', 'options')
            else:
                kwargs['allow_redirects'] = self.default_allow_redirects

        return super(RequesterDefaults, self)._prepare_parameters(kwargs)


class RequesterBaseUrl(RequesterBase):
    """ Common relative URL support for RequesterBase """

    # Warning: argument defaults are also duplicated in `Requester.__init__`.
    def __init__(self, base_url=None, **kwargs):
        self.base_url = base_url
        super(RequesterBaseUrl, self).__init__(**kwargs)

    # Warning: kwargs defaults are also duplicated in `Requester.request`.
    def _prepare_parameters(self, kwargs):
        if self.base_url:
            kwargs['url'] = urllib_parse.urljoin(
                self.base_url,
                kwargs['url'])  # required parameter
        return super(RequesterBaseUrl, self)._prepare_parameters(kwargs)


class RequesterContentType(RequesterBase):
    """
    `requests` itself allows a `json=...` parameter to the methods.

    However, that is a bad design decision.

    This mixin adds a support for 'content_type' paremeter which defines how
    the body should be serialized.

    WARNING: it also sets the default content type to `'json'`.
    """

    # Warning: argument defaults are also duplicated in `Requester.__init__`.
    def __init__(self, default_content_type='json', **kwargs):
        self.default_content_type = default_content_type
        super(RequesterContentType, self).__init__(**kwargs)

    # Warning: kwargs defaults are also duplicated in `Requester.request`.
    def _prepare_parameters(self, kwargs):
        content_type = kwargs.pop('content_type', None)
        if _is_special(content_type):
            content_type = self.default_content_type
        data = kwargs.get('data')
        json_data = kwargs.pop('json', None)
        if json_data is not None:
            raise Exception((
                "Please don't use `json=...` keyword here."
                " Use `content_type='json'` (likely, already the default)."))
        if data is not None and content_type is not None:
            # # Maybe:
            # if content_type == 'multipart':
            #     kwargs.pop('data', None)
            #     kwargs['files'] = data
            data, content_type_header = self.serialize_data(
                data, content_type=content_type, context=kwargs)
            if data is not None:
                kwargs['data'] = data
                if content_type_header is not None:
                    kwargs['headers']['content-type'] = content_type_header
        return super(RequesterContentType, self)._prepare_parameters(kwargs)

    json_content_type = 'application/json; charset=utf-8'

    def serialize_data(self, data, content_type, context=None):
        """
        Data object `data`, desired serialization `content_type`
        ->
        serialized_data, content_type_header

        NOTE: `content_type=None` has a special meaning 'pass the data as-is'.
        """
        if _is_special(content_type):
            return data, None

        # TODO: check `data` for being a file-like or a bytestream or such.

        # # Maybe:
        # if content_type == 'urlencode':
        #     return None, data
        if content_type == 'json':
            return self._serialize_data_json(data, context=context)
        if content_type == 'ujson':  # Special case for dangerous performance.
            return self._serialize_data_ujson(data, context=context)
        raise Exception("Unknown data serialization content_type", content_type)

    def _serialize_data_json(self, data, context=None):  # pylint: disable=unused-argument
        """ Overridable point for json-serialization customization. """
        from .anyjson import json_dumps
        data = to_bytes(json_dumps(data))
        return data, self.json_content_type

    def _serialize_data_ujson(self, data, context=None):  # pylint: disable=unused-argument
        import ujson
        # pylint: disable=c-extension-no-member
        data = to_bytes(ujson.dumps(data, ensure_ascii=False))
        return data, self.json_content_type


class RequesterMeta(RequesterBase):

    # Warning: argument defaults are also duplicated in `Requester.__init__`.
    def __init__(self, collect_call_info=True, call_info_in_ua=True, **kwargs):
        self.collect_call_info = collect_call_info
        self.call_info_in_ua = call_info_in_ua
        super(RequesterMeta, self).__init__(**kwargs)

    # Warning: kwargs defaults are also duplicated in `Requester.request`.
    def _prepare_parameters(self, kwargs):
        call_info = kwargs.pop('call_info', None)
        if call_info and self.call_info_in_ua:
            # `call_info`: `(cfile, cline, cfunc)`.
            kwargs['headers']['user-agent'] = '{}, {}:{}: {}'.format(
                kwargs['headers'].get('user-agent') or '',
                *call_info)
        return super(RequesterMeta, self)._prepare_parameters(kwargs)

    def request(self, url, **kwargs):
        """
        ...

        WARNING: should either be the entry-point method or be wrapped with

            kwargs['call_extra_depth'] = (kwargs.get('call_extra_depth') or 0) + 1
        """
        call_extra_depth = kwargs.pop('call_extra_depth', 0)
        if self.collect_call_info and kwargs.get('call_info') is None:
            # Could actually filter out the current module, but that's costly and palliative.
            kwargs['call_info'] = find_caller(extra_depth=call_extra_depth + 1)
        return super(RequesterMeta, self).request(url, **kwargs)


class RequesterAutoRaiseForStatus(RequesterBase):

    raise_with_content = True
    raise_content_cap = 1800  # in bytes.
    response_exception_cls = requests.exceptions.HTTPError

    # Warning: kwargs defaults are also duplicated in `Requester.request`.
    def _prepare_request(self, kwargs, **etcetera):
        # This could be done by auto-merging `send_request_keys` by the MRO /
        # property, but for a single case this is easier:
        require = kwargs.pop('require', True)
        request, send_kwargs = (
            super(RequesterAutoRaiseForStatus, self)
            ._prepare_request(kwargs, **etcetera))
        send_kwargs['require'] = require
        return request, send_kwargs

    def send_request(self, request, **kwargs):
        require = kwargs.pop('require', True)
        response = super(RequesterAutoRaiseForStatus, self).send_request(request, **kwargs)
        if require:
            self.raise_for_status(response)
        return response

    def raise_for_status(self, response):
        """ `response.raise_for_status()` or similar """
        if not self.raise_with_content:
            response.raise_for_status()
            return
        if response.ok:
            return
        self.raise_for_status_forced(response)

    def _get_raise_content(self, response):
        # TODO?: support streaming?
        # Note: cutting the bytestream.
        content = response.content
        content = _cut(
            content,
            length=self.raise_content_cap,
            marker=self.length_cut_marker)
        return content

    def raise_for_status_forced(self, response):
        """
        `response.raise_for_status()` but for any status.

        Also attempts to put a piece of the response body into the error.
        """
        try:
            content = self._get_raise_content(response)
        except Exception as exc:
            self.logger.warning("_get_raise_content failed: %r", exc)
            raise self.response_exception_cls(
                "Status Error: {} {}".format(
                    response.status_code, response.reason),
                response=response)
        raise self.response_exception_cls(
            "Status Error: {} {}: {}".format(
                response.status_code, response.reason, content),
            response=response)


class RequesterLog(RequesterBase):

    log_response_headers = True
    logging_url_cap = 1800

    def send_request(self, request, **kwargs):
        self.log_before(request, **kwargs)
        try:
            response = super(RequesterLog, self).send_request(request, **kwargs)
        except Exception as exc:
            exc_info = sys.exc_info()
            self.log_exc(request, exc, exc_info=exc_info, **kwargs)
            raise
        self.log_after(request, response, **kwargs)
        return response

    def request_for_logging(self, request):
        """ Make a log string out of a request """
        url = request.url
        url_cap = self.logging_url_cap
        if url_cap and len(url) > url_cap:
            url = '{}{}'.format(url[:url_cap], self.length_cut_marker)

        data_info = ''
        if request.body:
            data_info = "  data_len={}".format(len(request.body))

        return "{method} {url}{data_info}".format(
            method=request.method.upper(), url=url, data_info=data_info)

    def request_for_logging_lazy(self, request):
        """ ... exactly what it says """
        return LazyRepr(functools.partial(self.request_for_logging, request))

    def response_for_logging(self, response):
        """ Make a log string out of a response """
        pieces = [
            response.status_code,
            response.request.method,
            response.url,
            '    {}b'.format(len(response.content or '')),
        ]
        try:
            elapsed = "in {:.3f}s".format(response.elapsed.total_seconds())
        except Exception:  # pylint: disable=broad-except
            elapsed = "in ???s"
        pieces.append(elapsed)
        if self.log_response_headers:
            pieces.append('    response.headers={!r}'.format(response.headers))
        return ' '.join(
            text_type(piece) if not isinstance(piece, text_type) else piece
            for piece in pieces)

    # pylint: disable=unused-argument
    def log_before(self, request, level=logging.DEBUG, **kwargs):
        """ Hook for logging before a request is sent """
        if not self.logger.isEnabledFor(level):
            return
        self.logger.debug(
            "Sending request: %s",
            self.request_for_logging_lazy(request))

    # pylint: disable=unused-argument
    def log_after(self, request, response, level=logging.INFO, **kwargs):
        """ Hook for logging after a response is received """
        if not self.logger.isEnabledFor(level):
            return
        self.logger.log(
            level,
            "Response: %s    -> %s",
            self.request_for_logging_lazy(request),
            self.response_for_logging(response))

    # pylint: disable=unused-argument
    def log_exc(self, request, exc, exc_info=None, level=logging.ERROR, **kwargs):
        """ Log an exception that occured when getting a response """
        if not self.logger.isEnabledFor(level):
            return

        response_str = ''
        response = getattr(exc, 'response', None)
        if response is not None:
            response_str = ' ({})'.format(
                self.response_for_logging(response))

        self.logger.log(
            level,
            "Request exception: %s -> %r%s",
            self.request_for_logging_lazy(request),
            exc,
            response_str,
            exc_info=exc_info or True)


class Requester(
        RequesterLog, RequesterAutoRaiseForStatus, RequesterMeta,
        RequesterContentType, RequesterBaseUrl, RequesterDefaults,
        RequesterVerbMethods, RequesterSessionWrap, RequesterBase):

    def __init__(
            self,
            session=True,
            default_timeout=5.0, default_write_method='post', default_allow_redirects=AUTO,
            base_url=None,
            default_content_type='json',
            collect_call_info=True, call_info_in_ua=True,
            **kwargs):
        """
        ...

        WARNING: by default it uses a singleton session; if you want to put default
        headers on it, specify `session=None`.

        WARNING: by default, adds caller filename, line number, function name
        to the user-agent header.

        :param session: session object to use; additionally:
          `None`: make a new session;
          `True`: use a singleton session;

        :param default_timeout: ...

        :param default_write_method: what method to use when request body is specified.

        :param default_allow_redirects: whether return or follow a redirect;
          'auto' / `Autoselected` means "only for safe request methods".

        :param base_url: URL to use for relative links, in HTML /
          urllib.parse.urljoin semantics.

        :param default_content_type: serialization to use for non-bytestream `data`
          (request body) by default.

        :param collect_call_info: collect the caller (stack) information at all.

        :param call_info_in_ua: add the stack information to user agent.
        """
        super(Requester, self).__init__(
            session=session,
            default_timeout=default_timeout,
            default_write_method=default_write_method,
            default_allow_redirects=default_allow_redirects,
            base_url=base_url,
            default_content_type=default_content_type,
            collect_call_info=collect_call_info,
            call_info_in_ua=call_info_in_ua,
            **kwargs)

    # TODO: make it possible to supply defaults for everything in `__init__`.
    # pylint: disable=unused-argument,too-many-locals
    def request(
            self, url, params=None, data=None,
            # *,  # someday, someday.
            method=None, headers=None, auth=None, cookies=None,
            content_type=None, files=None,
            hooks=None, proxies=None, cert=None,
            verify=True, stream=False,
            timeout=UNDEF, allow_redirects=None,
            call_extra_depth=0, call_info=None,
            session=None,
            require=True,
            **kwargs):
        """
        Send a request, return a response.

        :param url: URL to send the request to;
          can be relative to `base_url`.

        :param params: query-string contents; see `requests.request`.

        :param data: a serializable python object, or bytestream, or file-like
        object to send in the request body; see `requests.request`.

        :param require: whether a non-okay response should raise (automatic raise_for_status).

        :param method: HTTP method to use;
          default: 'get' if no request body is specified,
          `default_write_method` otherwise.

        :param headers: additional HTTP headers dict.

        :param auth: Basic/Digest/Custom HTTP Auth;
          e.g. an `(username, password) tuple for sending to the server in basic auth;
          see `requests.request`;
        http://docs.python-requests.org/en/master/user/advanced/#custom-authentication

        :param cookies: dict or CookieJar; see `requests.request`.

        :param content_type: which type to serialize the `data` in, if necessary;
          default: see `default_content_type` in `__init__`.

        :param files: streams to send in a multipart body; see `requests.request`.

        :param hooks: see `requests.request`;
        http://docs.python-requests.org/en/master/user/advanced/#event-hooks

        :param proxies: protocol to proxy address; see `requests.request`;
        http://docs.python-requests.org/en/master/user/advanced/#proxies

        :param cert: SSL client certificate; see `requests.request`.

        :param verify: server certificate verification; see `requests.request`.

        :param stream: allow chunked reading of the response body; see `requests.request`.

        :param timeout: single request attempt timeout; see `requests.request`;
          default: see `default_timeout` in `__init__`;
          `None` means "no timeout".

        :param allow_redirects: whether HTTP 301 / HTTP 302 should be followed;
          see `requests.request`;
          default: see `default_allow_redirects` in `__init__`.

        :param call_extra_depth: for call_info collection, skip this many stack
          items. Increment this by one when wrapping the method.

        :param call_info: excplicit `(filename, lineno, func)` to use as caller
          info (instead of `call_extra_depth`).

        :param session: requests.Session object to use; defaults to `self.session`.
        """
        return super(Requester, self).request(
            url,
            session=session,
            method=method, headers=headers, auth=auth, cookies=cookies, params=params,
            data=data, content_type=content_type, files=files,
            hooks=hooks,
            proxies=proxies, cert=cert, verify=verify,
            stream=stream, timeout=timeout, allow_redirects=allow_redirects,
            call_extra_depth=call_extra_depth + 1, call_info=call_info,
            require=require,
            **kwargs)


class APIRequester(Requester):
    """
    Requester slightly tuned for backend-to-APIs interaction.

    Includes retries, large timeout, and considers only a very small list of
    statuses as okay.

    Warning: by default it uses a singleton session; if you want to put default
    headers on it, specify `session=None`.
    """

    def __init__(self, session=True, **kwargs):
        if session is True:
            session = SESSION_ZEALOUS
        elif _is_special(session):
            session = configure_session(requests.Session())
        kwargs.setdefault('default_timeout', 120)
        kwargs.setdefault('default_allow_redirects', False)
        super(APIRequester, self).__init__(session=session, **kwargs)

    # '204' is often returned for `DELETE` requests.
    okay_statuses = (200, 201, 204)

    def raise_for_status(self, response):
        if response.status_code not in self.okay_statuses:
            self.raise_for_status_forced(response)


def _make_short_levelnames(shortnum=True):
    """ Return a dict (levelnum -> levelname) with short names for logging.
    `shortnum`: also shorten all 'Level #' names to 'L##'.
    """
    _names = dict([
        (logging.DEBUG, 'DBG'),
        (logging.INFO, 'INFO'),  # d'uh
        (logging.WARN, 'WARN'),
        (logging.ERROR, 'ERR'),
        (logging.CRITICAL, 'CRIT'),
    ])
    if shortnum:
        for i in range(1, 100):
            _names.setdefault(i, "L%02d" % (i,))
    return _names


BASIC_LOG_FORMAT = str("%(asctime)s: %(levelname)-13s: %(name)s: %(message)s")


def init_logging(*args, **kwargs):
    """ Simple shorthand for neat and customizable logging init """

    short_levelnames = kwargs.pop('short_levelnames', True)
    if short_levelnames:
        _names = _make_short_levelnames()
        for lvl, name in _names.items():
            logging.addLevelName(lvl, str(name))

    kwargs.setdefault('level', logging.DEBUG)

    logformat = BASIC_LOG_FORMAT
    kwargs.setdefault('format', logformat)

    logging.basicConfig(*args, **kwargs)


def test():
    init_logging(level=1)
    reqr = Requester()
    resp = reqr.get('https://example.com?param1=a', params=dict(param2='b'))
    print(resp)
    assert resp.ok
    resp = reqr(
        'https://example.com?param2=a',
        params=dict(param1='b'),
        headers={'Content-Type': 'nope, delete this'},
        data=dict(param3=['c', 1]))
    print(resp)


if __name__ == '__main__':
    test()
