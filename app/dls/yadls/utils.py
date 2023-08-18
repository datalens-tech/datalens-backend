# coding: utf8
"""
...
"""


from __future__ import annotations

import os
import sys
import functools
import uuid
import collections
import datetime
import urllib
import json
import pkgutil

import pytz
import yaml
# import ujson  # pylint: disable=unused-import

from .exceptions import LimitReached


__all__ = (
    'json',
    'make_uuid',
    'groupby',
    'ensure_set',
    'simple_memoize',
    'StateSuperMixin',
    'ReprMixin',
    'DocStringInheritor',
    'copy_permissions',
    'split_list',
    'split_permissions',
    'flatten_permissions',
    'map_permissions',
    'first_or_default',
    'datetime_now',
    'alist',
)


UTC = pytz.UTC


def make_uuid():
    return str(uuid.uuid4())


def to_bytes(value, encoding='utf-8', default=None, **kwargs):
    if isinstance(value, bytes):
        return value
    if isinstance(value, str):  # NOTE: `future.types.newstr.newstr`
        return value.encode(encoding, **kwargs)
    if default is not None:
        value = to_bytes(default(value), encoding=encoding, **kwargs)
    return value


def to_text(value, encoding='utf-8', default=None, **kwargs):
    if isinstance(value, str):  # NOTE: `future.types.newstr.newstr`
        return value
    if isinstance(value, bytes):
        return value.decode(encoding, **kwargs)
    if default is not None:
        value = to_text(default(value), encoding=encoding, **kwargs)
    return value


# For `__repr__` implementation and `type(name, ...)` calling.
if sys.version_info.major == 2:
    to_str = to_bytes
else:
    to_str = to_text


def groupby(items):
    """
    Group pairs by first item.

    Modified version of
    https://github.com/pytoolz/toolz/blob/e052d819b2d8dcf50f0147b5659b9a530204d05f/toolz/itertoolz.py#L66

    >>> groupby([[1, 1], [2, 2], [1, 3]])
    {1: [1, 3], 2: [2]}
    """
    # A hopefully-performant hack by using an `append` of a list directly.
    # Could sometimes do even better by returning a dict without making another copy of it.
    data = collections.defaultdict(lambda: [].append)  # type: ignore  # TODO: fix
    for key, value in items:
        data[key](value)
    return {key: val.__self__ for key, val in data.items()}  # type: ignore  # TODO: fix


def ensure_set(iterable):
    if isinstance(iterable, set):
        return iterable
    return set(iterable)


def simple_memoize(func):
    """
    ...

    >>> import time
    >>> @simple_memoize
    ... def stuff():
    ...     return time.time()
    ...
    >>> val = stuff()
    >>> val == stuff()
    True
    """

    memo = {}  # type: ignore  # TODO: fix

    @functools.wraps(func)
    def simple_memoized(*args, **kwargs):
        if memo:
            return memo['result']
        result = func(*args, **kwargs)
        memo['result'] = result
        return result

    simple_memoized._memo = memo  # type: ignore  # TODO: fix  # pylint: disable=protected-access
    return simple_memoized


@simple_memoize
def hostname():
    import socket
    return socket.gethostname()


@simple_memoize
def hostname_short():
    result = hostname()  # e.g. iva5-a6bdaf547a14.qloud-c.yandex.net
    result = result.split('.', 1)[0]  # e.g. iva5-a6bdaf547a14
    return result


class StateSuperMixin:
    """
    A mixin class that implements the simple base logic of `__getstate__` and `__setstate__`.
    """

    def __getstate__(self):
        return self.__dict__.copy()

    def __setstate__(self, state):
        self.__dict__.update(state)


class ReprMixin:

    _repr_keys = ()

    def __unicode__(self):
        return '<%s(%s)>' % (
            self.__class__.__name__,
            ', '.join(  # type: ignore  # TODO: fix
                '%s=%r' % (key, getattr(self, key, None))
                for key in self._repr_keys))

    def __repr__(self):
        return to_str(self.__unicode__())


class DocStringInheritor(type):
    def __new__(cls, name, bases, clsdict):

        if not ('__doc__' in clsdict and clsdict['__doc__']):
            for mro_cls in (mro_cls for base in bases for mro_cls in base.mro()):
                doc = mro_cls.__doc__
                if doc:
                    clsdict['__doc__'] = doc
                    break

        for attr, attribute in clsdict.items():
            if not attribute.__doc__:
                mro_clses = (
                    mro_cls for base in bases for mro_cls in base.mro()
                    if hasattr(mro_cls, attr))
                for mro_cls in mro_clses:
                    doc = getattr(getattr(mro_cls, attr), '__doc__')
                    if doc:
                        if isinstance(attribute, property):
                            # XXX: might break very custom properties.
                            clsdict[attr] = property(
                                attribute.fget,  # type: ignore  # TODO: fix
                                attribute.fset,  # type: ignore  # TODO: fix
                                attribute.fdel,  # type: ignore  # TODO: fix
                                doc,
                            )
                        else:
                            attribute.__doc__ = doc
                        break

        return type.__new__(cls, name, bases, clsdict)


def copy_permissions(perms):
    """ A minimally-deep permissions structure copy """
    return {perm_kind: [subject for subject in subjects]
            for perm_kind, subjects in perms.items()}


def split_list(lst, cond):
    """
    Split list items into `(matching, non_matching)` by `cond(item)` callable.
    """
    res1, res2 = [], []
    for item in lst:
        if cond(item):
            res1.append(item)
        else:
            res2.append(item)
    return res1, res2


def split_permissions(perms, cond):
    matching = {}
    nonmatching = {}
    for perm_kind, values in perms.items():
        matching[perm_kind], nonmatching[perm_kind] = split_list(values, cond)
    return matching, nonmatching


def flatten_permissions(perms):
    return list(value for values in perms.values() for value in values)


def map_permissions(perms, mapper, filter_none=True):
    result = {
        perm_kind: [
            mapper(value, perm_kind=perm_kind)
            for value in values]
        for perm_kind, values in perms.items()}
    if filter_none:
        result = {
            perm_kind: [value for value in values if value is not None]
            for perm_kind, values in result.items()}
    return result


def first_or_default(iterable, default=None):
    iterable = iter(iterable)
    try:
        return next(iterable)
    except StopIteration:
        return default


def datetime_now():
    return pytz.UTC.localize(datetime.datetime.utcnow())  # pylint: disable=no-value-for-parameter


def call_on_last(iterable, func, catch_empty=True):
    """
    Prefetch an extra element from the iterable and call `func(**kwargs)` when the last
    one would be yielded (or if iterable is empty and `catch_empty`).

    A sentinel version.

    >>> list(call_on_last([], lambda **kwargs: kwargs))
    [{'case': 'empty', 'empty': True}]
    >>> list(call_on_last([], lambda **kwargs: kwargs, catch_empty=False))
    []
    >>> list(call_on_last([1], lambda **kwargs: kwargs))
    [{'case': 'single_value', 'value': 1}]
    >>> list(call_on_last([1, 2], lambda **kwargs: kwargs))
    [1, {'case': 'normal', 'value': 2, 'prev_value': 1}]
    >>> list(call_on_last([1, 2, 3], lambda **kwargs: kwargs))
    [1, 2, {'case': 'normal', 'value': 3, 'prev_value': 2}]
    """
    value = prev_value = sentinel = object()
    for item in iterable:
        prev_value = value
        if prev_value is not sentinel:
            yield prev_value
        value = item

    if prev_value is sentinel:
        if value is sentinel:
            if catch_empty:
                yield func(case='empty', empty=True)
            # else: -> []
        else:
            yield func(case='single_value', value=value)
    else:
        yield func(case='normal', value=value, prev_value=prev_value)


def call_on_last_v02(iterable, func, catch_empty=True):
    """
    Prefetch an extra element from the iterable and call `func(**kwargs)` when the last
    one would be yielded (or if iterable is empty and `catch_empty`).

    A StopIteration version.

    >>> list(call_on_last([], lambda **kwargs: kwargs))
    [{'case': 'empty', 'empty': True}]
    >>> list(call_on_last([], lambda **kwargs: kwargs, catch_empty=False))
    []
    >>> list(call_on_last([1], lambda **kwargs: kwargs))
    [{'case': 'single_value', 'value': 1}]
    >>> list(call_on_last([1, 2], lambda **kwargs: kwargs))
    [1, {'case': 'normal', 'value': 2, 'prev_value': 1}]
    >>> list(call_on_last([1, 2, 3], lambda **kwargs: kwargs))
    [1, 2, {'case': 'normal', 'value': 3, 'prev_value': 2}]
    """
    iterable = iter(iterable)
    try:
        value = next(iterable)
    except StopIteration:
        if catch_empty:
            yield func(case='empty', empty=True)
        # else: -> []
        return

    prev_value = value
    try:
        value = next(iterable)
    except StopIteration:
        yield func(case='single_value', value=prev_value)
        return

    yield prev_value

    for item in iterable:
        yield value
        prev_value = value
        value = item

    yield func(case='normal', value=value, prev_value=prev_value)


def filter_fields(item, fields, _sentinel=object()):
    """
    Return only the specified path-located fields in a structure.

    >>> data = {'a': 1, 'b': None, 'c': {'d': None}, 'e': {'f': {'g': 1, 'h': 2}, 'i': 3}}
    >>> filter_fields(data, ['a', 'b', 'c'])
    {'a': 1, 'b': None, 'c': {'d': None}}
    >>> filter_fields(data, ['a', 'b', 'c.d', 'c.e', 'c'])
    {'a': 1, 'b': None, 'c': {'d': None}}
    >>> filter_fields(data, ['c.d.e', 'e.f.g', 'e.f.h'])
    {'c': {'d': None}, 'e': {'f': {'g': 1, 'h': 2}}}
    >>> filter_fields(data, ['b.c.d', 'e.f.h', 'e.jk', 'e.i'])
    {'b': None, 'e': {'f': {'h': 2}, 'i': 3}}
    >>> data_l = {'j': {'k': 1, 'l': [{'m': 11, 'n': [{'o': 9, 'p': 10}]}, {'m': 12}, {'m': 13}]}}
    >>> filter_fields(data_l, ['j.l.m'])
    {'j': {'l': [{'m': 11}, {'m': 12}, {'m': 13}]}}
    >>> filter_fields(data_l, ['j.l.n.o'])
    {'j': {'l': [{'n': [{'o': 9}]}, {}, {}]}}
    """
    if isinstance(item, dict):
        pass
    elif isinstance(item, list):
        return [filter_fields(subitem, fields) for subitem in item]
    else:  # a non-processed type i.e. a scalar.
        return item

    result = {}
    fields = list(
        field.split('.') if not isinstance(field, (list, tuple)) else field
        for field in fields)

    # e.g. item = {'a': 1, 'b': None, 'e': {'f': {'g': 1}}}
    # e.g. roots = {'b': [('c', 'd')], 'e': [('f', 'h'), ('jk',)]}
    roots = groupby((fld[0], tuple(fld[1:])) for fld in fields)
    for root, subfields in roots.items():
        value = item.get(root, _sentinel)
        if value is _sentinel:
            continue  # a nonexistent field, leave it out of the result too.

        if any(not subfield for subfield in subfields):
            # Essentially requested the entire value anyhow.
            result[root] = value
            continue

        result[root] = filter_fields(
            item=value, fields=subfields)

    return result


def raise_on_last(iterable, exc):
    def func(**kwargs):
        raise exc
    return call_on_last(iterable, func=func)


def limited_range(*args, exc=LimitReached):
    """
    A `range()` wrapper that raises a `LimitReached` exception when reaching the end.

    Useful for limiting otherwise infinite loops.
    """
    return raise_on_last(range(*args), exc=exc)


def ascii_to_hex(string):
    return ''.join('%x' % (ord(v),) for v in string)


def ascii_to_int(string):
    if isinstance(string, str):
        string = string.encode('ascii')
    char_bitsize = 8
    return sum(
        char << shift * char_bitsize
        for shift, char in enumerate(reversed(string)))


def ascii_to_uuid_prefix(string):
    uuid_bitsize = 128
    if isinstance(string, str):
        string = string.encode('ascii')
    char_bitsize = 8
    width = len(string) * char_bitsize
    assert width <= uuid_bitsize
    number = ascii_to_int(string)
    return number << (uuid_bitsize - width)


def prefixed_uuid(prefix: str, value: int = 0):
    assert value >= 0
    # Ensure no overlap:
    assert len(str.encode('utf-8')) * 8 + value.bit_length() <= 128
    uuid_int = ascii_to_uuid_prefix(prefix) + value
    return str(uuid.UUID(int=uuid_int))


def chunks(lst, size):
    """ Yield successive chunks from lst. No padding.  """
    for idx in range(0, len(lst), size):
        yield lst[idx:idx + size]


async def alist(aiterable):
    result = []
    async for item in aiterable:
        result.append(item)
    return result


def cut_string(string: str, left, right=None, sep='…'):
    """
    ...

    >>> cut_string('12345678', left=3, right=3)
    '123…678'
    >>> cut_string('12345678', left=4, right=4)
    '12345678'
    >>> cut_string('12345678', 5)
    '123…78'
    >>> cut_string('1', 4)
    '1'
    """
    if right is None:
        right = int(left / 2)
        left = left - right
    if len(string) <= left + right:
        return string
    return '{}{}{}'.format(string[:left], sep, string[-right:])  # pylint: disable=invalid-unary-operand-type


def with_last(it):
    """
    Wrap an iterable yielding an `is_last, value`.

    >>> list(with_last([]))
    []
    >>> list(with_last([1]))
    [(True, 1)]
    >>> list(with_last([1, 2]))
    [(False, 1), (True, 2)]
    >>> list(with_last([1, 2, 3]))
    [(False, 1), (False, 2), (True, 3)]
    """
    it = iter(it)
    try:
        prev_value = next(it)
    except StopIteration:
        return
    value = prev_value
    for value in it:
        yield False, prev_value
        prev_value = value
    yield True, value


def maybe_postmortem(ei, print_exc=True):
    if not os.environ.get('DLS_IPDBG'):
        return

    if print_exc:
        import traceback
        traceback.print_exception(*ei)

    _, _, sys.last_traceback = ei
    import ipdb
    ipdb.pm()


def serialize_ts(value):
    if not value:
        return None
    return value.isoformat()


def uniq(lst, key=lambda value: value):
    """
    Get unique elements of an iterable preserving its order and optionally
    determining uniqueness by hash of a key.
    """
    known = set()
    for item in lst:
        item_key = key(item)
        if item_key not in known:
            yield item
            known.add(item_key)


def req(
        url, params=None,
        *,
        json=None,  # pylint: disable=redefined-outer-name
        method='GET',
        base_url=None,
        rfs=True,
        session=None,
        **kwargs):
    """
    Common catch point with conveniences for synchronous HTTP requesting.
    """
    if base_url is not None:
        url = urllib.parse.urljoin(base_url, url)
    if session is None:
        import requests
        session = requests
    resp = session.request(method, url, params=params, json=json, **kwargs)
    if rfs:
        resp.raise_for_status()
    return resp


def get_default_requests_session(
        retries_total=5,
        retries_backoff_factor=0.5,
        retries_status_forcelist=(500, 502, 503, 504, 521),
        retries_method_whitelist=frozenset(['HEAD', 'TRACE', 'GET', 'PUT', 'OPTIONS', 'DELETE', 'POST']),
        pool_connections=30,
        pool_maxsize=30,
        **kwargs):
    import requests
    from requests.packages.urllib3.util import Retry  # pylint: disable=import-error

    session = requests.Session(**kwargs)  # type: ignore  # TODO: fix

    retry_conf = Retry(
        total=retries_total,
        backoff_factor=retries_backoff_factor,
        status_forcelist=retries_status_forcelist,
        method_whitelist=retries_method_whitelist,
    )

    for prefix in ('http://', 'https://'):
        session.mount(
            prefix,
            requests.adapters.HTTPAdapter(
                max_retries=retry_conf,
                pool_connections=pool_connections,
                pool_maxsize=pool_maxsize,
            ))
    return session


def load_schema_data():
    return pkgutil.get_data(__name__, 'schemas.yaml')


def load_schema():
    data = load_schema_data()
    return yaml.load(data, Loader=getattr(yaml, 'CSafeLoader', None) or yaml.SafeLoader)
