#!/usr/bin/env python
# coding: utf8

from __future__ import annotations

import os
import uuid
import urllib.parse
import logging
import shutil
from threading import Lock
from multiprocessing.pool import ThreadPool as Pool

import simplejson as json
import requests

from yadls.utils import ascii_to_uuid_prefix
from yadls.dbg import init_logging


CS_ID_PFX = ascii_to_uuid_prefix('cs_001')
HOST = os.environ.get('DLS_HOST') or 'http://[::1]:38080/_dls/'
API_KEY = os.environ.get('DLS_API_KEY') or ''
LOG = logging.getLogger('conf_storage_migrate')
FILES_LOCK = Lock()
FILENAME_FAILS = '.failed.jsl'
FILENAME_PENDING = '.pending.jsl'
FILENAME_DONE = '.done.jsl'


def make_session():
    from requests.packages.urllib3.util import Retry  # pylint: disable=import-error
    retry_conf = Retry(
        total=5, backoff_factor=0.5,
        status_forcelist=[500, 502, 503, 504, 521],
        method_whitelist=frozenset(['HEAD', 'TRACE', 'GET', 'PUT', 'OPTIONS', 'DELETE', 'POST']),
    )
    session = requests.Session()
    for prefix in ('http://', 'https://'):
        session.mount(
            prefix,
            requests.adapters.HTTPAdapter(
                max_retries=retry_conf,
                pool_connections=100, pool_maxsize=100,
            ))
    return session


SESSION = make_session()


def cs_id_to_uuid(cs_id):
    cs_id = int(cs_id)
    return str(uuid.UUID(int=CS_ID_PFX + cs_id))


def get_requestses(filename='_conf_storage_roles_g.json'):
    with open(filename) as fo:
        data = json.load(fo)

    headers = {'X-API-Key': API_KEY, 'X-User-Id': 'system_user:root'}
    params = dict(_disable_validation='1')
    req_common = dict(
        params=params,
        headers=headers,
        verify=False,
    )

    for key, val in data.items():
        item_uuid = cs_id_to_uuid(key)
        val['diff']['removed'] = {}  # no need there
        reqs = [
            # # test
            # dict(
            #     method='GET',
            #     url='debug/',
            #     params=dict(_raise='the_test_raise_message'),
            #     headers=headers,
            # ),
            # # ...
            dict(
                req_common,
                method='PUT',
                url='nodes/entries/{}'.format(item_uuid),
                json=dict(
                    initialOwner='system_user:root',
                    initialPermissionsMode='void',
                    scope='widget',
                ),
            ),
            dict(
                req_common,
                method='PATCH',
                url='nodes/all/{}/permissions'.format(item_uuid),
                json=val,
            ),
        ]
        reqs = [dict(req, url=urllib.parse.urljoin(HOST, req['url']))
                for req in reqs]
        yield (dict(id=key), reqs)


def req_exc_info(exc):
    try:
        return '%s: %s' % (exc.response.status_code, exc.response.content)
    except Exception:
        return '%r' % (exc,)


def log_failed(args):
    data = json.dumps(args) + '\n'
    with FILES_LOCK:
        with open(FILENAME_FAILS, 'a', 1) as fo:
            fo.write(data)


def log_done(args):
    data = json.dumps(args[0]) + '\n'
    with FILES_LOCK:
        with open(FILENAME_DONE, 'a', 1) as fo:
            fo.write(data)


def process_one(args):
    meta, reqs = args
    try:
        for req in reqs:
            resp = SESSION.request(**req)
            resp.raise_for_status()
        log_done(args)
        LOG.info("Done: %r", meta)
    except Exception as exc:
        log_failed(args)
        LOG.exception("Failed: %r: %s", meta, req_exc_info(exc))


def process_items(reqses, pool_size):
    if pool_size:
        pool = Pool(pool_size)
        result = pool.imap_unordered(process_one, reqses, chunksize=50)
        for item in result:
            pass
    # else:
    for item in reqses:
        process_one(item)


def main(pool_size=24, limit=None, max_retry_loops=10):
    init_logging()

    reqses = get_requestses()

    for fln in (FILENAME_FAILS, FILENAME_PENDING):
        try:
            os.unlink(fln)
        except FileNotFoundError:
            pass

    if limit:
        import itertools
        reqses = itertools.islice(reqses, 1000)

    process_items(reqses, pool_size=pool_size)

    for tries_remain in reversed(range(max_retry_loops)):
        try:
            shutil.move(FILENAME_FAILS, FILENAME_PENDING)
        except FileNotFoundError:
            return
        LOG.error("Processing a retry loop (%r)", tries_remain)
        with open(FILENAME_PENDING) as fo:
            reqses = (json.loads(line.strip()) for line in fo)
            process_items(reqses, pool_size=pool_size)
    if os.path.exists(FILENAME_FAILS):
        raise Exception("Ran out of retries. See {}".format(FILENAME_FAILS))


if __name__ == '__main__':
    main()
