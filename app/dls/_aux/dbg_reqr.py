#!/usr/bin/env python
# coding: utf8

from __future__ import annotations

import os
import time
import urllib.parse
import logging
import itertools
import random as random_base
from threading import Lock
from multiprocessing.pool import ThreadPool as Pool

import requests

from yadls.utils import prefixed_uuid
from yadls.dbg import init_logging


HOST = os.environ.get('DLS_HOST') or 'http://[::1]:38080/_dls/'
API_KEY = os.environ.get('DLS_API_KEY') or ''
LOG = logging.getLogger('dbg_reqr')
RND = random_base.Random(1)
POOL_SIZE = 15
CONTEXT = {}
MIN_TIME = 0.1


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
                # max_retries=retry_conf,
                pool_connections=POOL_SIZE + 3, pool_maxsize=POOL_SIZE + 3,
            ))
    return session


SESSION = make_session()


def req_exc_info(exc):
    try:
        return '%s: %s' % (exc.response.status_code, exc.response.content)
    except Exception:  # pylint: disable=broad-except
        return '%r' % (exc,)


class RPSCounter:

    last_ts = None
    counter = None
    first_ts = None
    total_counter = None

    def __init__(self, name, min_td=2.0):
        self.name = name
        self.min_td = min_td
        self.total_counter = 0
        self.reset()

    def reset(self, now=None):
        if now is None:
            now = time.time()
        self.last_ts = now
        self.counter = 0
        return 0

    def feed(self, pre_handle=True, now=None, count=1):
        if pre_handle:
            self.handle(now=now)
        self.counter += count
        self.total_counter += count
        return self.counter

    def handle(self, now=None):
        if now is None:
            now = time.time()

        if now - self.last_ts >= self.min_td:
            self.emit(now=now)

        if self.first_ts is None:
            self.first_ts = now

        return now

    def emit(self, now):
        LOG.info(
            "RPS (%s): %.3f (total: %.3f)",
            self.name,
            self.counter / (now - self.last_ts),
            (self.total_counter / (now - self.first_ts)
             if self.first_ts is not None else float('NaN')),
        )
        return self.reset(now=now)


RPSC_MAIN = RPSCounter('main')
RPSC_SUCCESS = RPSCounter('successes')
RPSC_FAIL = RPSCounter('failures')
SYNC_LOCK = Lock()


def process_one_i__dbsleep(meta):
    resp = SESSION.request(
        method='GET',
        url=urllib.parse.urljoin(HOST, 'debug/'),
        params=dict(
            _db_sleep=0.6 + 1 * RND.random(),
            _db_level=RND.randrange(3),
            _meta=meta,
        ),
    )
    resp.raise_for_status()
    return resp, resp.json()


def process_one_i__entry(meta):
    base_meta, submeta = meta
    idx = base_meta + submeta * 100
    item_uuid = prefixed_uuid('x_dbg', idx)
    result = []
    resp = SESSION.request(
        method='PUT',
        url=urllib.parse.urljoin(HOST, 'nodes/entries/{}'.format(item_uuid)),
        headers={'X-API-Key': API_KEY, 'X-User-Id': 'system_user:root'},
        json=dict(
            initialOwner='system_user:root',
            initialPermissionsMode='void',
            scope='debug',
        ),
    )
    resp.raise_for_status()
    result.append(resp)
    resp = SESSION.request(
        method='PATCH',
        url=urllib.parse.urljoin(HOST, 'nodes/all/{}/permissions'.format(item_uuid)),
        headers={'X-API-Key': API_KEY, 'X-User-Id': 'system_user:root'},
        json={
            "diff": {
                "removed": {
                    "acl_adm": [
                        {
                            "subject": "user:robot-charts"
                        }
                    ]
                },
                "added": {
                    "acl_view": [
                        {
                            "comment": "import",
                            "subject": "system_group:staff_statleg"
                        }
                    ],
                    "acl_adm": [
                        {
                            "comment": "import",
                            "subject": "user:seqvirioum"
                        }
                    ],
                    "acl_edit": [
                        {
                            "comment": "import",
                            "subject": "system_group:staff_statleg"
                        }
                    ]
                }
            }
        },
    )
    resp.raise_for_status()
    result.append(resp)
    return result


def init_tst_check_context():
    # psql -qtAX 'dbname=yadls host=pgaas.mail.yandex.net port=12000 user=yadls' -c "select uuid from dls_nodes;" > node_uuids.lst  # pylint: disable=line-too-long
    CONTEXT['nodes'] = list(val.strip() for val in open('node_uuids.lst'))
    # psql -qtAX 'dbname=yadls host=pgaas.mail.yandex.net port=12000 user=yadls' -c "select name from dls_subject where kind = 'user';" > user_names.lst  # pylint: disable=line-too-long
    CONTEXT['users'] = list(val.strip() for val in open('user_names.lst'))
    from yadls.core import DLSBase
    CONTEXT['actions'] = list(DLSBase.default_scope_info['actions'])


def process_one_i__check(meta):
    node_uuid = RND.choice(CONTEXT['nodes'])
    action = RND.choice(CONTEXT['actions'])
    user = RND.choice(CONTEXT['users'])

    uri = '/_dls/nodes/all/{}/access/{}'.format(node_uuid, action)
    resp = SESSION.request(
        method='GET',
        url=urllib.parse.urljoin(HOST, uri),
        params=dict(
            user=user,
            # _optimization='parallel',
            # _optimization='none',
        ),
        headers={
            'X-API-Key': API_KEY,
            'X-Request-Id': 'dbg_{}'.format(meta),
        },
    )
    resp.raise_for_status()
    return resp, resp.json()['result']


process_one_i = process_one_i__check


def process_one(base_meta):
    for submeta in itertools.count(1):
        meta = (base_meta, submeta)
        t1 = time.time()
        try:
            result = process_one_i(meta)
            t2 = time.time()
            LOG.info("Done: %r: %r, in %.3fs", meta, result, t2 - t1)
            RPSC_SUCCESS.feed()
        except Exception as exc:  # pylint: disable=broad-except
            t3 = time.time()
            LOG.error("Failed: %r: %s, in %.3fs", meta, req_exc_info(exc), t3 - t1)
            RPSC_FAIL.feed()
        RPSC_MAIN.feed()
        t4 = time.time()
        td = t4 - t1
        if td < MIN_TIME + 0.001:
            time.sleep(MIN_TIME - td)


def main():
    init_logging()

    init_tst_check_context()
    reqses = range(POOL_SIZE)

    pool = Pool(POOL_SIZE)
    jobs = [pool.apply_async(process_one, args=(idx,))
            for idx in reqses]
    for job in jobs:
        job.wait()


if __name__ == '__main__':
    main()
