# coding: utf8
"""
uwsgi as source of signals for unistat.
"""

from __future__ import division, absolute_import, print_function, unicode_literals

import os
import time
import socket
import json
from collections import OrderedDict

from .common import get_sys_memstatus, get_common_prefix, maybe_float_size


__all__ = (
    'uwsgi_unistat',
)


def group(lst, cls=dict):
    res = cls()
    for key, val in lst:
        try:
            group_list = res[key]
        except KeyError:
            res[key] = [val]
        else:
            group_list.append(val)
    return res


def _collect_uwsgi_info(path, collect_memstatus=True, encoding='utf-8'):
    sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    sock.connect(path)
    try:
        data = ''
        while True:
            buf = sock.recv(64 * 2**10)
            if not buf:
                break
            data += buf.decode(encoding)
    finally:
        sock.close()

    data = json.loads(data)

    if collect_memstatus:
        for worker_info in data['workers']:
            worker_info['sys_memstatus'] = get_sys_memstatus(worker_info.get('pid'))

    return data


def uwsgi_collect_sensors(data, add_sensor):
    now_ts = time.time()

    # relevant: https://github.com/wdtinc/uwsgi-cloudwatch/blob/master/uwsgi_cloudwatch/main.py#L79
    add_sensor("load", data['load'])
    add_sensor("signal_queue", data['signal_queue'])

    # locks: [{'user 0': 0}, {'signal': 0}, ...]
    add_sensor("locks_total", sum(value for lock in data['locks'] for value in lock.values()))

    # sockets:
    # [{'can_offload': 0, 'name': '/run/yandex-statface-api-v4/uwsgi.sock',
    #   'proto': 'uwsgi', 'queue': 0, 'shared': 0, 'max_queue': 0}, ...]
    add_sensor("sockets_queue_total", sum(item['queue'] for item in data['sockets']))

    add_sensor("workers_total", len(data['workers']))
    w_by_s = group((item['status'], item) for item in data['workers'])
    for key in ('idle', 'busy', 'cheap'):
        w_by_s.setdefault(key, [])
    for status, items in w_by_s.items():
        add_sensor(
            "workers_{}".format(status),
            len(items),
            suffixes=['_ammx', '_ahhh'],
        )
    add_sensor("workers_idle", len(w_by_s["idle"]), suffixes=['_ammn', '_annn'])

    avg_rts = [worker['avg_rt'] for worker in data['workers']]
    avg_rts = [val for val in avg_rts if val]
    if avg_rts:
        add_sensor("max_avg_rt_sec", float(max(avg_rts)) / 1e6)

    # pylint: disable=invalid-name
    GiB = 2 ** 30  # Gibi- (for gibibytes)
    page_size = 4096  # 4KiB, linux constant
    pages_per_gib = GiB / page_size  # 262144

    mem_datas = [  # all values in GiB
        dict(
            rss_gib=maybe_float_size(worker['rss'], GiB),
            vsz_gib=maybe_float_size(worker['vsz'], GiB),
            vmsize_gib=maybe_float_size(
                worker.get('sys_memstatus', {}).get('size'),
                pages_per_gib),
            vmrss_gib=maybe_float_size(
                worker.get('sys_memstatus', {}).get('resident'),
                pages_per_gib),
        ) for worker in data['workers']]

    # `pandas` would be useful here; but probably not worth loading it up.
    # max_worker_rss max_worker_vsz all_worker_rss all_worker_vsz
    # max_worker_vmrss max_worker_vmsize all_worker_vmrss all_worker_vmsize
    for key in ('rss_gib', 'vsz_gib', 'vmsize_gib', 'vmrss_gib'):
        series = [mem_data[key] for mem_data in mem_datas if mem_data.get(key) is not None]
        if not series:
            continue
        for func_name, func in (('max', max), ('all', sum)):
            add_sensor("{}_worker_{}".format(func_name, key), func(series))

    cores = [core for worker in data['workers'] for core in worker['cores']]
    running_cores = [core for core in cores if core.get('req_info')]
    if running_cores:
        min_request_start = min(core['req_info'].get('request_start') for core in running_cores)
        add_sensor("longest_current_req_sec", now_ts - min_request_start)


def process_uwsgi_data_for_unistat(data, common_prefix=None, line_prefix=None):
    if common_prefix is None:
        common_prefix = get_common_prefix(require=True)

    results = OrderedDict()

    # # Qloud resource aggregates, for comparison:
    # memory: thhh tmmv tvvv txxx
    # cpu usage: hgram tmmv tvvv txxx
    # cpu limit: thhh tmmv txxx
    def add_sensor(label, value, prefix=line_prefix, suffixes=('_axxx', '_ahhh')):
        for suffix in suffixes:
            name = '{}{}{}{}'.format(
                common_prefix, prefix or '', label.lower(), suffix)
            results[name] = value

    # TODO: tune the aggregation for usefulness.

    uwsgi_collect_sensors(data, add_sensor)

    return results


def process_uwsgi_data_for_prometheus(data, label_prefix=''):
    results = []

    # pylint: disable=unused-argument
    def add_sensor(label, value, suffixes=None):
        results.append((label_prefix + label, value))

    uwsgi_collect_sensors(data, add_sensor)

    return results


def _get_uwsgi_data(sock_path=None):
    if sock_path is None:
        sock_path = os.environ.get('UWSGI_STATS')
    if not sock_path:
        raise Exception("UWSGI_STATS socket path env variable is not set")

    data = _collect_uwsgi_info(sock_path)
    return data


def uwsgi_prometheus(sock_path=None, label_prefix=''):
    data = _get_uwsgi_data(sock_path=sock_path)
    return process_uwsgi_data_for_prometheus(data, label_prefix=label_prefix)


# pylint: disable=too-many-locals
def uwsgi_unistat(sock_path=None, common_prefix=None, line_prefix=None):
    data = _get_uwsgi_data(sock_path=sock_path)
    return process_uwsgi_data_for_unistat(
        data,
        common_prefix=common_prefix,
        line_prefix=line_prefix)
