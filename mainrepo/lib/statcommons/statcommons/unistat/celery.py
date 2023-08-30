# coding: utf8
"""
celery as source of signals for unistat.
"""

from __future__ import division, absolute_import, print_function, unicode_literals

import logging
from collections import OrderedDict

from ..celery import get_queue_size_passive_i

from . import common


LOGGER = logging.getLogger(__name__)


def celery_unistat(celery_app, celery_host=None, queue_sizes=True, taskstreams=True, chq_prefixes=('host__', 'chq__')):
    """
    Unistat metrics for `celery_app`.

    :param taskstreams: return task execution counts in form of `task_count_<taskname>_dmmm`.

    :param queue_sizes: query the broker for sizes (pending message counts) of the queues.

    :param chq_prefixes: possible prefixes in queue names signifying a 'current host queue'.
    """
    # # Hopefully not necessary:
    # common_prefix = common.get_common_prefix(require=True)
    common_prefix = ''

    if celery_host is None:
        fqdn = common.get_fqdn()
        celery_host = 'celery@{}'.format(fqdn)

    results = OrderedDict()

    def add_sensor(label, value, suffixes=('_ammv', '_axxx', '_ahhh')):
        # cheatsheet: yasm suffixes: https://wiki.yandex-team.ru/golovan/aggregation-types/
        # 4-letter sequence:
        # [(`a`bsolute | `d`elta), group_aggregate, metagroup_aggregate, time_aggregate];
        # aggregates: (h) hgram, (x) max, (n) min, (m) summ, (e) summ_none, (t) trnsp "last added", (v) aver;
        # 'max' = 'axxx';
        for suffix in suffixes:
            name = '{}{}{}'.format(common_prefix, label, suffix)
            results[name] = value

    control = celery_app.control
    # Current-host celery only:
    inspect = control.inspect(destination=[celery_host])
    active = inspect.active()
    if not active:  # e.g. celery has not started yet or is being too slow.
        return results
    active = active[celery_host]
    stats = inspect.stats()[celery_host]

    if queue_sizes:
        queue_sizes_data = get_queue_sizes(
            celery_app=celery_app, inspect=inspect, celery_host=celery_host,
            chq_prefixes=chq_prefixes)
        for queue_name, queue_size in queue_sizes_data:
            add_sensor('queue_size_{}'.format(queue_name), queue_size)

    if taskstreams:
        for task_name, count in stats['total'].items():
            task_name_clean = task_name.replace('.', '-').strip('_')
            # delta (it is cumulative, except for restarts),
            # summation by everything else (total count of a task).
            add_sensor('task_count_{}'.format(task_name_clean), count, suffixes=('_dmmm',))

    # rusage = stats['rusage']
    # rusage: {
    #     idrss: 0, inblock: 2040, isrss: 0, ixrss: 0, majflt: 1, maxrss: 259864,
    #     minflt: 201107546, msgrcv: 0, msgsnd: 0, nivcsw: 107075, nsignals: 0, nswap: 0,
    #     nvcsw: 2931198, oublock: 0, stime: 689.6, utime: 3322.64}

    # XXX: There must be a better way:
    total_workers = stats['pool']['max-concurrency']
    current_workers = len(stats['pool']['processes'])
    active_workers = len(active)
    worker_suffixes = ('_ammx', '_ahhh')
    add_sensor('workers_total', total_workers, suffixes=worker_suffixes)
    add_sensor('workers_idle', current_workers - active_workers, suffixes=worker_suffixes)
    add_sensor('workers_busy', active_workers, suffixes=worker_suffixes)
    add_sensor('workers_cheap', total_workers - current_workers, suffixes=worker_suffixes)

    return results


def get_queue_sizes(celery_app, inspect, celery_host, chq_prefixes, chq_title='current-host-queue'):
    queues = inspect.active_queues()[celery_host]
    queue_names = list(queue['name'] for queue in queues)
    chq_name = None
    if chq_prefixes:
        chqs = list(
            name for name in queue_names
            if any(name.startswith(prefix) for prefix in chq_prefixes))
        if chqs:
            # TODO: if len(chqs) > 1: warning()
            chq_name = chqs[0]

    with celery_app.connection_or_acquire() as conn:
        for queue in queues:
            name = queue['name']
            if name == chq_name:
                name = chq_title
            try:
                size = get_queue_size_passive_i(conn=conn, queue=name)
            except Exception as exc:
                LOGGER.warning("Error getting celery queue size (%r): %r", name, exc)
                yield name, None
                continue
            yield name, size
