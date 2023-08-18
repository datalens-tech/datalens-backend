"""
Prepare logs located on Hahn for usage tracking
Used as a sandbox resource
Resources dir: /sandbox/projects/datalens/resources
"""

import argparse
import json
from argparse import Namespace
from collections import defaultdict
from datetime import timedelta, timezone
from multiprocessing.pool import ThreadPool
from typing import Optional, NamedTuple

from nile.api.v1 import (
    clusters,
    datetime as dt,
    extractors as ne,
    filters as nf,
)

from qb2.api.v1.typing import Int64, String


PLAINLY_PROJECTED_FIELDS_SCHEMA = dict(
    dash_id=String,
    dash_tab_id=String,
    chart_id=String,
    chart_kind=String,
    response_status_code=Int64,
    err_code=String,
    dataset_id=String,
    request_id=String,
    user_id=String,
    dataset_mode=String,
    connection_id=String,
    connection_type=String,
    source=String,
    username=String,
    execution_time=Int64,
    query=String,
    status=String,
    error=String,
    host=String,
    cluster=String,
    clique_alias=String,
    cache_used=bool,
    cache_full_hit=bool,
    is_public=Int64,
    endpoint_code=String,
    query_type=String,
)

CUSTOM_FIELDS_SCHEMA = dict(
    event_time=Int64,
    folder_id=String,
)

TABLE_SCHEMA = dict(
    **PLAINLY_PROJECTED_FIELDS_SCHEMA,
    **CUSTOM_FIELDS_SCHEMA
)


def validate_args(args: Namespace) -> None:
    """ Validates src paths according to the passed instance tags, throws ValueError with error messages if invalid """

    tags_expected = (args.time_scale, args.app_type, args.env)

    path_errors = defaultdict(list)
    n_src, n_dst = len(args.src), len(args.dst)
    if n_src != n_dst:
        path_errors['toplevel'].append(f'Number of source and destination tables should be equal ({n_src} != {n_dst})')

    for path in args.src:
        hierarchy = path.strip('/').split('/')
        if len(hierarchy) < 4:
            path_errors[path].append(
                'Should have at least 4 levels in --src path: '
                '"logs", service name with instance tags, time scale, table name'
            )
            continue
        if not (hierarchy[0] == 'logs' and hierarchy[1].startswith('datalens-back')):
            path_errors[path].append(f'Log paths should start with "//logs/datalens", got "{path}"')
            continue

        folder_tags = hierarchy[1].split('-')
        if folder_tags[-1] != 'fast':
            path_errors[path].append('Only fast logs.')
        app_type_actual, env_actual, time_scale_actual = folder_tags[-3], folder_tags[-2], '/'.join(hierarchy[2:-1])
        tags_actual = (time_scale_actual, app_type_actual, env_actual)
        if tags_actual != tags_expected:
            path_errors[path].append(f'The path does not match the arguments: expected {tags_expected}, got {tags_actual}')

    if path_errors:
        erreg = dict(
            message='Validation finished with errors',
            path_errors=path_errors,
        )
        raise ValueError(json.dumps(erreg, indent=4))


def mk_ttl(days: str) -> timedelta:
    return timedelta(days=int(days))


def get_args() -> Namespace:
    """ Parses command line arguments and calls validation """

    parser = argparse.ArgumentParser(description='Process logs from source and save to destination')
    parser.add_argument('--src', nargs='+', help='source table path(s) on Hahn')
    parser.add_argument('--dst', nargs='+', help='destination table path(s) on Hahn')
    parser.add_argument('--time-scale', required=True, choices=['30min', '1d', 'stream/5min'], help='time scale tag')
    parser.add_argument('--app-type', required=True, choices=['int', 'ext'], help='app type tag')
    parser.add_argument('--env', required=True, choices=['testing', 'production'], help='environment')
    parser.add_argument('--ttl-days', type=mk_ttl, default=None, help='table ttl, days')
    parser.add_argument('--filter-robot', action='store_true',
                        help='if set, would filter robot-datalens records from the result')
    args = parser.parse_args()

    validate_args(args)

    return args


class JobInput(NamedTuple):
    src: str
    dst: str
    ttl: Optional[timedelta] = None
    filter_robot: bool = False


def run(job_input: JobInput) -> None:
    """ Runs a Nile job on Hahn """

    cluster = clusters.yt.Hahn()
    job = cluster.job()

    log = job.table(job_input.src)
    records = log.filter(
        nf.equals('event_code', b'profile_db_request')
    ).project(
        *PLAINLY_PROJECTED_FIELDS_SCHEMA.keys(),
        event_time=ne.custom(
            lambda _: int(dt.Datetime.from_iso(_).replace(tzinfo=timezone.utc).timestamp()),
            'isotimestamp'
        ),
        folder_id='billing_folder_id',
    )

    if job_input.filter_robot:
        records = records.filter(nf.not_(nf.equals('username', b'robot-datalens')))
    records.put(job_input.dst, schema=TABLE_SCHEMA, ttl=job_input.ttl)
    job.run()


def main() -> None:
    args = get_args()
    with ThreadPool(8) as pool:
        pool.map(run, [JobInput(src, dst, args.ttl_days, args.filter_robot) for src, dst in zip(args.src, args.dst)])


if __name__ == '__main__':
    main()
