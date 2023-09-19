"""
Prepare aggregated usage tracking stats
Used as a sandbox resource
Resources dir: /sandbox/projects/datalens/resources
"""

import argparse

from nile.api.v1 import aggregators as na
from nile.api.v1 import clusters
from nile.api.v1 import extractors as ne
from nile.api.v1 import filters as nf
from qb2.api.v1 import typing


PLAINLY_PROJECTED_FIELDS = [
    "dash_id",
    "dash_tab_id",
    "chart_id",
    "chart_kind",
    "dataset_id",
    "user_id",
    "connection_id",
    "connection_type",
    "username",
    "host",
    "clique_alias",
]
TABLE_SCHEMA = {key: typing.Optional[typing.String] for key in PLAINLY_PROJECTED_FIELDS} | dict(
    cache_hits=typing.UInt64,
    errors_count=typing.UInt64,
    event_date=typing.String,
    request_count=typing.UInt64,
)


def get_args() -> argparse.Namespace:
    """Parses command line arguments"""

    parser = argparse.ArgumentParser(description="Prepares aggregated usage tracking stats")
    parser.add_argument("--src", required=True, help="source table path")
    parser.add_argument("--event-date", required=True, help="date associated with the source table, in CH Date format")
    parser.add_argument("--dst", required=True, help="destination table path")

    return parser.parse_args()


def run(src: str, event_date: str, dst: str) -> None:
    """Runs a Nile job on Hahn"""

    cluster = clusters.yt.Hahn()
    job = cluster.job()

    log = job.table(src)
    log.filter(nf.not_(nf.equals("username", b"robot-datalens"))).unique("request_id").project(
        *PLAINLY_PROJECTED_FIELDS,
        event_date=ne.const(event_date),
        is_cache_hit=ne.custom(lambda hit: 1 if hit else 0, "cache_full_hit"),
        is_error=ne.custom(lambda code: int(code != 200), "response_status_code"),
    ).groupby(*(PLAINLY_PROJECTED_FIELDS + ["event_date"])).aggregate(
        cache_hits=na.sum("is_cache_hit"),
        errors_count=na.sum("is_error"),
        request_count=na.count(),
    ).put(
        dst, schema=TABLE_SCHEMA, ensure_optional=False
    )

    job.run()


if __name__ == "__main__":
    args = get_args()
    run(args.src, args.event_date, args.dst)
