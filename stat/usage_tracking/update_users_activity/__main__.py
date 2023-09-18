"""
Update a users activity table using a log table from prepare logs script
Used as a sandbox resource
Resources dir: /sandbox/projects/datalens/resources
"""

import argparse

from nile.api.v1 import aggregators as na
from nile.api.v1 import clusters
from nile.api.v1 import filters as nf
from qb2.api.v1 import extractors as qe
from qb2.api.v1 import resources as qr
from qb2.api.v1 import typing

TABLE_SCHEMA = dict(
    user_id=typing.String,
    last_event_time=typing.Int64,
    received_email=typing.Bool,
)


def get_args() -> argparse.Namespace:
    """Parses command line arguments"""

    parser = argparse.ArgumentParser(description="Update inactive users table with a new log")
    parser.add_argument("--users-table", required=True, help="inactive users table")
    parser.add_argument("--log-table", required=True, help="prepared datalens log table")
    parser.add_argument("--dst", required=True, help="destination table path")

    return parser.parse_args()


def run(users_table: str, log_table: str, dst: str) -> None:
    """Runs a Nile job on Hahn"""

    cluster = clusters.yt.Hahn()
    job = cluster.job()

    # get a latest activity timestamp for each user from a new log table
    log = job.table(log_table)
    new_records = (
        log.filter(nf.not_(nf.equals("user_id", b"__ANONYMOUS_USER_OF_PUBLIC_DATALENS__")))
        .groupby("user_id")
        .aggregate(new_event_time=na.max("event_time"))
    )

    # update the users table with new records
    users = job.table(users_table)
    users.join(new_records, by="user_id", type="full").project(
        "user_id",
        last_event_time=qe.or_("last_event_time", "new_event_time", "last_event_time"),
        received_email=qe.or_("received_email", "received_email", qr.const(False)),
    ).put(dst, schema=TABLE_SCHEMA, ensure_optional=False)

    job.run()


if __name__ == "__main__":
    args = get_args()
    run(args.users_table, args.log_table, args.dst)
