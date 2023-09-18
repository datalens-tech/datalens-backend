"""
Parse entry -> admin's puid mapping from DLS
Used as a sandbox resource
Resources dir: /sandbox/projects/datalens/resources
"""

import argparse

from nile.api.v1 import clusters
from nile.api.v1 import filters as nf
from qb2.api.v1 import typing

TABLE_SCHEMA = dict(
    entry_id=typing.String,
    puid=typing.UInt64,
)


def get_args() -> argparse.Namespace:
    """Parses command line arguments"""

    parser = argparse.ArgumentParser(description="Prepares aggregated usage tracking stats")
    parser.add_argument("--entries-table", required=True, help="source entries table path")
    parser.add_argument("--groups-table", required=True, help="source groups table path")
    parser.add_argument("--dash-admins", required=True, help="destination table path for dashes")
    parser.add_argument("--entries-admins", required=True, help="destination table path for other entries")

    return parser.parse_args()


def run(entries_table: str, groups_table: str, dash_admins_dst: str, entries_admins_dst: str) -> None:
    """Runs a Nile job on Hahn"""

    cluster = clusters.yt.Hahn()
    job = cluster.job()

    # filter out non-admin rights and widget records
    entries = job.table(entries_table).filter(
        nf.and_(
            nf.equals("access_type", b"acl_adm"),
            nf.not_(nf.equals("scope", b"widget")),
            nf.not_(nf.equals("subject_id", 363)),  # Yandex group id
        )
    )

    # split by a subject kind; should either be a user or a group
    user_entries, group_entries = entries.split(nf.equals("kind", b"user"), strategy="stop_if_true")

    # for the group grants, map the groups to the users
    group_entries = (
        group_entries.project("entry_id", "scope", "subject_id")
        .join(job.table(groups_table), by_left="subject_id", by_right="group_id")
        .project("entry_id", "scope", "puid")
    )

    # join split tables and split them again, this time by entry scope (dash or [dataset, connection])
    dash_admins, entries_admins = (
        user_entries.project(
            "entry_id",
            "scope",
            "puid",
        )
        .concat(group_entries)
        .unique(*TABLE_SCHEMA)
        .split(nf.equals("scope", b"dash"), strategy="stop_if_true")
    )

    dash_admins.project(*TABLE_SCHEMA).put(dash_admins_dst, schema=TABLE_SCHEMA, ensure_optional=False)
    entries_admins.project(*TABLE_SCHEMA).put(entries_admins_dst, schema=TABLE_SCHEMA, ensure_optional=False)
    job.run()


if __name__ == "__main__":
    args = get_args()
    run(args.entries_table, args.groups_table, args.dash_admins, args.entries_admins)
