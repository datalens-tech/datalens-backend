"""
Move entries from US to clickhouse to use them in usage tracking
Used as a sandbox resource
Resources dir: /sandbox/projects/datalens/resources
"""

import argparse
from argparse import Namespace

from clickhouse_driver import connect as connect_ch
import psycopg2
from psycopg2 import sql


def get_args() -> Namespace:
    parser = argparse.ArgumentParser(description="Move entries from US to clickhouse to use them in usage tracking")

    group_pg = parser.add_argument_group("postgres")
    group_pg.add_argument("--pg-database", type=str)
    group_pg.add_argument("--pg-user", type=str)
    group_pg.add_argument("--pg-password", type=str)
    group_pg.add_argument("--pg-host", type=str)
    group_pg.add_argument("--pg-port", type=int, default=6432)
    group_pg.add_argument("--pg-table", type=str, default="entries")

    group_ch = parser.add_argument_group("clickhouse")
    group_ch.add_argument("--ch-database", type=str)
    group_ch.add_argument("--ch-user", type=str)
    group_ch.add_argument("--ch-password", type=str)
    group_ch.add_argument("--ch-host", type=str)
    group_ch.add_argument("--ch-port", type=int, default=9440)
    group_ch.add_argument(
        "--cert-path", type=str, default="/usr/local/share/ca-certificates/Yandex/YandexInternalRootCA.crt"
    )

    group_settings = parser.add_argument_group("settings")
    group_settings.add_argument("--app-type", type=str, choices=("int", "ext"), default="int")
    group_settings.add_argument("--env", type=str, choices=("testing", "production"), default="testing")
    group_settings.add_argument("--load-freq", type=str, choices=("once", "scheduled"), default="scheduled")

    args = parser.parse_args()

    return args


def main():
    args = get_args()
    ch_table = f"us_entries_{args.app_type}_{args.env}"
    script_name = f"us_entries_{args.app_type}_{args.env}_to_ch"

    pg_conn = psycopg2.connect(
        database=args.pg_database,
        user=args.pg_user,
        password=args.pg_password,
        host=args.pg_host,
        port=args.pg_port,
    )

    ch_conn = connect_ch(
        database=args.ch_database,
        host=args.ch_host,
        port=args.ch_port,
        user=args.ch_user,
        password=args.ch_password,
        secure=True,
        ca_certs=args.cert_path,
    )
    ch_cur = ch_conn.cursor()

    with pg_conn.cursor() as pg_cur:
        pg_cur.execute("select timestamp 'yesterday', timestamp 'today';")
        dates = pg_cur.fetchone()
        timestamp_yesterday, timestamp_today = dates[0], dates[1]

    ch_cur.execute(
        "select to from dataset_profile.script_runs where script_name = %(script_name)s order by run_date desc limit 1",
        {"script_name": script_name},
    )
    select_from = ch_cur.fetchone()
    if select_from is None:
        select_from = timestamp_yesterday

    full_load_query = sql.SQL(
        """
        select
            encode_id(entry_id),
            case
               when is_deleted = True then inner_meta ->> 'oldDisplayKey'
               else display_key
            end as displayKey
        from {table}
        where
            scope in ('connection', 'dataset', 'widget', 'dash')
        order by entry_id;
    """
    ).format(table=sql.Identifier(args.pg_table))

    scheduled_update_query = sql.SQL(
        """
        select
            encode_id(entry_id),
            case
               when is_deleted = True then inner_meta ->> 'oldDisplayKey'
               else display_key
            end as displayKey
        from {table}
        where
            scope in ('connection', 'dataset', 'widget', 'dash') and
            updated_at between %(select_from)s and timestamp 'today'
        order by entry_id;
    """
    ).format(table=sql.Identifier(args.pg_table))

    with pg_conn.cursor(name="transfer_entries_cursor") as pg_cur:
        if args.load_freq == "once":
            pg_cur.execute(full_load_query, {"pg_table": args.pg_table})
        else:
            pg_cur.execute(scheduled_update_query, {"pg_table": args.pg_table, "select_from": select_from})
        while records := pg_cur.fetchmany(500):
            ch_cur.executemany(
                "INSERT INTO {table} (encoded_entry_id, display_key) VALUES".format(table=ch_table),
                ((record[0], record[1]) for record in records),
            )

    ch_cur.execute(
        "INSERT INTO script_runs VALUES (%(script_name)s, %(run_date)s, %(from)s, %(to)s)",
        {
            "script_name": script_name,
            "run_date": timestamp_today,
            "from": select_from if args.load_freq == "scheduled" else None,
            "to": timestamp_today,
        },
    )

    ch_cur.close()
    ch_conn.close()
    pg_conn.close()


if __name__ == "__main__":
    main()
