"""
Execute action in clickhouse
Used as a sandbox resource
Resources dir: /sandbox/projects/datalens/resources
"""

import argparse
from argparse import Namespace

from clickhouse_driver import connect as connect_ch


def get_args() -> Namespace:
    parser = argparse.ArgumentParser(description="Execute action in clickhouse")

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
    actions = (
        "optimize_us_entries",
        "optimize_billing_dl_unit",
    )
    group_settings.add_argument("--action", type=str, choices=actions, default="optimize_us_entries")

    args = parser.parse_args()

    return args


def main():
    args = get_args()

    ch_conn = connect_ch(
        database=args.ch_database,
        host=args.ch_host,
        port=args.ch_port,
        user=args.ch_user,
        password=args.ch_password,
        secure=True,
        ca_certs=args.cert_path,
    )

    with ch_conn.cursor() as ch_cur:
        ch_cur.execute("select today();")
        dates = ch_cur.fetchone()
        timestamp_today = dates[0]

        ch_query = "select 1;"
        script_name = "unknown"
        ch_kwargs = {}
        if args.action == "optimize_us_entries":
            script_name = f"optimize_us_entries_{args.app_type}_{args.env}"
            ch_table = f"us_entries_{args.app_type}_{args.env}"
            ch_query = "optimize table {table} on cluster `{{cluster}}`;".format(table=ch_table)
        elif args.action == "optimize_billing_dl_unit":
            script_name = f"optimize_billing_dl_unit_{args.app_type}_{args.env}"
            ch_table = f"billing_dl_unit_{args.app_type}_{args.env}"
            ch_query = "optimize table {table} on cluster `{{cluster}}`;".format(table=ch_table)

        ch_cur.execute(ch_query, ch_kwargs)

        ch_cur.execute(
            "INSERT INTO script_runs VALUES (%(script_name)s, %(run_date)s, %(from)s, %(to)s)",
            {
                "script_name": script_name,
                "run_date": timestamp_today,
                "from": None,
                "to": None,
            },
        )


if __name__ == "__main__":
    main()
