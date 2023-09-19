"""
Copy US entries from ClickHouse into YT
Used as a sandbox resource
Resources dir: /sandbox/projects/datalens/resources
"""

import argparse
from argparse import Namespace
import logging

from clickhouse_driver import connect as connect_ch
import yt.wrapper


logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S %Z"
)
LOGGER = logging.getLogger(__name__)


@yt.wrapper.yt_dataclass
class USEntryYTRecord:
    encoded_entry_id: str
    display_key: str


def get_args() -> Namespace:
    parser = argparse.ArgumentParser(description="Copy US entries from ClickHouse into YT")

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

    group_yt = parser.add_argument_group("yt")
    group_yt.add_argument("--yt-dir-path", type=str)

    args = parser.parse_args()

    return args


def main():
    args = get_args()
    ch_table = f"us_entries_{args.app_type}_{args.env}"
    yt_table = args.yt_dir_path.rstrip("/") + "/" + ch_table
    tmp_yt_table = args.yt_dir_path.rstrip("/") + "/_" + ch_table + "_tmp"

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

    yt_client = yt.wrapper.YtClient(proxy="hahn")

    ch_cur.execute(
        "select encoded_entry_id, display_key from dataset_profile.{table} order by encoded_entry_id".format(
            table=ch_table
        ),
    )

    LOGGER.info(f'Running preliminary erase on tmp YT table "{tmp_yt_table}"')
    yt_client.run_erase(table=tmp_yt_table)

    while records := ch_cur.fetchmany(100_000):
        yt_records = [USEntryYTRecord(record[0], record[1]) for record in records]
        LOGGER.info(f'Going to write {len(yt_records)} records to tmp YT table "{tmp_yt_table}"')
        yt_client.write_table_structured(
            "<append=%true>" + tmp_yt_table,
            USEntryYTRecord,
            yt_records,
        )

    LOGGER.info(f'Running preliminary erase on YT table "{yt_table}" to replace data')
    yt_client.run_erase(table=yt_table)

    LOGGER.info(f'Going to merge tmp table "{tmp_yt_table}" into the main one "{yt_table}"')
    yt_client.run_merge(
        source_table=[tmp_yt_table],
        destination_table=yt_table,
        mode="sorted",
    )

    LOGGER.info(f'Going to erase tmp table "{yt_table}')
    yt_client.run_erase(table=tmp_yt_table)

    ch_cur.close()
    ch_conn.close()


if __name__ == "__main__":
    main()
