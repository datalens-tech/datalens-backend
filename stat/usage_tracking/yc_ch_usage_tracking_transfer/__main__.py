"""
Transfer a new table for the usage tracking from YT to CH
Used as a sandbox resource
Resources dir: /sandbox/projects/datalens/resources
"""

import argparse
import json
import logging
import os
import time

from clickhouse_driver import connect as connect_ch
import requests

logging.basicConfig(level=logging.INFO)
LOGGER = logging.getLogger(__name__)


DATA_TRANSFER_ENDPOINT = "https://cdc.n.yandex-team.ru"
IAM_TOKEN_URL = "https://gw.db.yandex-team.ru/iam/v1/tokens"


def get_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="")

    group_ch = parser.add_argument_group("clickhouse")
    group_ch.add_argument("--ch-database", required=True)
    group_ch.add_argument("--ch-user", required=True)
    group_ch.add_argument("--ch-password", required=True)
    group_ch.add_argument("--ch-host", required=True)
    group_ch.add_argument("--ch-cluster")
    group_ch.add_argument("--ch-port", type=int, default=9440)
    group_ch.add_argument("--cert-path", default="/usr/local/share/ca-certificates/Yandex/YandexInternalRootCA.crt")

    group_dt = parser.add_argument_group("data-transfer")
    group_dt.add_argument("--transfer-id", required=True, help="Yandex Data Transfer resource id")
    group_dt.add_argument("--transfer-timeout", type=int, default=1800, help="Timeout for the transfer operation")
    group_dt.add_argument("--table-size", type=int, required=True, help="YT table size")
    group_dt.add_argument(
        "--partition-to-move",
        required=True,
        help="Name of partition of a tmp table to move to the usage tracking table, in YYYYMM format",
    )
    group_dt.add_argument(
        "--partition-to-drop", help="Name of partition to drop from the usage tracking table, in YYYYMM format"
    )
    group_dt.add_argument("--usage-tracking-table", default="usage_tracking", help="Name of the usage tracking table")
    group_dt.add_argument(
        "--tmp-table",
        default="usage_tracking_tmp",
        help="Name of the tmp table (i.e. the destination for the transfer)",
    )

    args = parser.parse_args()

    return args


def get_iam_token() -> str:
    oauth_token = os.environ["DATA_TRANSFER_TOKEN"]
    resp = requests.post(IAM_TOKEN_URL, json={"yandexPassportOauthToken": oauth_token}, verify=False)
    resp.raise_for_status()
    return resp.json()["iamToken"]


def parse_data_transfer_request(resp: requests.Response) -> dict:
    resp.raise_for_status()
    json_resp = resp.json()
    if "error" in json_resp:
        raise RuntimeError(f"Found an error in request {resp.url}: {json.dumps(json_resp, indent=4)}")
    return json_resp


def run_transfer(args: argparse.Namespace, token: str) -> None:
    headers = {"Authorization": f"Bearer {token}"}
    resp = requests.post(
        f"{DATA_TRANSFER_ENDPOINT}/v1/transfer/{args.transfer_id}:activate",
        headers=headers,
        json={"transfer_id": args.transfer_id},
    )  # TODO: check if body is really required
    operation_id = parse_data_transfer_request(resp)["id"]
    LOGGER.info(f"Activated a transfer {args.transfer_id}, launched an operation {operation_id}")

    timeout = int(time.time()) + args.transfer_timeout
    while int(time.time()) < timeout:
        resp = requests.get(f"{DATA_TRANSFER_ENDPOINT}/v1/operation/{operation_id}", headers=headers)
        data = parse_data_transfer_request(resp)
        if data.get("done", False):
            LOGGER.info(f"Operation {operation_id} completed!")
            return

        LOGGER.info(f'Current progress: {data["metadata"]["health_meta"]}')
        time.sleep(10)

    LOGGER.error(f"Operation {operation_id} wasn't complete in {args.transfer_timeout} seconds!")
    raise RuntimeError(f"Data transfer operation {operation_id} timed out")


def update_partitions(args: argparse.Namespace) -> None:
    ch_conn = connect_ch(
        database=args.ch_database,
        host=args.ch_host,
        port=args.ch_port,
        user=args.ch_user,
        password=args.ch_password,
        secure=True,
        ca_certs=args.cert_path,
    )
    on_cluster = f"ON CLUSTER {args.ch_cluster}" if args.ch_cluster else ""

    with ch_conn.cursor() as ch_cur:
        # validate that transfer was fully completed
        ch_cur.execute(f"SELECT count() FROM {args.tmp_table}")
        actual_table_size = ch_cur.fetchone()[0]
        if actual_table_size != args.table_size:
            raise ValueError(
                f"Table sizes mismatch: " f"YT table has {args.table_size} rows but CH table has {actual_table_size}"
            )

        # move partition from tmp table to the full one
        query = (
            f"ALTER TABLE {args.tmp_table} {on_cluster} MOVE PARTITION {args.partition_to_move} "
            f"TO TABLE {args.usage_tracking_table}"
        )
        LOGGER.info(f"Executing query: {query}")
        ch_cur.execute(query)

        # check if all data from tmp table is moved to the full one
        ch_cur.execute(f"SELECT count() FROM {args.tmp_table}")
        table_size = ch_cur.fetchone()[0]
        if table_size != 0:
            raise ValueError(f"{args.tmp_table} is not empty after the MOVE PARTITION query! Actual size: {table_size}")

        # drop the outdated partition
        if args.partition_to_drop:
            query = f"ALTER TABLE {args.usage_tracking_table} {on_cluster} DROP PARTITION {args.partition_to_drop}"
            LOGGER.info(f"Executing query: {query}")
            ch_cur.execute(query)


if __name__ == "__main__":
    args = get_args()
    token = get_iam_token()
    run_transfer(args, token)
    update_partitions(args)
