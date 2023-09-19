from __future__ import annotations

import argparse
from collections import (
    Counter,
    defaultdict,
)
from dataclasses import (
    field,
    make_dataclass,
)
from datetime import datetime
import logging
import os
from typing import (
    Iterable,
    Type,
    TypeVar,
)

from clickhouse_driver import connect as connect_ch
from clickhouse_driver.dbapi import Connection
import requests
from yt.wrapper import (
    TablePath,
    YtClient,
    yt_dataclass,
)
from yt.wrapper.schema import Int64


logging.basicConfig(level=logging.INFO)
LOGGER = logging.getLogger(__name__)


STAFF_API_ENDPOINT = "https://staff-api.yandex-team.ru/v3/"
STAFF_API_LIMIT = 900_000
STAFF_DEPARTMENTS_DEPTH = 8

DEPARTMENTS_API_PARAMS = {"_fields": "name,id,parent.id", "type": "department"}
DEPARTMENTS_CH_QUERY = "INSERT INTO {table} (id, parent_id, name, update_time) VALUES"
USERS_API_PARAMS = {"_fields": "login,department_group.id,official.is_dismissed"}
USERS_CH_QUERY = "INSERT INTO {table} (login, department, update_time) VALUES"


@yt_dataclass
class StaffUser:
    username: str
    department_id: Int64
    is_active: bool

    @staticmethod
    def from_dict(staff_user: dict) -> StaffUser:
        return StaffUser(
            username=staff_user["login"].lower(),
            department_id=staff_user.get("department_group", {}).get("id", 0),
            is_active=not staff_user["official"]["is_dismissed"],
        )


DEP_TV = TypeVar("DEP_TV", bound="StaffDepartmentBase")


@yt_dataclass
class StaffDepartmentBase:
    department_id: Int64
    department_name: str
    number_of_users: Int64

    @classmethod
    def from_dict(cls: Type[DEP_TV], staff_department: dict, users_per_dep: dict[int, int]) -> DEP_TV:
        return cls(
            department_id=staff_department["id"],
            department_name=staff_department["name"],
            number_of_users=users_per_dep.get(staff_department["id"], 0),
        )


# create full schema dynamically to avoid listing all STAFF_DEPARTMENTS_DEPTH levels of departments
StaffDepartment = make_dataclass(
    "StaffDepartment",
    [(f"department_level{i}", Int64, field(default=0)) for i in range(1, STAFF_DEPARTMENTS_DEPTH + 1)]
    + [(f"department_level{i}_name", str, field(default="Остальные")) for i in range(1, STAFF_DEPARTMENTS_DEPTH + 1)],
    bases=(StaffDepartmentBase,),
)


SCHEMA_TV = TypeVar("SCHEMA_TV", StaffUser, StaffDepartment)


def get_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="")
    parser.add_argument("--timestamp", type=datetime.fromisoformat)

    group_ch = parser.add_argument_group("clickhouse")
    group_ch.add_argument("--ch-user", type=str)
    group_ch.add_argument("--ch-password", type=str)
    group_ch.add_argument("--ch-host", type=str)
    group_ch.add_argument("--ch-port", type=int, default=9440)
    group_ch.add_argument("--ch-users-table", type=str)
    group_ch.add_argument("--ch-departments-table", type=str)
    group_ch.add_argument(
        "--cert-path", type=str, default="/usr/local/share/ca-certificates/Yandex/YandexInternalRootCA.crt"
    )

    group_yt = parser.add_argument_group("yt")
    group_yt.add_argument("--yt-cluster", type=str, default="hahn")
    group_yt.add_argument("--yt-users-table", type=str)
    group_yt.add_argument("--yt-departments-table", type=str)

    args = parser.parse_args()
    return args


def get_yt_client(yt_cluster: str) -> YtClient:
    yt_client = YtClient(proxy=yt_cluster)
    # https://st.yandex-team.ru/YT-17234
    transaction = os.environ.get("YT_TRANSACTION")
    if transaction is not None:
        yt_client.COMMAND_PARAMS["transaction_id"] = transaction
    return yt_client


def get_staff_data(url: str, params: dict[str, str], token: str) -> list[dict]:
    LOGGER.info(f"Reading {url} from staff: {params}")
    params["_limit"] = str(STAFF_API_LIMIT)
    response = requests.get(STAFF_API_ENDPOINT + url, params=params, headers={"Authorization": f"OAuth {token}"})
    response.raise_for_status()

    rows = response.json()["result"]
    assert isinstance(rows, list)
    assert len(rows) <= STAFF_API_LIMIT
    LOGGER.info(f"Received {len(rows)} rows from staff")
    return rows


def fill_department_data(departments: dict[int, StaffDepartment], parents: dict[int, int]) -> None:
    """
    Fill number_of_users and department_levels for each department
    Not optimal, but close to it (considering each department has a constant number of parents)
    and way easier to implement
    """

    # map: department id -> list of all parent departments (oneself, parent, parent of parent, ...)
    all_parents = defaultdict(list)
    for id in departments:
        cur_id = id
        while cur_id in departments:
            all_parents[id].append(cur_id)
            cur_id = parents[cur_id]

    # sort by amount of parents; this way child departments will have
    # their full capacity counted before parents
    for id, department in sorted(departments.items(), key=lambda x: len(all_parents[x[0]])):
        for i, parent_id in enumerate(reversed(all_parents[id])):
            parent_department = departments[parent_id]
            if id != parent_id:
                parent_department.number_of_users += department.number_of_users
            if i < STAFF_DEPARTMENTS_DEPTH:
                setattr(department, f"department_level{i + 1}", parent_id)
                setattr(department, f"department_level{i + 1}_name", parent_department.department_name)


def write_to_clickhouse(ch_conn: Connection, query: str, data: list[tuple]) -> None:
    LOGGER.info(f"Executing CH query: {query}")
    with ch_conn.cursor() as ch_cur:
        ch_cur.executemany(query, data)
    LOGGER.info("Finished writing to CH")


def write_to_yt(
    yt_client: YtClient, table: str | TablePath, table_schema: Type[SCHEMA_TV], data: Iterable[SCHEMA_TV]
) -> None:
    LOGGER.info(f"Writing data to YT: {table}")
    yt_client.write_table_structured(table, table_schema, data)
    LOGGER.info("Finished writing to YT")


if __name__ == "__main__":
    args = get_args()
    staff_token = os.environ["STAFF_TOKEN"]

    ch_conn = connect_ch(
        host=args.ch_host,
        port=args.ch_port,
        user=args.ch_user,
        password=args.ch_password,
        secure=True,
        ca_certs=args.cert_path,
    )
    yt_client = get_yt_client(args.yt_cluster)

    staff_users = get_staff_data("persons", params=USERS_API_PARAMS, token=staff_token)
    users = [StaffUser.from_dict(user) for user in staff_users]
    users_per_dep = Counter(user.department_id for user in users if user.is_active)
    users_ch_data = [(user.username, user.department_id, args.timestamp) for user in users]
    write_to_clickhouse(ch_conn, query=USERS_CH_QUERY.format(table=args.ch_users_table), data=users_ch_data)
    write_to_yt(yt_client, table=args.yt_users_table, table_schema=StaffUser, data=users)

    staff_departments = get_staff_data("groups", params=DEPARTMENTS_API_PARAMS, token=staff_token)
    departments = {
        department["id"]: StaffDepartment.from_dict(department, users_per_dep) for department in staff_departments
    }
    parents = {department["id"]: department.get("parent", {}).get("id", 0) for department in staff_departments}
    departments_ch_data = [
        (dep.department_id, parents[dep.department_id], dep.department_name, args.timestamp)
        for dep in departments.values()
    ]
    write_to_clickhouse(
        ch_conn, query=DEPARTMENTS_CH_QUERY.format(table=args.ch_departments_table), data=departments_ch_data
    )

    fill_department_data(departments, parents)
    departments_table = TablePath(args.yt_departments_table, sorted_by=["department_id"])
    write_to_yt(yt_client, table=departments_table, table_schema=StaffDepartment, data=departments.values())
