#!/usr/bin/env python3
import contextlib
import json
import secrets
import string
from typing import (
    List,
    Optional,
    Sequence,
    Tuple,
    Union,
)

import attr
import click
from clickhouse_driver import Client as CHClient
from clickhouse_driver.errors import (
    ErrorCodes,
    ServerException,
)
import psycopg2
import psycopg2.extras

try:
    from library.python.vault_client.instances import Production as VaultClient
except ImportError:  # not Tier0 build
    from vault_client.instances import Production as VaultClient


DICT_DB_NAME = "__dl_dictionaries"
DICT_DB_USER_NAME = "dl_dict_mngr"


@attr.s(frozen=True, kw_only=True)
class DBInfo:
    database_name: str = attr.ib()
    user_name: str = attr.ib()
    password: str = attr.ib()


def random_word(length: int, with_digits=False) -> str:
    chars = string.ascii_letters
    if with_digits:
        chars += string.digits
    return "".join(secrets.choice(chars) for _ in range(length))


def clickhouse_client(host: str, user: str, password: str, port: Optional[int] = None) -> CHClient:
    kwargs = dict(
        host=host,
        user=user,
        password=password,
        secure=True,
    )
    if port:
        kwargs["port"] = port
    return CHClient(**kwargs)


def create_database(client: CHClient, database_name: str, dry_run: bool) -> None:
    query = f"CREATE DATABASE {database_name} ON CLUSTER '{{cluster}}' ENGINE=Atomic"
    if dry_run:
        print("Creating database:", query)
    else:
        client.execute(query)


def create_random_named_database(client: CHClient, random_len: int, dry_run: bool) -> str:
    while True:
        database_name = random_word(random_len)
        try:
            create_database(client, database_name, dry_run)
        except ServerException as e:
            if e.code == ErrorCodes.DATABASE_ALREADY_EXISTS:
                continue
            raise
        else:
            return database_name


def create_user(client: CHClient, user_name: str, password: str, dry_run: bool) -> None:
    query = f"CREATE USER {user_name} ON CLUSTER '{{cluster}}' IDENTIFIED BY '{password}'"
    if dry_run:
        print("Creating user:", query)
    else:
        client.execute(query)


def show_users(client: CHClient) -> List[str]:
    return [row[0] for row in client.execute("SHOW USERS")]


def create_random_named_user(client: CHClient, random_len: int, dry_run: bool) -> Tuple[str, str]:
    password = random_word(random_len)
    while True:
        user_name = random_word(random_len)
        try:
            create_user(client, user_name, password, dry_run)
        except ServerException as e:
            if e.code == 493:  # user already exists
                continue
            raise
        else:
            return user_name, password


def grant_permission(client: CHClient, database_name: str, user_name: Union[str, Sequence[str]], dry_run: bool) -> None:
    if not isinstance(user_name, str):
        user_name = ", ".join(user_name)
    query = f"GRANT ON CLUSTER '{{cluster}}' ALL ON {database_name}.* TO {user_name}"
    if dry_run:
        print("Grant permission:", query)
    else:
        client.execute(query)


def slice_in_chunks(l: List, n: int) -> List:
    return [l[i : i + n] for i in range(0, len(l), n)]


def create_secret_with_passwords(client: VaultClient, secret_name: str, infos: List[DBInfo], dry_run: bool) -> str:
    secret_version = {"value": {info.user_name: info.password for info in infos}}
    roles = [
        {"login": "robot-datalens-back", "role": "OWNER"},
        {"abc_id": 15898, "abc_scope_id": 84, "role": "OWNER"},  # DevOps of YC DataLens
    ]
    if dry_run:
        print("Create secret", secret_name, "with version")
        print(json.dumps(secret_version, indent=2))
        print("and roles")
        print(json.dumps(roles, indent=2))
        return secret_name
    else:
        res = client.create_complete_secret(secret_name, secret_version=secret_version, roles=roles)
        assert res["status"] == "ok"
        return res["uuid"]


def add_dictionaries_database_info_to_secret(
    client: VaultClient,
    secret_id: str,
    secret_key: str,
    cluster_id: str,
    host: str,
    port: int,
    username: str,
    password: str,
    dry_run: bool,
) -> str:
    version = client.get_version(secret_id)
    try:
        data = json.loads(version["value"][secret_key])
    except KeyError:
        data = []

    if any([d["cluster_id"] == cluster_id for d in data]):
        raise ValueError(f"Data for cluster {cluster_id} already exists")

    data.append(
        {
            "cluster_id": cluster_id,
            "host": host,
            "port": port,
            "username": username,
            "password": password,
        }
    )
    diff = [{"key": secret_key, "value": json.dumps(data)}]
    if dry_run:
        print("Update secret", secret_key, "with diff")
        print(json.dumps(diff, indent=2))
        return version["version"]
    else:
        return client.create_diff_version(version["version"], diff=diff, check_head=True)


def create_passwords_table(conn, table_name: str, dry_run: bool) -> None:
    CREATE_PASSWORDS_TABLE_TMP = (
        "CREATE TABLE {table_name} ("
        "db_name text not null, "
        "user_name text not null, "
        "user_password text not null, "
        "is_used boolean not null, "
        "folder_id text);"
    )
    query = CREATE_PASSWORDS_TABLE_TMP.format(table_name=table_name)
    if dry_run:
        print("Create table:", query)
    else:
        with conn.cursor() as cur:
            cur.execute(query)


def insert_passwords_into_table(conn, table_name: str, infos: List[DBInfo], dry_run: bool) -> None:
    INSERT_PASSWORDS_INTO_TABLE_TMP = "INSERT INTO {table_name} (db_name, user_name, user_password, is_used) VALUES %s;"
    query = INSERT_PASSWORDS_INTO_TABLE_TMP.format(table_name=table_name)
    if dry_run:
        print("Insert query:", query)
    else:
        with conn.cursor() as cur:
            psycopg2.extras.execute_values(
                cur, query, [(info.database_name, info.user_name, "in yav", False) for info in infos]
            )


@click.group()
@click.option("--dry-run", is_flag=True, help="Print to output instead of do actual work")
@click.pass_context
def cli(ctx, dry_run):
    ctx.ensure_object(dict)
    ctx.obj["dry_run"] = dry_run


@cli.command(name="make-dbs", help="Make materialization databases in ClickHouse cluster")
@click.option("--host", required=True, help="ClickHouse host")
@click.option("--port", type=int, help="ClickHouse port")
@click.option("--user", default="admin", help="ClickHouse user", show_default=True)
@click.option("--password-secret", required=True, help="YaV secret ID where ClickHouse password is stored")
@click.option("--password-key", required=True, help="YaV secret key for ClickHouse password")
@click.option("--database-count", default=10, help="How many databases to create", show_default=True)
@click.option("--random-len", default=32, help="Random string length", show_default=True)
@click.option("--output-file", required=True, help="Output JSON file to store generated data")
@click.pass_context
def make_dbs(ctx, host, port, user, password_secret, password_key, database_count, random_len, output_file):
    dry_run = ctx.obj.get("dry_run")

    client = clickhouse_client(host, user, VaultClient().get_version(password_secret)["value"][password_key], port=port)

    infos: List[DBInfo] = []
    for _ in range(database_count):
        database_name = create_random_named_database(client, random_len, dry_run)
        user_name, password = create_random_named_user(client, random_len, dry_run)
        grant_permission(client, database_name, user_name, dry_run)
        infos.append(DBInfo(database_name=database_name, user_name=user_name, password=password))
    infos.sort(key=lambda info: info.database_name)

    with open(output_file, "w") as f:
        json.dump([attr.asdict(info) for info in infos], f, indent=4)
    print("Data stored in", output_file)


@cli.command(name="create-secrets", help="Create secrets with user names and passwords for materialization databases")
@click.option("--input-file", required=True, help="Input JSON file where data is stored")
@click.option("--base-secret-name", required=True, help="Base secret name")
@click.option("--keys-per-secret", default=700, help="Max number of keys in secret", show_default=True)
@click.pass_context
def create_secrets(ctx, input_file, base_secret_name, keys_per_secret):
    dry_run = ctx.obj.get("dry_run")

    with open(input_file, "r") as f:
        infos = [DBInfo(**d) for d in json.load(f)]

    client = VaultClient()
    secrets = []
    for i, chunk in enumerate(slice_in_chunks(infos, keys_per_secret), start=1):
        output_secret_name = "-".join([base_secret_name, str(i)])
        secrets.append(create_secret_with_passwords(client, output_secret_name, chunk, dry_run))
    print("Secrets created:")
    print("\n".join(secrets))


@cli.command(name="save-setup", help="Save passwords in PostgreSQL database")
@click.option("--input-file", required=True, help="Input JSON file where data is stored")
@click.option("--host", required=True, help="PG DB host")
@click.option("--port", required=True, type=int, help="PG DB port")
@click.option("--dbname", default="setup_folder", help="PG DB name", show_default=True)
@click.option("--user", default="setup_folder", help="PG DB user", show_default=True)
@click.option("--password-secret", required=True, help="YaV secret ID where PG DB password is stored")
@click.option("--password-key", required=True, help="YaV secret key for PG DB password")
@click.option("--target-session-attrs", default="read-write", show_default=True)
@click.option("--sslmode", default="verify-full", show_default=True)
@click.option("--table-name", required=True, help="PG DB table to store data")
@click.pass_context
def save_setup(
    ctx, input_file, host, port, dbname, user, password_secret, password_key, target_session_attrs, sslmode, table_name
):
    dry_run = ctx.obj.get("dry_run")

    with open(input_file, "r") as f:
        infos = [DBInfo(**d) for d in json.load(f)]

    conn = (
        contextlib.nullcontext()
        if dry_run
        else psycopg2.connect(
            host=host,
            port=port,
            dbname=dbname,
            user=user,
            password=VaultClient().get_version(password_secret)["value"][password_key],
            target_session_attrs=target_session_attrs,
            sslmode=sslmode,
        )
    )
    try:
        with conn:
            create_passwords_table(conn, table_name, dry_run)
            insert_passwords_into_table(conn, table_name, infos, dry_run)
    except:
        raise
    else:
        print("Created table:", table_name)
    finally:
        if not dry_run:
            conn.close()


@cli.command(name="make-dict-db", help="Make dictionaries database in ClickHouse cluster")
@click.option("--host", required=True, help="ClickHouse host")
@click.option("--port", type=int, help="ClickHouse port")
@click.option("--user", default="admin", help="ClickHouse user", show_default=True)
@click.option("--password-secret", required=True, help="YaV secret ID where ClickHouse password is stored")
@click.option("--password-key", required=True, help="YaV secret key for ClickHouse password")
@click.option("--database-name", default=DICT_DB_NAME, help="Dictionaries database name", show_default=True)
@click.option(
    "--database-user-name", default=DICT_DB_USER_NAME, help="Dictionaries database user name", show_default=True
)
@click.option("--random-len", default=32, help="Random string length", show_default=True)
@click.option(
    "--no-all-users-access", is_flag=True, help="Do not grant dictionaries database access to all created users"
)
@click.pass_context
def make_dict_db(
    ctx,
    host,
    port,
    user,
    password_secret,
    password_key,
    database_name,
    database_user_name,
    random_len,
    no_all_users_access,
):
    dry_run = ctx.obj.get("dry_run")

    client = clickhouse_client(host, user, VaultClient().get_version(password_secret)["value"][password_key], port=port)

    create_database(client, database_name, dry_run)
    password = random_word(random_len, with_digits=True)
    create_user(client, database_user_name, password, dry_run)
    if no_all_users_access:
        grant_permission(client, database_name, database_user_name, dry_run)
    else:
        users = [un for un in show_users(client) if un != "admin"]
        grant_permission(client, database_name, users, dry_run)
    result = DBInfo(database_name=database_name, user_name=database_user_name, password=password)
    print("Dictionaries database created:\n", json.dumps(attr.asdict(result), indent=4))


@cli.command(name="add-dict-db-to-secret", help="Add dictionaries database info to secret")
@click.option("--cluster", required=True, help="ClickHouse cluster ID")
@click.option("--host", required=True, help="ClickHouse host")
@click.option("--port", default=8443, help="ClickHouse port", show_default=True)
@click.option(
    "--database-user-name", default=DICT_DB_USER_NAME, help="Dictionaries database user name", show_default=True
)
@click.option("--database-password", required=True, help="Dictionaries database user password")
@click.option("--secret", required=True, help="YaV secret ID where dictionaries database info is stored")
@click.option("--secret-key", required=True, help="YaV secret key for dictionaries database info")
@click.pass_context
def add_dict_db_to_secret(ctx, cluster, host, port, database_user_name, database_password, secret, secret_key):
    dry_run = ctx.obj.get("dry_run")

    client = VaultClient()
    version_id = add_dictionaries_database_info_to_secret(
        client, secret, secret_key, cluster, host, port, database_user_name, database_password, dry_run
    )
    print(f"Secret {secret} updated, new version is {version_id}")


def main():
    cli(obj={})


if __name__ == "__main__":
    main()
