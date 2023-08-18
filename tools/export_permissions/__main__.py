import asyncio
import os
import csv
import sys

import asyncpg
import click
import requests


HOST = 'https://datalens.yandex-team.ru'


def make_links(us_id, scope):
    return {
        'widget': f'{HOST}/wizard/{us_id}',
        'folder': None,
        'connection': f'{HOST}/connections/{us_id}',
        'dataset': f'{HOST}/datasets/{us_id}',
        'dash': f'{HOST}/{us_id}',
    }[scope]


def translate_scope(scope):
    return {
        'widget': 'Chart',
        'folder': 'Dir',
        'connection': 'Connection',
        'dataset': 'Dataset',
        'dash': 'Dashboard',
    }[scope]


def translate_perm_kind(perm_kind):
    return {
        'acl_adm': 'admin',
        'acl_execute': 'execute',
        'acl_edit': 'edit',
        'acl_view': 'view',
    }[perm_kind]


def translate_role_type(role_type):
    return {
        'service': 'service',
        'servicerole': 'service role',
    }[role_type]


async def get_dls_info(pg_host, pg_port, pg_db, pg_user, pg_password, abc_id):
    result = []
    conn = await asyncpg.connect(
        host=pg_host, port=pg_port,
        user=pg_user, password=pg_password,
        database=pg_db,
        ssl=True,
        statement_cache_size=0,
    )
    try:
        subjects = await conn.fetch(
            f"""
                SELECT
                    id,
                    meta -> '_staff_item' ->> 'name' as role,
                    meta ->> 'type' as role_type
                FROM dls_subject
                WHERE kind = 'group'
                    AND (
                        (
                            meta ->> 'type' = 'servicerole'
                            AND meta -> 'parent' ->> '__rlsid' = 'group:svc_{abc_id}'
                        ) OR (
                            meta ->> 'type' = 'service'
                            AND meta ->> '__rlsid' = 'group:svc_{abc_id}'
                        )
                    )
                """,
        )
        for subject in subjects:
            subject_id = subject['id']
            grants = await conn.fetch(
                f"""
                    SELECT perm_kind, node_config_id
                    FROM dls_grant
                    WHERE subject_id = {subject_id}
                        AND active = True
                        AND state = 'active'
                    """
            )
            for grant in grants:
                grant_id = grant['node_config_id']
                nodes = await conn.fetch(
                    f"""
                        SELECT dls_nodes.identifier AS identifier
                        FROM dls_node_config
                        JOIN dls_nodes
                        ON dls_nodes.id = dls_node_config.node_id
                        WHERE dls_node_config.id = {grant_id};
                        """,
                )
                for node in nodes:
                    result.append({
                        'perm_kind': grant['perm_kind'],
                        'role': subject['role'],
                        'role_type': subject['role_type'],
                        'us_id': node['identifier'],
                    })
    finally:
        await conn.close()
    return result


async def get_us_info(us_id, us_host, us_token, ignore_ssl):
    response = requests.get(
        f'{us_host}/private/entries/{us_id}',
        verify=not ignore_ssl,
        headers={
            'x-us-master-token': us_token,
        },
        timeout=10,
    )
    if response.status_code != 200:
        return None
    info = response.json()
    return {
        'scope': info['scope'],
        'title': info['key'],
    }


def prepare_result(record):
    return {
        'abc_id': record['abc_id'],
        'type': translate_scope(record['scope']),
        'title': record['title'],
        'link': make_links(us_id=record['us_id'], scope=record['scope']),
        'permission': translate_perm_kind(record['perm_kind']),
        'role': record['role'],
        'role_type': translate_role_type(record['role_type']),
    }


def prepare_csv(records):
    columns = list(records[0].keys())
    yield columns
    for record in records:
        yield [record[column] for column in columns]


async def _main(pg_host, pg_port, pg_db, pg_user, pg_password, abc_id, us_host, us_token, ignore_ssl):
    all_dls_info = await get_dls_info(pg_host, pg_port, pg_db, pg_user, pg_password, abc_id)
    result = []
    for dls in all_dls_info:
        us = await get_us_info(
            dls['us_id'],
            us_host,
            us_token,
            ignore_ssl,
        )
        if us is None:
            continue
        result.append(
            prepare_result(
                {
                    **dls,
                    **us,
                    **{'abc_id': abc_id},
                }
            ),
        )
    if not result:
        return
    result_csv = prepare_csv(result)
    writer = csv.writer(
        sys.stdout,
        delimiter=',',
        quotechar='"',
        quoting=csv.QUOTE_MINIMAL,
    )
    writer.writerows(result_csv)


@click.command(
    help="""
    Extract permissions from DLS
    and join it with US

    Environment variables (look at README.md):
    PG_PASSWORD
    US_TOKEN
    """
)
@click.option('--pg-host', help='Postgres host')
@click.option('--pg-port', help='Postgres port')
@click.option('--pg-user', help='Postgres user')
@click.option('--pg-db', help='Postgres database name')
@click.option('--abc-id', help='ABC project id')
@click.option('--us-host', help='Datalens United Storage host')
@click.option(
    '--ignore-ssl',
    type=bool,
    default=False,
    help='Skip ssl validation for US connections',
)
def main(pg_host, pg_port, pg_user, pg_db, abc_id, us_host, ignore_ssl):
    loop = asyncio.new_event_loop()
    loop.run_until_complete(
        _main(
            pg_host=pg_host,
            pg_port=pg_port,
            pg_user=pg_user,
            pg_db=pg_db,
            pg_password=os.environ['PG_PASSWORD'],
            abc_id=abc_id,
            us_host=us_host,
            us_token=os.environ['US_TOKEN'],
            ignore_ssl=ignore_ssl,
        ),
    )


if __name__ == '__main__':
    main()
