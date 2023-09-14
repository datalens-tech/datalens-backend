#!/usr/bin/env python3

from __future__ import annotations

import os
from urllib.parse import urlencode

import requests
import sqlalchemy as sa

from bi_sqlalchemy_chyt import CHYTTablesRange


def get_engine(fmt="JSONCompact"):
    token = open(os.path.expanduser("~/.yt/token")).read().strip()

    resp = requests.get("http://hahn.yt.yandex.net/hosts", timeout=15)
    resp.raise_for_status()
    hosts = resp.json()
    host = hosts[0]

    dsn_template = "{dialect}://{user}:{passwd}@{host}:{port}/{db_name}"
    dsn_base = dsn_template.format(
        dialect="chyt",
        user="default",
        passwd=token,
        host=host,
        port=80,
        db_name="*ch_datalens",
    )

    params = dict(
        endpoint="query",
        format=fmt,
        timeout=300,
        connect_timeout=3,
        statuses_to_retry="500,502,504",
    )
    dsn = "{}?{}".format(dsn_base, urlencode(params))

    connect_args = {
        "ch_settings": {
            "join_use_nulls": 1,
            "distributed_ddl_task_timeout": 300,
        },
    }
    execution_options = {
        # 'stream_results': True,
    }
    engine = sa.create_engine(
        dsn,
        connect_args=connect_args,
        execution_options=execution_options,
    )
    return engine


def main_a():
    engine = get_engine()
    table = CHYTTablesRange(
        directory="//statbox/home/dmifedorov/bi_test_data/chyt_table_func__bi_418",
        start="table04z",
        end="table07",
    )
    inspection = sa.inspect(engine)
    table_columns = inspection.get_columns(table, schema=None)
    print("Columns:", table_columns)


def main_b():
    from pyaux.runlib import init_logging

    init_logging(level=1)

    engine = get_engine(fmt="default")
    version = engine.scalar("select version() format TabSeparatedWithNamesAndTypes")
    assert version

    engine = get_engine(fmt="JSONCompact")
    version = engine.scalar("select version() FORMAT JSONCompact")
    assert version

    engine = get_engine()
    res = engine.execute("select '' as val WITH TOTALS FORMAT JSONCompact")
    cursor = res.cursor
    res_items = list(res)
    assert cursor.ch_stats
    assert cursor.ch_totals
    print(res_items)
    assert res_items


if __name__ == "__main__":
    main_b()
