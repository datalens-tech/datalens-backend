#!/usr/bin/env python3

from __future__ import annotations

import csv
import json
import sys

import numpy as np
import pandas as pd
import requests

CH_HOST = 'vla-8ezlclo7cpkbuctd.db.yandex.net'
CH_USER = 'samples_user'
CH_PASSWORD = 'ilovesamplessomuch'
CH_CLUSTER = 'ya_bi_cluster'
CH_DATABASE = 'samples'
CH_TABLE_NAME = 'DatalensExtract'
CH_URL = f'https://{CH_HOST}:8443/?database={CH_DATABASE}'

DT_FMT = '%d.%m.%Y %H:%M:%S'
DT_COLS = ('Дата рождения', 'Дата заказа', 'Дата доставки')
DATE_COLS = ('Дата рождения',)
INT_COLS = (
    'Document Index (generated)', 'Number of Records', 'orders-count',
    'orders.Index (generated)', 'Количество позиций в заказе',
    'orders.Value.goods.Index (generated)', 'ID Продукта')
RUFLOAT_COLS = (
    'Адрес доставки. Долгота', 'Адрес доставки. Широта', 'Адрес магазина. Долгота',
    'Адрес магазина. Широта', 'Сумма скидки', 'Сумма покупки',
    'Итоговая сумма с учетом скидки', 'Цена продукта')
CHTYPE_OVERRIDES_EXTRA = {
    # Below 1970, cannot be stored as date / datetime:
    'Дата рождения': 'Nullable(String)',
}


DTYPEMAP = {
    np.dtype('<M8[ns]'): 'DateTime',
    np.dtype('O'): 'String',
    np.dtype('float64'): 'Float64',
    np.dtype('int64'): 'Int64',
}


def req_ch(query, _echo=True, **kwargs):
    if _echo:
        print(f"req: {query!r}")
        print("")

    full_kwargs = {}
    full_kwargs.update(
        url=CH_URL,
        headers={
            'X-ClickHouse-User': CH_USER,
            'X-ClickHouse-Key': CH_PASSWORD,
        },
        # verify='/usr/share/yandex-internal-root-ca/YandexInternalRootCA.crt',
    )
    full_kwargs.update(kwargs)

    params = full_kwargs.get('params') or {}
    params['distributed_ddl_task_timeout'] = 3600
    params['allow_experimental_data_skipping_indices'] = 1

    if len(query) > 4096 and full_kwargs.get('data') is None:
        full_kwargs['data'] = query
    else:  # elif len(query) < 4096 or full_kwargs.get('data') is not None:
        params['query'] = query

    full_kwargs['params'] = params

    resp = requests.post(**full_kwargs)
    if _echo:
        print(" ... {} msec".format(int(resp.elapsed.total_seconds() * 1000)))
    if not resp.ok:
        raise Exception("req_ch error", resp, resp.text)
    resp.raise_for_status()
    return resp.content


def req_ch_df(query, **kwargs):
    response = req_ch(query + ' FORMAT JSONCompact', **kwargs)
    resp_data = json.loads(response)
    result = pd.DataFrame(
        resp_data['data'],
        columns=[col['name'] for col in resp_data['meta']],
        # dtypes=...
    )
    result.__dict__['_meta'] = resp_data
    return result


def quote_column(name):
    # A simplified form under the assumption there's no useful doublequotes.
    return '"{}"'.format(name.replace('"', '').replace('\\', ''))


def read_data(filename):
    print("read_data()...")
    with open(filename, encoding='utf-16') as fobj:
        data = fobj.read()
    data = data.strip('\n').splitlines()
    data = [row.split('\t') for row in data]
    columns = data[0]
    data = data[1:]

    df = pd.DataFrame(data, columns=columns)
    return df



def prepare_data(
        df, dt_fmt=DT_FMT, dt_cols=DT_COLS, date_cols=DATE_COLS,
        int_cols=INT_COLS, rufloat_cols=RUFLOAT_COLS,
        auto_int_cols=False, auto_rufloat_cols=False):

    chtype_overrides = {}

    if auto_int_cols:
        int_cols = []
        for col in df.columns:
            if col in dt_cols:
                continue
            if df.dtypes[col] != object:
                continue
            try:
                df[col].map(int)
            except Exception:
                continue
            int_cols.append(col)

    if auto_rufloat_cols:
        rufloat_cols = []
        for col in df.columns:
            if col in dt_cols:
                continue
            if col in int_cols:
                continue
            if df.dtypes[col] != object:
                continue
            try:
                df[col].str.replace(',', '.').astype(float)
            except Exception:
                continue
            rufloat_cols.append(col)

    for col in dt_cols:
        df[col] = pd.to_datetime(df[col], format=dt_fmt, errors='coerce')
        if df[col].isnull().any():
            df[col] = df[col].fillna('0000-00-00 00:00:00')
            chtype_overrides[col] = 'Nullable(DateTime)'

    for col in date_cols:
        df[col] = df[col].map(lambda ts: ts.date().isoformat())
        chtype_overrides[col] = 'Nullable(Date)'

    for col in rufloat_cols:
        df[col] = df[col].str.replace(',', '.').astype(float)

    for col in int_cols:
        df[col] = df[col].astype(int)

    return df, chtype_overrides


def make_create_table_sql(df, chtype_overrides, ch_database=CH_DATABASE, ch_table_name=CH_TABLE_NAME, ch_cluster=CH_CLUSTER):

    def column_type(df, col):
        type_name = chtype_overrides.get(col) or DTYPEMAP[df.dtypes[col]]
        has_nulls = df[col].isnull().any()
        if has_nulls:
            type_name = f'Nullable({type_name})'
        return type_name

    column_to_type = {
        col: column_type(df, col)
        for col in df.columns
    }

    columns_s = ',\n'.join(
        f'{quote_column(col)} {column_to_type[col]}'
        for col in df.columns
    )
    columns_order = ', '.join(
        quote_column(col)
        for col in df.columns
        if not df[col].isnull().any() and
        not column_to_type[col].startswith('Nullable'))
    create_table_sql = f'''
    CREATE TABLE {ch_database}.{ch_table_name}
    ON CLUSTER {ch_cluster}
    (
    {columns_s}
    )
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/{{shard}}/{ch_table_name}', '{{replica}}')
    PARTITION BY tuple()
    ORDER BY ({columns_order})
    SETTINGS index_granularity = 8192
    '''
    return create_table_sql


def main(dry_run=False):
    filename = sys.argv[1]
    df = read_data(filename)

    auto_int_cols = False
    auto_rufloat_cols = False

    df, chtype_overrides = prepare_data(df)
    chtype_overrides.update(CHTYPE_OVERRIDES_EXTRA)

    create_table_sql = make_create_table_sql(df, chtype_overrides)

    if not dry_run:
        req_ch(f'''DROP TABLE IF EXISTS {CH_DATABASE}.{CH_TABLE_NAME} ON CLUSTER {CH_CLUSTER}''')
        req_ch(create_table_sql)

    # df.to_json('/home/hhell/bi-common/datalens_extract.json', orient='records', lines=True)
    # proper_csv_filename = '/home/hhell/bi-common/datalens_extract_proper.csv'
    proper_csv_filename = '_datalens_extract_proper.csv'
    df.to_csv(proper_csv_filename, index=False, quoting=csv.QUOTE_ALL)
    if not dry_run:
        req_ch(f'''INSERT INTO {CH_DATABASE}.{CH_TABLE_NAME} FORMAT CSVWithNames''', data=open(proper_csv_filename, 'rb'))

    return locals()


def debugs():
    # from pyaux.madness import IPNBDFDisplay as H

    def H(df, **kwargs):
        print(df.to_string())

    stuff = main(dry_run=True)
    df = stuff['df']

    x = df.sample(3).transpose()
    x['dtype'] = df.dtypes
    print("Parsed data sample")
    H(x)

    print('select 1')
    print(req_ch('select 1 format JSON'))

    print('casts and types')
    H(req_ch_df('''
    select
        1 as stuff
        , 'b' as strstuff
        , toDate('2019-01-01')
        , toDate('1958-08-04')
    '''))

    print('count by birth year')
    H(req_ch_df('''
    select
        substring("Дата рождения", 1, 4) as age_year
        , any("Дата рождения")
        , any("ID Заказа")
        , count(1) as cnt
    from samples.DatalensExtract
    group by age_year
    order by age_year
    ''').sample(10))

    H(req_ch_df('''
    select *
    from samples.DatalensExtract
    where "Дата рождения" > '2000-01-01 00:00:00'
    limit 10
    '''))


def make_empty_table():
    req_ch(f'''DROP TABLE IF EXISTS {CH_DATABASE}.Empty ON CLUSTER {CH_CLUSTER}''')
    req_ch(f'''
    CREATE TABLE {CH_DATABASE}.Empty
    ON CLUSTER {CH_CLUSTER}
    (name String, value UInt64)
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/{{shard}}/Empty', '{{replica}}')
    PARTITION BY tuple()
    ORDER BY (name)
    SETTINGS index_granularity = 8192
    ''')


if __name__ == '__main__':
    main()
