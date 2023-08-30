from __future__ import annotations

import sys
import re
from json.decoder import JSONDecodeError

import requests


def returns_list(f):  # type: ignore  # TODO: fix
    def wrapper(*args, **kwargs):  # type: ignore  # TODO: fix
        return [v['name'] for v in f(*args, **kwargs)['data']]

    return wrapper


class CHHost:
    def __init__(self, host, port, proto, username, password):  # type: ignore  # TODO: fix
        print('Working with {}://{}:{} as {}'.format(proto, host, port, username))
        self.host = host
        self.port = port
        self.proto = proto
        self.username = username
        self.password = password

    def get_host_params_as_dict(self):  # type: ignore  # TODO: fix
        return dict(host=self.host, port=self.port, proto=self.proto,
                    username=self.username, password=self.password)

    @property
    def server_url(self):  # type: ignore  # TODO: fix
        return '{}://{}:{}'.format(self.proto, self.host, self.port)

    def _execute(self, query):  # type: ignore  # TODO: fix
        r = requests.post(
            self.server_url,
            data=query.encode('utf-8'),
            headers={
                'X-Clickhouse-User': self.username,
                'X-Clickhouse-Key': self.password,
            }
        )

        if r.status_code > 200:
            print('ERROR:', r.status_code, r.content)
        r.raise_for_status()
        try:
            return r.json()
        except JSONDecodeError:
            pass

    @returns_list
    def get_databases(self):  # type: ignore  # TODO: fix
        return self._execute('SHOW DATABASES FORMAT JSON')

    @returns_list
    def get_tables(self, db_name):  # type: ignore  # TODO: fix
        return self._execute('SHOW TABLES FROM {} FORMAT JSON'.format(db_name))

    def table_exists(self, db_name, table_name):  # type: ignore  # TODO: fix
        data = self._execute('EXISTS TABLE {}.{}'.format(db_name, table_name))
        return data == 1

    def get_create_table_statement(self, db_name, table_name):  # type: ignore  # TODO: fix
        data = self._execute('SHOW CREATE TABLE {}.{} FORMAT JSON'.format(db_name, table_name))['data']
        return data[0]['statement']

    def get_table_engine(self, db_name, table_name):  # type: ignore  # TODO: fix
        statement = self.get_create_table_statement(db_name, table_name)
        return re.search(r'ENGINE = ([^(]+)', statement).group(1)  # type: ignore  # TODO: fix

    def copy_table_to_another_host(self, db_name, table_name, dst_ch):  # type: ignore  # TODO: fix
        print('Copying table {}.{} from {} to {}'.format(db_name, table_name, self.host, dst_ch.host))
        create_table_statement = self.get_create_table_statement(db_name, table_name)
        dst_ch._execute(create_table_statement)
        dst_ch._execute(
            '''INSERT INTO {db_name}.{table_name}
               SELECT * FROM remote(\'{src_host}\', {db_name}.{table_name})'''.format(
                db_name=db_name,
                table_name=table_name,
                src_host=self.host
            )
        )

    def drop_database(self, db_name):  # type: ignore  # TODO: fix
        self._execute('DROP DATABASE {}'.format(db_name))

    def drop_table(self, db_name, table_name):  # type: ignore  # TODO: fix
        self._execute('DROP TABLE {}.{}'.format(db_name, table_name))


def main():  # type: ignore  # TODO: fix
    ch = CHHost(*sys.argv[1:])

    dst_host_params = {
        **ch.get_host_params_as_dict(),
        'host': 'vla-8ezlclo7cpkbuctd.db.yandex.net'
    }

    dst_ch = CHHost(**dst_host_params)

    DB_NAME = 'RPTVdbzDEYPoweAbaRWkjLAxiNGdzpIE'

    for table_name in ch.get_tables(DB_NAME):
        engine = ch.get_table_engine(DB_NAME, table_name)

        if engine != 'ReplicatedMergeTree' and not dst_ch.table_exists(DB_NAME, table_name):
            ch.copy_table_to_another_host(DB_NAME, table_name, dst_ch)


if __name__ == '__main__':
    main()
