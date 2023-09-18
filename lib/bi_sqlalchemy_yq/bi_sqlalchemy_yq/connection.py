from __future__ import absolute_import

from bi_sqlalchemy_yq.cli import DLYQClient
from bi_sqlalchemy_yq.cursor import Cursor


class Connection(object):
    pool = None

    dl_yq_cli_cls = DLYQClient

    def __init__(self, host, port, password, database, username=None, cli_cls=None, **conn_kwargs):
        self.host = host
        self.port = port
        self.database = database
        self._conn_kwargs = conn_kwargs
        # Ignoring the `username`
        self.cli = self._create_cli(
            cli_cls=(cli_cls or self.dl_yq_cli_cls),
            host=host,
            port=port,
            password=password,
            database=database,
            **conn_kwargs,
        )

    def cursor(self):
        return Cursor(self)

    def execute(self, sql, parameters=None):
        return self.cursor().execute(sql, parameters)

    def executemany(self, sql, parameters):
        return self.cursor().executemany(sql, parameters)

    def describe(self, table_path):
        raise Exception("TODO")

    def check_exists(self, table_path):
        raise Exception("TODO")

    def commit(self):
        pass

    def rollback(self):
        pass

    def close(self):
        self.cli.close()

    @staticmethod
    def _create_endpoint(host, port):
        return "%s:%d" % (host, port)

    @staticmethod
    def _create_cli(cli_cls, host, port, password, database, **conn_kwargs):
        return cli_cls.create(
            endpoint=f"{host}:{port}",
            bearer_token=password,
            database_name=database,
            # effectively required: cloud_id, folder_id
            **conn_kwargs,
        )
