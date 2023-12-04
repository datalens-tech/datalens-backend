from __future__ import annotations

from urllib.parse import urljoin

from .cli import SyncPromQLClient
from .cursor import Cursor


class Connection:
    dl_promql_cli_cls = SyncPromQLClient

    def __init__(
        self, host, port, username=None, password=None, protocol="http", path=None, cli_cls=None, **conn_kwargs
    ):
        base_url = "{protocol}://{host}:{port}".format(
            protocol=protocol,
            host=host,
            port=port,
        )
        if path is not None:
            if not path.endswith("/"):
                path += "/"
            base_url = urljoin(base_url, path)
        self.cli = self._create_cli(
            cli_cls=(cli_cls or self.dl_promql_cli_cls),
            base_url=base_url,
            username=username,
            password=password,
            **conn_kwargs,
        )

    def cursor(self):
        return Cursor(self)

    def commit(self):
        pass

    def rollback(self):
        pass

    def close(self):
        self.cli.close()

    @staticmethod
    def _create_cli(cli_cls, base_url, username, password, **conn_kwargs):
        return cli_cls(base_url=base_url, username=username, password=password, **conn_kwargs)
