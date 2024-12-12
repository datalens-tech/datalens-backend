from __future__ import annotations

import contextlib
import typing

import attr

from dl_core.connection_executors.adapters.adapters_base_sa_classic import BaseClassicAdapter
from dl_core.connectors.base.error_transformer import DbErrorTransformer

from dl_connector_mysql.core.adapters_base_mysql import BaseMySQLAdapter
from dl_connector_mysql.core.error_transformer import sync_mysql_db_error_transformer
from dl_connector_mysql.core.target_dto import MySQLConnTargetDTO


@attr.s()
class MySQLAdapter(BaseMySQLAdapter, BaseClassicAdapter[MySQLConnTargetDTO]):
    execution_options = {
        "stream_results": True,
    }
    _error_transformer: typing.ClassVar[DbErrorTransformer] = sync_mysql_db_error_transformer

    def get_connect_args(self) -> dict:
        connect_args = dict(
            super().get_connect_args(),
            charset="utf8",
            local_infile=0,
        )

        if self._target_dto.ssl_enable:
            connect_args["ssl"] = (
                {
                    "ca": self.get_ssl_cert_path(self._target_dto.ssl_ca),
                }
                if self._target_dto.ssl_ca
                else {
                    "ssl_check_hostname": False,
                }
            )

        return connect_args

    @contextlib.contextmanager
    def execution_context(self) -> typing.Generator[None, None, None]:
        contexts: list[typing.ContextManager[None]] = [super().execution_context()]

        if self._target_dto.ssl_ca is not None:
            contexts.append(self.ssl_cert_context(self._target_dto.ssl_ca))

        with contextlib.ExitStack() as stack:
            for context in contexts:
                stack.enter_context(context)
            try:
                yield
            finally:
                stack.close()
