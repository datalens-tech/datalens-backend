from __future__ import annotations

import logging
from typing import (
    TYPE_CHECKING,
    ClassVar,
    Iterable,
    TypeVar,
)

import attr
import grpc
from ydb import DriverConfig
import ydb_dbapi
from ydb.driver import credentials_impl
import ydb.issues as ydb_cli_err

from dl_constants.enums import ConnectionType
from dl_core import exc
from dl_core.connection_models import TableIdent

from dl_connector_ydb.core.base.adapter import YQLAdapterBase
from dl_connector_ydb.core.ydb.constants import (
    CONNECTION_TYPE_YDB,
    YDBAuthTypeMode,
)
from dl_connector_ydb.core.ydb.target_dto import YDBConnTargetDTO


if TYPE_CHECKING:
    from dl_core.connection_models import SchemaIdent


LOGGER = logging.getLogger(__name__)


_DBA_YDB_BASE_DTO_TV = TypeVar("_DBA_YDB_BASE_DTO_TV", bound=YDBConnTargetDTO)


@attr.s
class YDBAdapterBase(YQLAdapterBase[_DBA_YDB_BASE_DTO_TV]):
    conn_type: ClassVar[ConnectionType] = CONNECTION_TYPE_YDB
    dsn_template: ClassVar[str] = "{dialect}:///ydb/"  # 'yql:///ydb/'

    proto_schema: ClassVar[str] = "grpc"

    def _update_connect_args(self, args: dict) -> None:
        if self._target_dto.auth_type == YDBAuthTypeMode.oauth:
            args.update(auth_token=self._target_dto.password)
        elif self._target_dto.auth_type == YDBAuthTypeMode.password:
            driver_config = DriverConfig(
                endpoint="{}://{}:{}".format(
                    self.proto_schema,
                    self._target_dto.host,
                    self._target_dto.port,
                ),
                database=self._target_dto.db_name,
            )
            args.update(
                credentials=credentials_impl.StaticCredentials(
                    driver_config=driver_config, user=self._target_dto.username, password=self._target_dto.password
                )
            )
        else:
            args.update(credentials=credentials_impl.AnonymousCredentials())

    def get_connect_args(self) -> dict:
        target_dto = self._target_dto
        args = dict(
            endpoint="{}://{}:{}".format(
                self.proto_schema,
                target_dto.host,
                target_dto.port,
            ),
            database=target_dto.db_name,
        )
        self._update_connect_args(args)
        return args

    EXTRA_EXC_CLS = (ydb_dbapi.Error, ydb_cli_err.Error, grpc.RpcError)

    def _list_table_names_i(self, db_name: str, show_dot: bool = False) -> Iterable[str]:
        assert db_name, "db_name is required here"
        db_engine = self.get_db_engine(db_name)
        connection = db_engine.connect()
        try:
            # SA db_engine -> SA connection -> DBAPI connection -> YDB driver
            driver = connection.connection.driver  # type: ignore  # 2024-01-24 # TODO: "DBAPIConnection" has no attribute "driver"  [attr-defined]
            assert driver

            queue = [db_name]
            # Relative paths in `select` are also valid (i.e. "... from `some_dir/some_table`"),
            # so, for visual convenience, remove the db prefix.
            unprefix = db_name.rstrip("/") + "/"
            while queue:
                path = queue.pop(0)
                resp = driver.scheme_client.async_list_directory(path)
                res = resp.result()
                children = [
                    (
                        "{}/{}".format(path, child.name),
                        child,
                    )
                    for child in res.children
                    if show_dot or not child.name.startswith(".")
                ]
                children.sort()
                for full_path, child in children:
                    if child.is_any_table():
                        yield full_path.removeprefix(unprefix)
                    elif child.is_directory():
                        queue.append(full_path)
        finally:
            connection.close()

    def _list_table_names(self, db_name: str, show_dot: bool = False) -> Iterable[str]:
        driver_excs = self.EXTRA_EXC_CLS
        try:
            result = self._list_table_names_i(db_name=db_name, show_dot=show_dot)
            for item in result:
                yield item
        except driver_excs as err:
            raise exc.DatabaseQueryError(db_message=str(err), query="list_directory()") from None

    def _get_tables(self, schema_ident: SchemaIdent) -> list[TableIdent]:
        db_name = schema_ident.db_name
        assert db_name is not None
        return [
            TableIdent(
                schema_name=None,
                db_name=db_name,
                table_name=name,
            )
            for name in self._list_table_names(db_name)
        ]


class YDBAdapter(YDBAdapterBase[YDBConnTargetDTO]):
    pass
