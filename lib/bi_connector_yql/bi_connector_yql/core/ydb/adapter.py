from __future__ import annotations

from typing import TYPE_CHECKING, Any, ClassVar, Iterable, List

import json
import logging

import attr
import grpc

import ydb.dbapi as ydb_dbapi
import ydb.issues as ydb_cli_err
from ydb.iam import ServiceAccountCredentials

from bi_constants.enums import ConnectionType

from bi_core import exc
from bi_core.connection_models import TableIdent

from bi_connector_yql.core.ydb.constants import CONNECTION_TYPE_YDB
from bi_connector_yql.core.ydb.target_dto import YDBConnTargetDTO
from bi_connector_yql.core.yql_base.adapter import YQLAdapterBase

if TYPE_CHECKING:
    from bi_core.connection_models import SchemaIdent


LOGGER = logging.getLogger(__name__)


@attr.s
class YDBAdapter(YQLAdapterBase[YDBConnTargetDTO]):
    conn_type: ClassVar[ConnectionType] = CONNECTION_TYPE_YDB
    dsn_template: ClassVar[str] = '{dialect}:///ydb/'  # 'yql:///ydb/'

    def _make_cloud_creds(self, data_s: str) -> Any:
        data = json.loads(data_s)
        creds = ServiceAccountCredentials(
            service_account_id=data['service_account_id'],
            access_key_id=data['id'],
            private_key=data['private_key'],
        )
        return creds

    def get_connect_args(self) -> dict:
        target_dto = self._target_dto
        is_cloud = target_dto.is_cloud
        proto = 'grpcs' if is_cloud else 'grpc'
        args = dict(
            endpoint='{}://{}:{}'.format(
                proto,
                target_dto.host,
                target_dto.port,
            ),
            database=target_dto.db_name,
        )
        if target_dto.iam_token:  # (primarily yacloud) service_account_id auth
            args.update(auth_token=self._target_dto.iam_token)
        elif is_cloud:  # yacloud sa key auth
            args.update(credentials=self._make_cloud_creds(target_dto.password))
        else:  # yateam oauth-token auth
            # Effectively, `credentials=ydb.credentials.AuthTokenCredentials(auth_token)`
            args.update(auth_token=self._target_dto.password)
        return args

    EXTRA_EXC_CLS = (ydb_dbapi.Error, ydb_cli_err.Error, grpc.RpcError)

    def _list_table_names_i(self, db_name: str, show_dot: bool = False) -> Iterable[str]:
        assert db_name, 'db_name is required here'
        db_engine = self.get_db_engine(db_name)
        connection = db_engine.connect()
        try:
            # SA db_engine -> SA connection -> DBAPI connection -> YDB driver
            driver = connection.connection.driver
            assert driver

            queue = [db_name]
            # Relative paths in `select` are also valid (i.e. "... from `some_dir/some_table`"),
            # so, for visual convenience, remove the db prefix.
            unprefix = db_name.rstrip('/') + '/'
            while queue:
                path = queue.pop(0)
                resp = driver.scheme_client.async_list_directory(path)
                res = resp.result()
                children = [
                    (
                        '{}/{}'.format(path, child.name),
                        child,
                    )
                    for child in res.children
                    if show_dot or not child.name.startswith('.')
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
            raise exc.DatabaseQueryError(db_message=str(err), query='list_directory()')

    def _get_tables(self, schema_ident: SchemaIdent) -> List[TableIdent]:
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
