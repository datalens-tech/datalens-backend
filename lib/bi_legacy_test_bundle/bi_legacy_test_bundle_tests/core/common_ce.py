from __future__ import annotations

import asyncio
import contextlib
import time
from typing import (
    Any,
    Callable,
    ClassVar,
    Generator,
    List,
    NamedTuple,
    Optional,
    Sequence,
    Type,
)

import attr
import pytest
import sqlalchemy as sa
from sqlalchemy.sql.type_api import TypeEngine

from dl_api_commons.base_models import RequestContextInfo
from dl_constants.enums import (
    BIType,
    ConnectionType,
)
from dl_core import exc
from dl_core.connection_executors import (
    AsyncConnExecutorBase,
    ConnExecutorQuery,
    ExecutionMode,
    SyncWrapperForAsyncConnExecutor,
)
from dl_core.connection_executors.async_sa_executors import DefaultSqlAlchemyConnExecutor
from dl_core.connection_models import (
    ConnDTO,
    ConnectOptions,
    DBIdent,
    DefaultSQLDTO,
    SchemaIdent,
    TableIdent,
)
from dl_core.connections_security.base import (
    ConnectionSecurityManager,
    InsecureConnectionSecurityManager,
)
from dl_core.db import (
    SchemaColumn,
    SchemaInfo,
)
from dl_core.db.native_type import (
    CommonNativeType,
    GenericNativeType,
    norm_native_type,
)
from dl_core.mdb_utils import MDBDomainManagerFactory
from dl_core_testing.database import (
    C,
    make_table,
)
from dl_utils.aio import (
    ContextVarExecutor,
    await_sync,
)


@attr.s(auto_attribs=True)
class SelectDataTestSet:
    query: ConnExecutorQuery
    expected_data: Sequence[Sequence[Any]]


@attr.s(auto_attribs=True)
class ErrorTestSet:
    query: ConnExecutorQuery
    expected_err_cls: Type[Exception]
    expected_message_substring: Optional[str] = None


@attr.s(auto_attribs=True, frozen=True)
class ExecutorOptions:
    exec_mode: Optional[ExecutionMode] = None
    sec_mgr: Optional[ConnectionSecurityManager] = None
    req_ctx_info: Optional[RequestContextInfo] = None
    connect_options: Optional[ConnectOptions] = None
    host_fail_callback: Callable = lambda h: None


@attr.s()
class ExecutorBuilder:
    options: Optional[ExecutorOptions] = attr.ib(factory=ExecutorOptions)
    conn_dto: Optional[ConnDTO] = attr.ib(default=None)
    built_executors: List[AsyncConnExecutorBase] = attr.ib(init=False, factory=list)

    def with_options(self, opts: ExecutorOptions):
        self.options = opts
        return self

    def with_dto(self, dto: ConnDTO):
        self.conn_dto = dto
        return self

    def _build(self) -> AsyncConnExecutorBase:
        raise NotImplementedError()

    def build(self) -> AsyncConnExecutorBase:
        executor = self._build()
        # TODO: For auto-close in fixture
        self.built_executors.append(executor)
        return executor

    def close(self):
        for executor in self.built_executors:
            await_sync(executor.close())


# noinspection PyMethodMayBeStatic
class BaseConnExecutorSupport:
    """Base test class for CE tests with no actual tests"""

    executor_cls: ClassVar[Type[DefaultSqlAlchemyConnExecutor]]

    @staticmethod
    async def wait_for_log_record(caplog, prefix, timeout, pause=0.3):
        start_time = time.monotonic()
        last_record = 0
        while True:
            next_last_record = len(caplog.records)
            records = [record for record in caplog.records[last_record:] if record.message.startswith(prefix)]
            last_record = next_last_record
            if records:
                assert len(records) == 1, f"Unexpected amount of matched records: {records}"
                return records[0]
            if time.monotonic() > start_time + timeout:
                raise Exception("Timed out waiting for the expected record to appear")
            await asyncio.sleep(pause)

    @pytest.fixture()
    def db(self):
        raise NotImplementedError()

    @pytest.fixture()
    def conn_dto(self) -> ConnDTO:
        raise NotImplementedError()

    @pytest.fixture()
    def default_conn_options(self) -> ConnectOptions:
        return ConnectOptions()

    @pytest.fixture()
    def conn_hosts_pool(self, conn_dto) -> Sequence[str]:
        if isinstance(conn_dto, DefaultSQLDTO):
            return tuple(conn_dto.multihosts)
        else:
            return ()

    @pytest.fixture()
    def executor_builder(
        self, loop, conn_dto, conn_hosts_pool, default_conn_options, query_executor_options
    ) -> ExecutorBuilder:
        executor_cls = self.executor_cls

        with ContextVarExecutor() as pool:

            class _Builder(ExecutorBuilder):
                def _build(self) -> AsyncConnExecutorBase:
                    return executor_cls(
                        conn_dto=self.conn_dto or conn_dto,
                        conn_hosts_pool=conn_hosts_pool,
                        conn_options=self.options.connect_options or default_conn_options,
                        tpe=pool,
                        # Params controlled by options
                        req_ctx_info=self.options.req_ctx_info or None,
                        exec_mode=self.options.exec_mode or ExecutionMode.DIRECT,
                        remote_qe_data=query_executor_options,
                        sec_mgr=self.options.sec_mgr or InsecureConnectionSecurityManager(),
                        mdb_mgr=MDBDomainManagerFactory().get_manager(),
                        host_fail_callback=self.options.host_fail_callback,
                    )

            builder = _Builder()
            yield builder
            builder.close()

    @pytest.fixture(params=[ExecutionMode.DIRECT, ExecutionMode.RQE])
    def exec_mode(self, request) -> ExecutionMode:
        return request.param

    @pytest.fixture()
    def executor(self, executor_builder: ExecutorBuilder, exec_mode) -> AsyncConnExecutorBase:
        if exec_mode not in self.executor_cls.supported_exec_mode:
            pytest.skip(f"Mode {exec_mode} is not supported for {self.executor_cls}")

        return executor_builder.with_options(ExecutorOptions(exec_mode=exec_mode)).build()

    @pytest.fixture()
    def sync_exec_wrapper(self, loop, executor) -> SyncWrapperForAsyncConnExecutor:
        wrapper = SyncWrapperForAsyncConnExecutor(
            async_conn_executor=executor,
            loop=loop,
        )
        wrapper.initialize()
        yield wrapper
        wrapper.close()

    # Tables fixtures
    @pytest.fixture(params=["sqla", "plain_text"])
    def select_data_test_set(self, db, request) -> SelectDataTestSet:
        if request.param == "plain_text":
            tbl = make_table(
                db,
                rows=10,
                columns=[
                    C("str_val", BIType.string, vg=lambda rn, **kwargs: str(rn)),
                ],
            )
            yield SelectDataTestSet(
                # table=tbl,
                query=ConnExecutorQuery(
                    query=f"SELECT * FROM {db.quote(tbl.name)} ORDER BY str_val",
                    chunk_size=6,
                ),
                expected_data=[(str(i),) for i in range(10)],
            )
            db.drop_table(tbl.table)
        elif request.param == "sqla":
            tbl = make_table(
                db,
                rows=10,
                columns=[
                    C("str_val", BIType.string, vg=lambda rn, **kwargs: str(rn)),
                ],
            )
            yield SelectDataTestSet(
                # table=tbl,
                query=ConnExecutorQuery(
                    query=sa.select(columns=[sa.column("str_val")])
                    .select_from(sa.text(db.quote(tbl.name)))
                    .order_by(sa.column("str_val")),
                    chunk_size=6,
                ),
                expected_data=[(str(i),) for i in range(10)],
            )
            db.drop_table(tbl.table)
        else:
            raise ValueError(f"Unknown request param: {request.param}")

    # Data selection
    # ################

    @pytest.fixture()
    def error_test_set(self, request) -> ErrorTestSet:
        return ErrorTestSet(
            query=ConnExecutorQuery("SELECT * FROM table_1234123_not_existing"),
            expected_err_cls=exc.DatabaseQueryError,
            expected_message_substring="table_1234123_not_existing",
        )

    # Schema discovery
    # ################

    class CD(NamedTuple):
        cn: str
        sa_type: Optional[TypeEngine]
        # Expected data
        ut_bi_type: BIType
        ut_nullable: bool = True
        nt_name_str: Optional[str] = None
        nt: Optional[GenericNativeType] = None

        def to_sa_col(self):
            return sa.Column(self.cn, self.sa_type)

    def column_data_to_schema_info(self, columns_data: Sequence[CD], conn_type_for_nt: ConnectionType) -> SchemaInfo:
        return SchemaInfo(
            schema=[
                SchemaColumn(
                    name=cd.cn,
                    user_type=cd.ut_bi_type,
                    nullable=cd.ut_nullable,  # to be deprecated
                    native_type=(
                        cd.nt
                        or CommonNativeType(
                            conn_type=conn_type_for_nt,
                            name=norm_native_type(cd.nt_name_str if cd.nt_name_str is not None else cd.sa_type),
                            nullable=cd.ut_nullable,
                        )
                    ),
                )
                for cd in columns_data
            ],
            skipped=None,
        )

    @attr.s(auto_attribs=True)
    class TypeDiscoveryTestCase:
        table_ident: TableIdent
        expected_schema_info: SchemaInfo

    @pytest.fixture()
    def all_supported_types_test_case(self) -> "TypeDiscoveryTestCase":
        raise NotImplementedError()

    @pytest.fixture()
    def index_test_case(self) -> "TypeDiscoveryTestCase":
        raise pytest.skip("No fixture defined")

    # List tables
    # ###########

    @attr.s(auto_attribs=True, frozen=True)
    class ListTableTestCase:
        target_schema_ident: SchemaIdent
        expected_table_names: List[str]
        # If false - result of get_schema_names must contains all schemas, otherwise - should be totally equals
        full_match_required: bool

        def clone(self, **kwargs):
            return attr.evolve(self, **kwargs)

    @contextlib.contextmanager
    def _get_table_names_test_case(
        self,
        db,
        schema_name: Optional[str] = None,
    ) -> Generator[BaseConnExecutorSupport.ListTableTestCase, None, None]:
        tbl_lst = [make_table(db, schema=schema_name) for _ in range(3)]
        test_case = self.ListTableTestCase(
            target_schema_ident=SchemaIdent(db_name=db.name, schema_name=schema_name),
            expected_table_names=[tbl.name for tbl in tbl_lst],
            full_match_required=False,
        )
        try:
            yield test_case
        finally:
            for tbl in tbl_lst:
                db.drop_table(tbl.table)

    @pytest.fixture()
    def get_table_names_test_case(self, db) -> BaseConnExecutorSupport.ListTableTestCase:
        with self._get_table_names_test_case(db) as test_case:
            yield test_case

    # Table existence
    # ###############

    @attr.s(auto_attribs=True, frozen=True)
    class TableExistsTestCase:
        table_ident: TableIdent
        expected_exists: bool

    @pytest.fixture(params=["not_exists", "exists"])
    def is_table_exists_test_case(self, request, db) -> "BaseConnExecutorSupport.TableExistsTestCase":
        param = request.param
        if param == "not_exists":
            yield self.TableExistsTestCase(
                TableIdent(table_name="random_table_name", schema_name=None, db_name=None), False
            )
        elif param == "exists":
            tbl = make_table(db, rows=0, columns=[C("str_val", BIType.integer)])
            yield self.TableExistsTestCase(
                TableIdent(table_name=tbl.name, schema_name=tbl.schema, db_name=tbl.db.name), True
            )
            db.drop_table(tbl.table)
        else:
            raise ValueError(f"Unknown fixture param {param}")


class BaseConnExecutorSet(BaseConnExecutorSupport):
    """Base tests for all CE classes"""

    # Data selection
    # ################

    @pytest.mark.asyncio
    async def test_conn_executor_base(self, executor, select_data_test_set):
        result = await executor.execute(select_data_test_set.query)
        rows = await result.get_all()
        assert rows == select_data_test_set.expected_data

    @pytest.mark.asyncio
    async def test_conn_executor_correct_exc(self, executor, error_test_set):
        with pytest.raises(error_test_set.expected_err_cls) as exc_info:
            await executor.execute(error_test_set.query)

        if error_test_set.expected_message_substring:
            assert error_test_set.expected_message_substring in exc_info.value.message, (
                error_test_set.expected_message_substring,
                exc_info.value.message,
            )

    def test_sync_wrapped_execute(self, sync_exec_wrapper, select_data_test_set):
        result = sync_exec_wrapper.execute(select_data_test_set.query)
        rows = result.get_all()
        assert rows == select_data_test_set.expected_data

    # Schema discovery
    # ################

    @staticmethod
    def _check_type_discovery_test_case(
        sync_executor: SyncWrapperForAsyncConnExecutor,
        test_case: "TypeDiscoveryTestCase",
        match_schema: bool = True,
        match_indexes: bool = False,
        match_all: bool = False,
    ):
        actual_schema_info = sync_executor.get_table_schema_info(
            test_case.table_ident,
        )
        if match_schema or match_all:
            assert actual_schema_info.schema == test_case.expected_schema_info.schema
        if match_indexes or match_all:
            assert actual_schema_info.indexes == test_case.expected_schema_info.indexes
        if match_all:
            assert actual_schema_info == test_case.expected_schema_info

    def test_type_discovery_sync(self, sync_exec_wrapper, all_supported_types_test_case):
        self._check_type_discovery_test_case(sync_exec_wrapper, all_supported_types_test_case)

    def test_indexes_discovery(self, executor_builder, exec_mode, default_conn_options, index_test_case, loop):
        ce = executor_builder.with_options(
            ExecutorOptions(
                connect_options=attr.evolve(
                    default_conn_options,
                    fetch_table_indexes=True,
                ),
                exec_mode=exec_mode,
            )
        ).build()
        sync_ce = SyncWrapperForAsyncConnExecutor(ce, loop=loop)
        self._check_type_discovery_test_case(sync_ce, index_test_case, match_indexes=True, match_schema=False)

    # DB version
    # ##########

    @pytest.mark.asyncio
    async def test_get_db_version(self, executor):
        version = await executor.get_db_version(DBIdent(db_name=None))
        assert isinstance(version, str)

    def test_get_db_version_sync(self, sync_exec_wrapper):
        version = sync_exec_wrapper.get_db_version(DBIdent(db_name=None))
        assert isinstance(version, str)

    # List tables
    # ###########

    @pytest.mark.asyncio
    async def test_get_table_names(self, executor, get_table_names_test_case):
        test_case = get_table_names_test_case
        actual_tables = await executor.get_tables(test_case.target_schema_ident)
        actual_table_names = [tid.table_name for tid in actual_tables]

        if test_case.full_match_required:
            assert sorted(actual_table_names) == sorted(test_case.expected_table_names)
        else:
            assert set(actual_table_names).issuperset(set(test_case.expected_table_names))

    def test_get_table_names_sync(self, sync_exec_wrapper, get_table_names_test_case):
        test_case = get_table_names_test_case
        actual_tables = sync_exec_wrapper.get_tables(test_case.target_schema_ident)
        actual_table_names = [tid.table_name for tid in actual_tables]

        if test_case.full_match_required:
            assert sorted(actual_table_names) == sorted(test_case.expected_table_names)
        else:
            assert set(actual_table_names).issuperset(set(test_case.expected_table_names))

    # Table existence
    # ###############

    @pytest.mark.asyncio
    async def test_table_exists(self, executor, is_table_exists_test_case):
        test_case = is_table_exists_test_case
        actual_exists = await executor.is_table_exists(test_case.table_ident)
        assert actual_exists == test_case.expected_exists

    def test_table_exists_sync(self, sync_exec_wrapper, is_table_exists_test_case):
        test_case = is_table_exists_test_case
        actual_exists = sync_exec_wrapper.is_table_exists(test_case.table_ident)
        assert actual_exists == test_case.expected_exists
