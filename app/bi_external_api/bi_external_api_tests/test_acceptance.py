from __future__ import annotations

import contextlib
from functools import singledispatchmethod
from typing import Iterable, Iterator, Sequence, Optional, Type, ClassVar

import attr
import pytest
from sqlalchemy import Column
from sqlalchemy.engine.url import URL

from bi_connector_clickhouse.db_testing.engine_wrapper import (
    ClickhouseDbEngineConfig, ClickHouseEngineWrapper, EngineWrapperBase,
)
from bi_external_api.attrs_model_mapper import pretty_repr, Processor
from bi_external_api.attrs_model_mapper.field_processor import FieldMeta
from bi_external_api.domain import external as ext
from bi_external_api.domain.external import CommonError, rpc_dc
from bi_external_api.domain.internal import dashboards
from bi_external_api.ext_examples import SuperStoreExtDSBuilder, CHConnectionBuilder
from bi_external_api.internal_api_clients.main import InternalAPIClients
from bi_external_api.structs.mappings import FrozenMappingStrToStrOrStrSeq
from bi_external_api.testings import WorkbookOpsClient
from bi_external_api.workbook_ops.facade import WorkbookOpsFacade
from bi_external_api.workbook_ops.public_exceptions import WorkbookOperationException


def clear_volatile_common_error_data(exc_data: ext.ErrWorkbookOp) -> ext.ErrWorkbookOp:
    class VolatileCommonErrorDataCleaner(Processor[ext.CommonError]):
        def _should_process(self, meta: FieldMeta) -> bool:
            return meta.clz == ext.CommonError

        def _process_single_object(self, obj: Optional[ext.CommonError], meta: FieldMeta) -> Optional[ext.CommonError]:
            if obj is not None:
                return attr.evolve(obj, exc_message=None)
            return None

    return VolatileCommonErrorDataCleaner().process(exc_data)


def create_indicator_for_sum_sales(
        *,
        name: str,
        dataset_name: str,
) -> ext.ChartInstance:
    ad_hoc_field_id = "sum_sales"

    return ext.ChartInstance(
        name=name,
        chart=ext.Chart(
            ad_hoc_fields=[ext.AdHocField(field=ext.DatasetField(
                id=ad_hoc_field_id,
                title="Sum Sales",
                calc_spec=ext.FormulaCS(formula="sum([Sales])"),
                cast=ext.FieldType.float,
                description="",
            ))],
            datasets=[dataset_name],
            visualization=ext.Indicator(field=ext.ChartField.create_as_ref(id=ad_hoc_field_id))
        )
    )


@attr.s(auto_attribs=True)
class ConnectionTestingData:
    connection: ext.Connection
    secret: Optional[ext.Secret]

    target_db_name: str

    # Table options
    sample_super_store_table_name: str
    sample_super_store_schema_name: Optional[str] = None
    sample_super_store_sub_select_sql: Optional[str] = None

    # Table fixing option
    perform_tables_validation: bool = attr.ib(default=True)
    perform_tables_fix: bool = attr.ib(default=True)


@attr.s(auto_attribs=True)
class CreatedConnectionTestingData:
    entry_info: ext.EntryInfo
    data: ConnectionTestingData


@attr.s
class DataFiller:
    db_data: ConnectionTestingData = attr.ib()
    ew: EngineWrapperBase = attr.ib(init=False, default=None)

    def __attrs_post_init__(self) -> None:
        self.ew = self.create_ew(self.db_data.connection)

    @singledispatchmethod
    def create_ew(self, conn: ext.Connection) -> EngineWrapperBase:
        raise NotImplementedError()

    @create_ew.register
    def create_ew_ch(self, conn: ext.ClickHouseConnection):
        secret = self.db_data.secret
        assert isinstance(secret, ext.PlainSecret), "Plain secret required to configure EW for ClickHouse"

        return ClickHouseEngineWrapper(
            config=ClickhouseDbEngineConfig(
                url=URL(
                    drivername="clickhouse",
                    username=conn.username,
                    password=secret.secret,
                    host=conn.host,
                    port=conn.port,
                    database=self.db_data.target_db_name,
                    query=dict(
                        protocol="https",
                    )
                ),
                cluster=None,
            )
        )

    @singledispatchmethod
    def sample_superstore_sa_columns(self, conn: ext.Connection) -> list[Column]:
        raise NotImplementedError()

    @sample_superstore_sa_columns.register
    def sample_superstore_sa_columns_ch(self, conn: ext.ClickHouseConnection) -> list[Column]:
        from clickhouse_sqlalchemy import types
        return [
            Column("Category", types.String),
            Column("City", types.String),
            Column("Country", types.String),
            Column("Customer ID", types.String),
            Column("Customer Name", types.String),
            Column("Discount", types.Float32),
            Column("Order Date", types.Date),
            Column("Order Date Str", types.String),
            Column("Order ID", types.String),
            Column("Postal Code", types.Int32),
            Column("Product ID", types.String),
            Column("Product Name", types.String),
            Column("Profit", types.Float32),
            Column("Quantity", types.Int32),
            Column("Region", types.String),
            Column("Row ID", types.Int32),
            Column("Sales", types.Float32),
            Column("Segment", types.String),
            Column("Ship Date", types.Date),
            Column("Ship Date Str", types.String),
            Column("Ship Mode", types.String),
            Column("State", types.String),
            Column("Sub-Category", types.String),
        ]

    def ensure_sample_superstore_table_schema(self):
        table_name = self.db_data.sample_super_store_table_name
        schema_name = self.db_data.sample_super_store_schema_name

        table_exists: bool = self.ew.has_table(table_name=table_name, schema=schema_name)
        expected_columns = self.sample_superstore_sa_columns(self.db_data.connection)

        create_table: bool

        if table_exists:
            # Ensuring table schema
            existing_table = self.ew.load_table(table_name=table_name, schema=schema_name)
            actual_col_types = [(c.name, type(c.type)) for c in existing_table.c]
            expected_col_types = [(c.name, type(c.type)) for c in expected_columns]

            if actual_col_types != expected_col_types:
                create_table = True
                self.ew.drop_table(self.db_data.target_db_name, existing_table)
            else:
                create_table = False
        else:
            create_table = True

        if create_table:
            self.ew.create_table(
                self.ew.table_from_columns(
                    table_name=table_name,
                    schema=schema_name,
                    columns=expected_columns
                ),
            )


class AcceptanceScenario:
    _DO_ADD_EXC_MESSAGE: ClassVar[bool] = True

    @pytest.fixture()
    def api(self) -> WorkbookOpsFacade:
        raise NotImplementedError()

    @pytest.fixture()
    def int_api_clients(self) -> InternalAPIClients:
        raise NotImplementedError()

    @pytest.fixture(scope="session")
    def ch_connection_testing_data(self) -> ConnectionTestingData:
        pytest.skip("No CHYT connection data provided for scenario")

    def ensure_conn_data(self, conn_t: Type[ext.Connection], data: ConnectionTestingData) -> ConnectionTestingData:
        conn_def = data.connection
        assert isinstance(conn_def, conn_t)

        data_filler = DataFiller(data)
        data_filler.ensure_sample_superstore_table_schema()
        return data

    async def create_connection(
            self,
            api: WorkbookOpsFacade,
            wb_id: str,
            conn_t: Type[ext.Connection],
            name: str,
            data: ConnectionTestingData,
    ) -> CreatedConnectionTestingData:
        conn_def = data.connection
        assert isinstance(conn_def, conn_t)

        resp = await api.create_connection(ext.ConnectionCreateRequest(
            workbook_id=wb_id,
            connection=ext.ConnectionInstance(
                name=name,
                connection=conn_def,
            ),
            secret=data.secret,
        ))
        return CreatedConnectionTestingData(
            entry_info=resp.connection_info,
            data=data
        )

    @pytest.fixture(scope="session")
    def ch_connection_testing_data_ensured(self, ch_connection_testing_data) -> ConnectionTestingData:
        return self.ensure_conn_data(ext.ClickHouseConnection, ch_connection_testing_data)

    @pytest.fixture()
    async def wb_id(self) -> str:
        raise NotImplementedError()

    @pytest.fixture()
    async def ch_connection(self, api, wb_id: str, ch_connection_testing_data_ensured) -> CreatedConnectionTestingData:
        return await self.create_connection(
            api, wb_id,
            ext.ClickHouseConnection,
            "ch_conn", ch_connection_testing_data_ensured
        )

    @pytest.fixture(params=[
        "ch_connection",
    ])
    def conn_td(self, request) -> CreatedConnectionTestingData:
        """
        Dispatching fixture that return any connection
        """
        return request.getfixturevalue(request.param)

    @pytest.fixture()
    async def broken_dash(
            self,
            int_api_clients,
            wb_id: str,
    ) -> dashboards.DashInstance:
        dash = dashboards.Dashboard(  # region
            tabs=(
                dashboards.Tab(
                    id='Qk',
                    title='Вкладка 1',
                    items=(
                        dashboards.ItemControl(
                            id='Wa',
                            namespace='default',
                            data=dashboards.ManualControlData(
                                title='Test Param',
                                source=dashboards.ManualControlSourceSelect(
                                    showTitle=True,
                                    fieldName='test_param',
                                    multiselectable=False,
                                    acceptableValues=[
                                        dashboards.SelectorItem(
                                            title='azaza',
                                            value='azaza',
                                        ),
                                        dashboards.SelectorItem(
                                            title='ololo',
                                            value='ololo',
                                        ),
                                    ],
                                ),
                            ),
                            defaults=FrozenMappingStrToStrOrStrSeq({
                                'test_param': '',
                            }),
                        ),
                    ),
                    layout=(
                        dashboards.LayoutItem(
                            i='Wa',
                            h=2,
                            w=8,
                            x=0,
                            y=0,
                        ),
                    ),
                    connections=(),
                    aliases=dashboards.Aliases(
                        default=(),
                    ),
                ),
            ),  # endregion
        )
        dash_cli = int_api_clients.dash
        if dash_cli is None:
            raise pytest.skip("Can not create broken dash due to client missing")

        summary = await dash_cli.create_dashboard(dash, workbook_id=wb_id, name="broken_dash")
        return dashboards.DashInstance(
            summary=summary,
            dash=dash,
        )

    # Helpers
    @contextlib.contextmanager
    def wb_op_exc_pretty_print(self) -> Iterator[None]:
        try:
            yield
        except WorkbookOperationException as main_exc:
            print(pretty_repr(main_exc.data))
            raise

    @classmethod
    def get_ext_field_projection(cls, ext_f: ext.DatasetField) -> tuple:
        assert isinstance(ext_f.calc_spec, ext.DirectCS)
        return ext_f.calc_spec.field_name, ext_f.cast, ext_f.aggregation

    @classmethod
    def get_all_ext_field_projection(cls, all_ext_f: Iterable[ext.DatasetField]) -> Sequence[tuple]:
        return sorted(cls.get_ext_field_projection(f) for f in all_ext_f)

    @pytest.mark.asyncio
    @pytest.mark.parametrize("ref_factory", [
        lambda bi_conn, wb_id: ext.EntryIDRef(bi_conn.id),
        lambda bi_conn, wb_id: ext.EntryWBRef(wb_id=wb_id, entry_name=bi_conn.name),
    ], ids=["id_ref", "wb_ref"])
    async def test_advise_fields_by_conn_id(self, api, conn_td: CreatedConnectionTestingData, wb_id: str, ref_factory):
        dataset = SuperStoreExtDSBuilder("--replace-me--").set_source_as_table(
            table_name=conn_td.data.sample_super_store_table_name,
            db_name=conn_td.data.target_db_name,
        ).build()
        expected_dataset = SuperStoreExtDSBuilder("--replace-me--").set_source_as_table(
            table_name=conn_td.data.sample_super_store_table_name,
            db_name=conn_td.data.target_db_name,
        ).do_add_default_fields().build()

        assert dataset.fields == (), "Builder returns dataset with fields despite of it was not asked"

        resp = await api.advise_dataset_fields(ext.AdviseDatasetFieldsRequest(
            partial_dataset=dataset,
            connection_ref=ref_factory(conn_td.entry_info, wb_id),
        ))

        assert self.get_all_ext_field_projection(
            resp.dataset.fields
        ) == self.get_all_ext_field_projection(expected_dataset.fields)

    @pytest.mark.asyncio
    async def test_write_default_dataset(self, api, conn_td: CreatedConnectionTestingData, wb_id: str):
        dataset = SuperStoreExtDSBuilder(
            conn_td.entry_info.name
        ).do_add_default_fields().set_source_as_table(
            table_name=conn_td.data.sample_super_store_table_name,
            db_name=conn_td.data.target_db_name,
        ).build_instance("main")
        wb = ext.WorkBook(datasets=[dataset], charts=[], dashboards=[])

        with self.wb_op_exc_pretty_print():
            await api.write_workbook(
                ext.WorkbookWriteRequest(workbook_id=wb_id, workbook=wb)
            )

    @pytest.mark.parametrize("f_type,param_default_value,err_text", [
        (ext.FieldType.string, "ololo", None),
        (ext.FieldType.integer, "1", None),
        (ext.FieldType.float, "1.5", None),
        (ext.FieldType.float, "not_a_float", "not a valid float: not_a_float"),
    ])
    @pytest.mark.asyncio
    async def test_write_default_dataset_with_parameter(
            self,
            api,
            conn_td: CreatedConnectionTestingData,
            wb_id: str,
            # Params
            f_type: ext.FieldType,
            param_default_value: str,
            err_text: Optional[str],
    ):
        param_field = ext.DatasetField(
            id="parampampam",
            title="ParamPamPam",
            description=None,
            aggregation=ext.Aggregation.none,
            cast=f_type,
            calc_spec=ext.ParameterCS(
                default_value=param_default_value,
            ),
        )
        dataset = SuperStoreExtDSBuilder(conn_td.entry_info.name).set_source_as_table(
            table_name=conn_td.data.sample_super_store_table_name,
            db_name=conn_td.data.target_db_name,
        ).add_field(param_field).build_instance("main")
        wb = ext.WorkBook(datasets=[dataset], charts=[], dashboards=[])
        write_rq = ext.WorkbookWriteRequest(workbook_id=wb_id, workbook=wb)

        if err_text is None:
            await api.write_workbook(write_rq)
            read_rs = await api.read_workbook(ext.WorkbookReadRequest(workbook_id=wb_id))
            read_dataset: ext.Dataset = read_rs.workbook.datasets[0].dataset

            assert read_dataset.fields[0] == param_field
        else:
            with pytest.raises(WorkbookOperationException) as exc_data:
                await api.write_workbook(write_rq)

            cleared_err_data = clear_volatile_common_error_data(exc_data.value.data)

            assert cleared_err_data.entry_errors == (
                ext.EntryError(
                    name=dataset.name,
                    errors=[
                        ext.CommonError(
                            path="fields.parampampam",
                            message=f"Constraint violation: {err_text}",
                            exc_message=None,
                            stacktrace=None
                        )
                    ]
                ),
            )

    @pytest.mark.asyncio
    async def test_write_dataset_replaced_source(self, api, conn_td: CreatedConnectionTestingData, wb_id: str):
        sub_sql = conn_td.data.sample_super_store_sub_select_sql
        if sub_sql is None:
            pytest.skip("No SQL for sub-SQL data source was provided in fixture")

        dataset = SuperStoreExtDSBuilder(conn_td.entry_info.name).set_source(
            ext.DataSource(
                connection_ref=conn_td.entry_info.name,
                id="main",
                title="Subselect",
                spec=ext.SubSelectDataSourceSpec(
                    sql=sub_sql,
                )
            ),
        ).do_add_default_fields().build_instance("main")
        wb = ext.WorkBook(datasets=[dataset], charts=[], dashboards=[])

        with self.wb_op_exc_pretty_print():
            await api.write_workbook(
                ext.WorkbookWriteRequest(workbook_id=wb_id, workbook=wb)
            )

    @pytest.mark.parametrize("check_id,check_title", [
        (True, False),
        (False, True),
        (True, True),
    ])
    @pytest.mark.asyncio
    async def test_ds_add_field_invalid_id_or_title(
            self,
            api,
            conn_td: CreatedConnectionTestingData,
            wb_id: str,
            # params
            check_id: bool,
            check_title: bool,
    ):
        builder = SuperStoreExtDSBuilder(conn_td.entry_info.name).set_source_as_table(
            table_name=conn_td.data.sample_super_store_table_name,
            db_name=conn_td.data.target_db_name,
        )
        if check_id:
            builder = builder.add_field(ext.DatasetField(
                id="!@#t",
                title="bad id",
                description=None,
                aggregation=ext.Aggregation.none,
                cast=ext.FieldType.integer,
                calc_spec=ext.ParameterCS(
                    default_value="1",
                ),
            ))

        if check_title:
            builder = builder.add_field(ext.DatasetField(
                id="bad_title",
                title="very_long_title" * 20,
                description=None,
                aggregation=ext.Aggregation.none,
                cast=ext.FieldType.integer,
                calc_spec=ext.ParameterCS(
                    default_value="1",
                ),
            ))

        dataset = builder.build_instance("main")
        wb = ext.WorkBook(datasets=[dataset], charts=[], dashboards=[])
        write_rq = ext.WorkbookWriteRequest(workbook_id=wb_id, workbook=wb)

        with pytest.raises(WorkbookOperationException) as exc_data:
            await api.write_workbook(write_rq)

        cleared_err_data = clear_volatile_common_error_data(exc_data.value.data)

        expected = []
        if check_id:
            expected.append(
                CommonError(
                    path='fields.!@#t.attrs.id',
                    message='Constraint violation: Got invalid ID for a field, allowed values: [a-zA-Z0-9_]+',
                    exc_message=None,
                    stacktrace=None,
                ),
            )
        if check_title:
            expected.append(
                CommonError(
                    path='fields.bad_title.attrs.title',
                    message='Constraint violation: Title exceeds max length of 35 characters',
                    exc_message=None,
                    stacktrace=None,
                )
            )

        assert cleared_err_data.entry_errors[0].errors == tuple(expected)

    @pytest.mark.asyncio
    async def test_dataset_add_id_formula(
            self,
            api,
            conn_td: CreatedConnectionTestingData,
            wb_id: str
    ):
        dataset = (
            SuperStoreExtDSBuilder(conn_td.entry_info.name)
                .set_source_as_table(
                table_name=conn_td.data.sample_super_store_table_name,
                db_name=conn_td.data.target_db_name,
            )
                .do_add_default_fields()
                .add_field(
                ext.DatasetField(
                    id='profit_sum',
                    title='Total profit',
                    description=None,
                    cast=ext.FieldType.float,
                    aggregation=ext.Aggregation.none,
                    calc_spec=ext.IDFormulaCS(
                        formula='SUM([profit])'
                    )
                )
            )
                .add_field(
                ext.DatasetField(
                    id='profit_avg',
                    title='Average profit',
                    description=None,
                    cast=ext.FieldType.float,
                    aggregation=ext.Aggregation.none,
                    calc_spec=ext.FormulaCS(
                        formula='AVG([Profit])'
                    )
                )
            )
        ).build_instance("main")

        wb = ext.WorkBook(datasets=[dataset], charts=[], dashboards=[])

        with self.wb_op_exc_pretty_print():
            await api.write_workbook(
                ext.WorkbookWriteRequest(workbook_id=wb_id, workbook=wb)
            )

        # Retrieving with title formula
        for use_guid in [None, False]:
            resp = await api.read_workbook(ext.WorkbookReadRequest(
                workbook_id=wb_id,
                use_id_formula=use_guid,
            ))
            actual_workbook = resp.workbook

            fields_by_title = {
                f.title: f
                for f in actual_workbook.datasets[0].dataset.fields
            }
            assert fields_by_title['Total profit'] == ext.DatasetField(
                id='profit_sum',
                title='Total profit',
                description=None,
                hidden=False,
                cast=ext.FieldType.float,
                aggregation=ext.Aggregation.none,
                calc_spec=ext.FormulaCS(formula='SUM([Profit])')
            )
            assert fields_by_title['Average profit'] == ext.DatasetField(
                id='profit_avg',
                title='Average profit',
                description=None,
                hidden=False,
                cast=ext.FieldType.float,
                aggregation=ext.Aggregation.none,
                calc_spec=ext.FormulaCS(formula='AVG([Profit])')
            )

        # And now with guid
        resp = await api.read_workbook(ext.WorkbookReadRequest(
            workbook_id=wb_id,
            use_id_formula=True,
        ))
        actual_workbook = resp.workbook

        fields_by_title = {
            f.title: f
            for f in actual_workbook.datasets[0].dataset.fields
        }
        assert fields_by_title['Total profit'] == ext.DatasetField(
            id='profit_sum',
            title='Total profit',
            description=None,
            hidden=False,
            cast=ext.FieldType.float,
            aggregation=ext.Aggregation.none,
            calc_spec=ext.IDFormulaCS(formula='SUM([profit])')
        )
        assert fields_by_title['Average profit'] == ext.DatasetField(
            id='profit_avg',
            title='Average profit',
            description=None,
            hidden=False,
            cast=ext.FieldType.float,
            aggregation=ext.Aggregation.none,
            calc_spec=ext.IDFormulaCS(formula='AVG([profit])')
        )

    @pytest.mark.asyncio
    async def test_dataset_with_bad_field_produces_common_error(
            self, api, conn_td: CreatedConnectionTestingData,
            wb_id: str,
    ):
        dataset = (
            SuperStoreExtDSBuilder(conn_td.entry_info.name)
                .set_source_as_table(
                table_name=conn_td.data.sample_super_store_table_name,
                db_name=conn_td.data.target_db_name,
            )
                .add_field(
                ext.DatasetField(
                    id='profit_bad',
                    title='Profit Bad',
                    description=None,
                    hidden=False,
                    cast=ext.FieldType.float,
                    aggregation=ext.Aggregation.none,
                    calc_spec=ext.DirectCS(field_name='Profitx', avatar_id='-42', ),
                ),
            )
                .build_instance("main")
        )
        wb = ext.WorkBook(datasets=[dataset], charts=[], dashboards=[])

        with pytest.raises(WorkbookOperationException) as exc_info:
            await api.write_workbook(
                ext.WorkbookWriteRequest(workbook_id=wb_id, workbook=wb)
            )

        cleared_data = clear_volatile_common_error_data(exc_info.value.data)
        assert cleared_data == ext.ErrWorkbookOp(
            message='Error writing workbook',
            common_errors=(
                ext.CommonError(
                    message='Workbook validation failed. Persisting will not be performed.',
                ),
            ),
            entry_errors=[
                ext.EntryError(name='main', errors=(
                    ext.CommonError(
                        path='field.profit_bad',
                        message='Unknown referenced source column: Profitx, code: ERR.DS_API.FORMULA.UNKNOWN_SOURCE_COLUMN',
                        exc_message=None, stacktrace=None
                    ),
                ))
            ],
            partial_workbook=None,
        )

    @pytest.mark.asyncio
    async def test_load_broken_dash(
            self,
            wb_id: str,
            api,
            broken_dash: dashboards.DashInstance,
    ):
        with pytest.raises(WorkbookOperationException) as exc_info:
            await api.read_workbook(ext.WorkbookReadRequest(workbook_id=wb_id))

        exc: WorkbookOperationException = exc_info.value
        assert exc.data.entry_errors == (ext.EntryError(
            name=broken_dash.summary.name,
            errors=[ext.CommonError(
                message="Error during handling entry broken_dash: <class 'bi_external_api.converter.converter_exc.NotSupportedYet'>",
                # Until we rework tests to pytest.mark.parametrize over different props
                exc_message="Manual selectors are not yet implemented in API" if self._DO_ADD_EXC_MESSAGE else None,
                stacktrace=None,
            )]
        ),)

    @pytest.mark.asyncio
    @pytest.mark.parametrize("do_rewrite_dash", [True, False], ids=["rewrite", "remove"])
    async def test_force_rewrite_with_broken_dash(
            self,
            wb_id: str,
            api,
            broken_dash: dashboards.DashInstance,
            conn_td: CreatedConnectionTestingData,
            do_rewrite_dash: bool,
    ):
        dataset = SuperStoreExtDSBuilder(conn_td.entry_info.name).set_source_as_table(
            table_name=conn_td.data.sample_super_store_table_name,
            db_name=conn_td.data.target_db_name,
        ).do_add_default_fields().build_instance("main")
        chart = create_indicator_for_sum_sales(name="indi", dataset_name=dataset.name)
        dash = ext.DashInstance(
            name=broken_dash.summary.name,
            dashboard=ext.Dashboard(tabs=[ext.DashboardTab(
                id="tabId",
                title="Tab title",
                items=[],
                ignored_connections=(),
            )])
        )

        desired_wb = ext.WorkBook(
            datasets=[dataset],
            charts=[chart],
            dashboards=[dash] if do_rewrite_dash else [],
        )

        with pytest.raises(WorkbookOperationException):
            await api.write_workbook(ext.WorkbookWriteRequest(
                workbook_id=wb_id,
                workbook=desired_wb,
            ))

        # TODO FIX: Validate error messages

        # Trying to rewrite with force
        write_resp = await api.write_workbook(ext.WorkbookWriteRequest(
            workbook_id=wb_id,
            workbook=desired_wb,
            force_rewrite=True,
        ))

        assert write_resp

        # Getting back config
        read_resp = await api.read_workbook(ext.WorkbookReadRequest(workbook_id=wb_id))
        actual_workbook = read_resp.workbook

        assert len(actual_workbook.charts) == 1
        assert len(actual_workbook.datasets) == 1
        # Check that broken dash was removed
        if do_rewrite_dash:
            assert len(actual_workbook.dashboards) == 1
        else:
            assert len(actual_workbook.dashboards) == 0

    @pytest.mark.asyncio
    async def test_error_missing_dataset(
            self,
            wb_id: str,
            api,
    ):
        chart = create_indicator_for_sum_sales(name="indi", dataset_name="ne_dataset")

        desired_wb = ext.WorkBook(
            datasets=[],
            charts=[chart],
            dashboards=[],
        )

        with pytest.raises(WorkbookOperationException) as exc_info:
            await api.write_workbook(ext.WorkbookWriteRequest(
                workbook_id=wb_id,
                workbook=desired_wb,
            ))

        cleared_data = clear_volatile_common_error_data(exc_info.value.data)

        assert cleared_data == ext.ErrWorkbookOp(
            message='Error writing workbook',
            common_errors=(
                ext.CommonError(
                    message='Workbook validation failed. Persisting will not be performed.',
                ),
            ),
            entry_errors=(
                ext.EntryError(
                    name='indi',
                    # Validation will be aborted on ad-hoc fields resolution,
                    #  so there is no error for visualization field.
                    #  May be it will be fixed later.
                    errors=(
                        ext.CommonError(
                            path='datasets.ne_dataset',
                            message="Referenced entry not found in workbook: DatasetInstance/EntryNameRef(name='ne_dataset') not found in workbook.",
                        ),
                        ext.CommonError(
                            path='ad_hoc_fields.sum_sales',
                            message="Referenced entry not found in workbook: DatasetInstance/EntryNameRef(name='ne_dataset') not found in workbook.",
                        ),
                    )
                ),
            ),
            partial_workbook=None
        )


async def _test_dc_workbook_create_modify_delete(client: WorkbookOpsClient, *, wb_title: str, project_id: str):
    resp = await client.dc_create_workbook(rpc_dc.DCOpWorkbookCreateRequest(
        project_id=project_id,
        workbook_title=wb_title,
    ))
    wb_id = resp.workbook_id
    assert wb_id
    resp2 = await client.dc_modify_workbook(
        rpc_dc.DCOpWorkbookModifyRequest(workbook_id=wb_id,
                                         workbook=ext.WorkBook(datasets=[], charts=[], dashboards=[]))
    )
    assert resp2.executed_plan
    resp3 = await client.dc_read_workbook(
        rpc_dc.DCOpWorkbookGetRequest(
            workbook_id=wb_id,
        )
    )
    assert resp3.workbook
    resp4 = await client.dc_delete_workbook(
        rpc_dc.DCOpWorkbookDeleteRequest(
            workbook_id=wb_id,
        )
    )
    assert resp4

    # TODO FIX: BI-4282 Uncomment after fix project ID resolution for deleted workbooks
    # with pytest.raises(WorkbookOperationException):
    #     await client.dc_read_workbook(
    #         rpc_dc.DCOpWorkbookGetRequest(
    #             workbook_id=wb_id,
    #         )
    #     )


async def _test_dc_connection_create_modify_delete(client, wb_id, conn_name):
    connection = CHConnectionBuilder().build_instance(conn_name)
    modified_connection = CHConnectionBuilder(port=666).build_instance(conn_name)

    resp = await client.dc_connection_create(
        ext.DCOpConnectionCreateRequest(
            workbook_id=wb_id,
            connection=connection,
            secret=ext.PlainSecret("My-Lovely-YT-token"),
        )
    )
    assert resp

    resp2 = await client.dc_connection_get(
        ext.DCOpConnectionGetRequest(
            workbook_id=wb_id,
            name=conn_name,
        )
    )
    assert resp2

    resp3 = await client.dc_connection_modify(
        ext.DCOpConnectionModifyRequest(
            workbook_id=wb_id,
            connection=modified_connection,
            secret=ext.PlainSecret("My-Lovely-YT-token"),
        )
    )
    assert resp3

    resp4 = await client.dc_connection_delete(
        ext.DCOpConnectionDeleteRequest(
            workbook_id=wb_id,
            name=conn_name,
        )
    )
    assert resp4

    with pytest.raises(WorkbookOperationException):
        await client.dc_connection_delete(
            ext.DCOpConnectionDeleteRequest(
                workbook_id=wb_id,
                name=conn_name,
            )
        )
