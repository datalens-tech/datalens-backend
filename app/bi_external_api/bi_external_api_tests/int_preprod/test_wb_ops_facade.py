from typing import Optional

import attr
import pytest

from bi_external_api.attrs_model_mapper import Processor, pretty_repr
from bi_external_api.attrs_model_mapper.field_processor import FieldMeta
from bi_external_api.domain import external as ext
from bi_external_api.domain.internal import datasets
from bi_external_api.domain.internal.dl_common import EntrySummary
from bi_external_api.workbook_ops.public_exceptions import WorkbookOperationException
from bi_external_api.workbook_ops.wb_mod_steps.utils import TaggedStringAttrSetter
from dl_testing.utils import skip_outside_devhost


def create_default_ext_dashboard(pg_connection: datasets.ConnectionInstance) -> ext.WorkBook:
    # Preparing dataset
    EXT_DS_INST = ext.DatasetInstance(
        name="main_ds",
        dataset=ext.Dataset(
            avatars=None,
            sources=[
                ext.DataSource(
                    id="tbl", title="tbl",
                    connection_ref=pg_connection.summary.name,
                    spec=ext.SubSelectDataSourceSpec(
                        sql="SELECT t.* FROM (VALUES(1, 'one'), (2, 'two')) AS t (num, txt)",
                    ),
                ),
            ],
            fields=[
                ext.DatasetField(
                    calc_spec=ext.DirectCS(field_name="num"),
                    cast=ext.FieldType.integer,
                    description=None,
                    id="num",
                    title="NUM",
                ),
                ext.DatasetField(
                    calc_spec=ext.DirectCS(field_name="txt"),
                    cast=ext.FieldType.string,
                    description=None,
                    id="txt",
                    title="TXT",
                ),
            ],
        )
    )
    EXT_CHART_TBL = ext.ChartInstance(
        name="flat_table_over_main_ds",
        chart=ext.Chart(
            datasets=[EXT_DS_INST.name],
            ad_hoc_fields=[
                ext.AdHocField(
                    field=ext.DatasetField(
                        id="descr",
                        cast=ext.FieldType.string,
                        description="",
                        calc_spec=ext.FormulaCS(formula="CONCAT([TXT],'--')"),
                        title="The descr"
                    )
                )
            ],
            visualization=ext.FlatTable(
                columns=[
                    ext.ChartField.create_as_ref("num"),
                    ext.ChartField.create_as_ref("txt"),
                    ext.ChartField.create_as_ref("descr"),
                ]
            )
        )
    )
    EXT_DASH = ext.DashInstance(
        name="main_dash",
        dashboard=ext.Dashboard(
            tabs=(
                ext.DashboardTab(
                    id='Ak',
                    title='Вкладка 1',
                    items=(
                        ext.DashboardTabItem(
                            id="g5",
                            placement=ext.DashTabItemPlacement(h=10, w=12, x=0, y=2),
                            element=ext.DashChartsContainer(
                                hide_title=False,
                                tabs=[ext.WidgetTab(
                                    id="ma",
                                    title="tbl",
                                    chart_name=EXT_CHART_TBL.name,
                                )],
                                default_active_chart_tab_id="ma",
                            )
                        ),
                        ext.DashboardTabItem(
                            id="OK",
                            placement=ext.DashTabItemPlacement(h=2, w=12, x=0, y=0),
                            element=ext.DashControlMultiSelect(
                                title="txt title",
                                show_title=True,
                                source=ext.ControlValueSourceDatasetField(
                                    dataset_name=EXT_DS_INST.name,
                                    field_id="txt",
                                ),
                                default_value=ext.MultiStringValue(values=["two"]),
                            ),
                        )
                    ),
                    ignored_connections=(),
                ),
            ),
        ),
    )
    return ext.WorkBook(
        datasets=[EXT_DS_INST],
        charts=[EXT_CHART_TBL],
        dashboards=[EXT_DASH],
    )


@skip_outside_devhost
@pytest.mark.asyncio
async def test_rm_dataset(
        wb_ops_facade,
        pg_connection,
        pseudo_wb_path,
        wb_ctx_loader,
        dataset_factory,
):
    # Preparing dataset
    ds_inst = await dataset_factory.create_dataset(
        "testy",
        query="SELECT t.* FROM (VALUES(1, 'one'), (2, 'two')) AS t (num, txt)",
    )
    wb_ctx_before_write = await wb_ctx_loader.load(pseudo_wb_path)
    assert wb_ctx_before_write.resolve_dataset_by_name("testy") == ds_inst

    # Executing operation
    await wb_ops_facade.write_workbook(ext.WorkbookWriteRequest(
        workbook_id=pseudo_wb_path,
        workbook=ext.WorkBook(
            datasets=[],
            charts=[],
            dashboards=[]
        ),
    ))

    # Reloading workbook context
    wb_ctx_after_write = await wb_ctx_loader.load(pseudo_wb_path)

    # TODO FIX: replace attr.evolve with clone
    assert wb_ctx_after_write == attr.evolve(wb_ctx_before_write, datasets=[])


@skip_outside_devhost
@pytest.mark.asyncio
async def test_rm_and_create_dataset(
        wb_ops_facade,
        pg_connection,
        pseudo_wb_path,
        wb_ctx_loader,
        dataset_factory,
):
    # Preparing dataset
    ds_inst = await dataset_factory.create_dataset(
        "testy",
        query="SELECT t.* FROM (VALUES(1, 'one'), (2, 'two')) AS t (num, txt)",
    )
    wb_ctx_before_write = await wb_ctx_loader.load(pseudo_wb_path)
    assert wb_ctx_before_write.resolve_dataset_by_name("testy") == ds_inst

    new_ds_inst = ext.DatasetInstance(
        name="my_new_ds",
        dataset=ext.Dataset(
            avatars=None,
            sources=[
                ext.DataSource(
                    id="tbl", title="tbl",
                    connection_ref=pg_connection.name,
                    spec=ext.SubSelectDataSourceSpec(
                        sql="SELECT 1 as ololo",
                    ),
                ),
            ],
            fields=[
                ext.DatasetField(
                    calc_spec=ext.DirectCS(field_name="ololo"),
                    cast=ext.FieldType.integer,
                    description=None,
                    id="ololo",
                    title="Ololo",
                ),
            ],
        )
    )

    # Executing operation
    await wb_ops_facade.write_workbook(ext.WorkbookWriteRequest(
        workbook_id=pseudo_wb_path,
        workbook=ext.WorkBook(
            datasets=[new_ds_inst],
            charts=[],
            dashboards=[]
        ),
    ))

    # Reloading workbook context
    wb_ctx_after_write = await wb_ctx_loader.load(pseudo_wb_path)

    # TODO FIX: replace attr.evolve with clone
    assert wb_ctx_after_write == attr.evolve(
        wb_ctx_before_write,
        datasets=[wb_ctx_after_write.resolve_dataset_by_name("my_new_ds")]
    )


@skip_outside_devhost
@pytest.mark.asyncio
async def test_create_dataset_chart_dash_from_scratch(
        wb_ops_facade,
        pg_connection,
        pseudo_wb_path,
        wb_ctx_loader,
        dataset_factory,
):
    workbook = create_default_ext_dashboard(pg_connection)

    assert workbook.datasets
    assert workbook.charts
    assert workbook.dashboards

    await wb_ops_facade.write_workbook(ext.WorkbookWriteRequest(
        workbook_id=pseudo_wb_path,
        workbook=workbook,
    ))

    wb_read_resp = await wb_ops_facade.read_workbook(ext.WorkbookReadRequest(workbook_id=pseudo_wb_path))
    assert wb_read_resp.workbook
    # TODO FIX: Check when defaulter will be externalized
    # assert wb_read_resp.workbook == workbook


@skip_outside_devhost
@pytest.mark.asyncio
async def test_update_dash(
        wb_ops_facade,
        pg_connection,
        pseudo_wb_path,
        wb_ctx_loader,
        dataset_factory,
):
    workbook = create_default_ext_dashboard(pg_connection)

    await wb_ops_facade.write_workbook(ext.WorkbookWriteRequest(
        workbook_id=pseudo_wb_path,
        workbook=workbook,
    ))

    wb_read_resp = await wb_ops_facade.read_workbook(ext.WorkbookReadRequest(workbook_id=pseudo_wb_path))
    assert wb_read_resp.workbook

    # TODO FIX: Check when defaulter will be externalized
    # assert wb_read_resp.workbook == workbook

    class DashboardModifier(Processor[ext.DashTabItemPlacement]):
        def _should_process(self, meta: FieldMeta) -> bool:
            return meta.clz == ext.DashTabItemPlacement

        def _process_single_object(
                self,
                obj: ext.DashTabItemPlacement,
                meta: FieldMeta
        ) -> Optional[ext.DashTabItemPlacement]:
            return attr.evolve(obj, x=obj.x + 1)

    workbook_with_modified_dash = DashboardModifier().process(workbook)
    assert workbook != workbook_with_modified_dash

    write_resp = await wb_ops_facade.write_workbook(ext.WorkbookWriteRequest(
        workbook_id=pseudo_wb_path,
        workbook=workbook_with_modified_dash,
    ))
    # TODO FIX: Check later that chart/dataset was not changed
    assert ext.EntryOperation(
        entry_name="main_dash",
        entry_kind=ext.EntryKind.dashboard,
        operation_kind=ext.EntryOperationKind.modify,
    ) in write_resp.executed_plan.operations


@skip_outside_devhost
@pytest.mark.asyncio
async def test_update_dataset(
        wb_ops_facade,
        pg_connection,
        pseudo_wb_path,
        wb_ctx_loader,
        dataset_factory,
):
    # Preparing dataset
    EXT_DS_INST = ext.DatasetInstance(
        name="main_ds",
        dataset=ext.Dataset(
            avatars=None,
            sources=[
                ext.DataSource(
                    id="tbl", title="tbl",
                    connection_ref=pg_connection.name,
                    spec=ext.SubSelectDataSourceSpec(
                        sql="SELECT t.* FROM (VALUES(1, 'one'), (2, 'two')) AS t (num, txt)",
                    ),
                ),
            ],
            fields=[
                ext.DatasetField(
                    calc_spec=ext.DirectCS(field_name="num"),
                    cast=ext.FieldType.integer,
                    description=None,
                    id="num",
                    title="NUM",
                ),
                ext.DatasetField(
                    calc_spec=ext.DirectCS(field_name="txt"),
                    cast=ext.FieldType.string,
                    description=None,
                    id="txt",
                    title="TXT",
                ),
            ],
        )
    )
    MODIFIED_EXT_DS_INST = attr.evolve(
        EXT_DS_INST,
        dataset=attr.evolve(
            EXT_DS_INST.dataset,
            fields=[]
        )
    )
    # TODO FIX: Create dataset manually to ensure that revision ID is set
    await wb_ops_facade.write_workbook(ext.WorkbookWriteRequest(
        workbook_id=pseudo_wb_path,
        workbook=ext.WorkBook(
            datasets=[EXT_DS_INST],
            charts=[],
            dashboards=[]
        ),
    ))
    await wb_ops_facade.write_workbook(ext.WorkbookWriteRequest(
        workbook_id=pseudo_wb_path,
        workbook=ext.WorkBook(
            datasets=[MODIFIED_EXT_DS_INST],
            charts=[],
            dashboards=[]
        ),
    ))


@skip_outside_devhost
@pytest.mark.asyncio
async def test_no_changes(
        wb_ops_facade,
        pg_connection,
        pseudo_wb_path,
        wb_ctx_loader,
        dataset_factory,
):
    # Preparing dataset
    EXT_DS_INST = ext.DatasetInstance(
        name="main_ds",
        dataset=ext.Dataset(
            avatars=None,
            sources=[
                ext.DataSource(
                    id="tbl", title="tbl",
                    connection_ref=pg_connection.name,
                    spec=ext.SubSelectDataSourceSpec(
                        sql="SELECT t.* FROM (VALUES(1, 'one'), (2, 'two')) AS t (num, txt)",
                    ),
                ),
            ],
            fields=[
                ext.DatasetField(
                    calc_spec=ext.DirectCS(field_name="num"),
                    cast=ext.FieldType.integer,
                    description=None,
                    id="num",
                    title="NUM",
                ),
                ext.DatasetField(
                    calc_spec=ext.DirectCS(field_name="txt"),
                    cast=ext.FieldType.string,
                    description=None,
                    id="txt",
                    title="TXT",
                ),
            ],
        )
    )
    WB = ext.WorkBook(
        datasets=[EXT_DS_INST],
        charts=[],
        dashboards=[]
    )

    inital_resp = await wb_ops_facade.write_workbook(ext.WorkbookWriteRequest(
        workbook_id=pseudo_wb_path,
        workbook=WB,
    ))

    assert inital_resp.executed_plan == ext.ModificationPlan(
        operations=[ext.EntryOperation(
            entry_name=EXT_DS_INST.name,
            entry_kind=ext.EntryKind.dataset,
            operation_kind=ext.EntryOperationKind.create,
        )]
    )

    consequent_resp = await wb_ops_facade.write_workbook(ext.WorkbookWriteRequest(
        workbook_id=pseudo_wb_path,
        workbook=WB,
    ))

    assert consequent_resp.executed_plan == ext.ModificationPlan(
        operations=[],
    )


@skip_outside_devhost
@pytest.mark.asyncio
@pytest.mark.skip
async def test_exc(wb_ops_facade):
    with pytest.raises(WorkbookOperationException) as exc_info:
        await wb_ops_facade.read_workbook(ext.WorkbookReadRequest(workbook_id="alpha_workbooks/my_wb_1"))

    exc: WorkbookOperationException = exc_info.value
    print(pretty_repr(exc.data))


@skip_outside_devhost
@pytest.mark.asyncio
@pytest.mark.skip
async def test_get_wb(wb_ops_facade):
    try:
        resp = await wb_ops_facade.read_workbook(ext.WorkbookReadRequest(workbook_id="alpha_workbooks/reference_wb"))
        print(pretty_repr(resp.workbook, preferred_cls_name_prefixes={ext: "ext"}))
    except WorkbookOperationException as exc:
        print(pretty_repr(exc.data))


@skip_outside_devhost
@pytest.mark.asyncio
@pytest.mark.skip
async def test_copy_wb(wb_ops_facade, pseudo_wb_path, pg_connection: datasets.BIConnectionSummary):
    wb_get_resp = await wb_ops_facade.read_workbook(ext.WorkbookReadRequest(workbook_id="alpha_workbooks/all_charts"))

    wb = TaggedStringAttrSetter(
        tag=ext.ExtModelTags.connection_name,
        value_to_set=pg_connection.name,
    ).process(wb_get_resp.workbook)

    wb_write_resp = await wb_ops_facade.write_workbook(ext.WorkbookWriteRequest(
        workbook=wb,
        workbook_id=pseudo_wb_path,
    ))
    assert wb_write_resp is not None


@skip_outside_devhost
@pytest.mark.asyncio
@pytest.mark.skip
async def test_copy_ref_wb(wb_ops_facade, pseudo_wb_path, chyt_connection_ext_value_and_secret):
    conn_def, conn_secret = chyt_connection_ext_value_and_secret
    conn_name = "conn_chyt"
    target_conn = ext.ConnectionInstance(
        name=conn_name,
        connection=conn_def,
    )
    new_wb_id = f"{pseudo_wb_path}/ref_wb"

    wb_get_resp = await wb_ops_facade.read_workbook(ext.WorkbookReadRequest(workbook_id="alpha_workbooks/reference_wb"))

    wb = TaggedStringAttrSetter(
        tag=ext.ExtModelTags.connection_name,
        value_to_set=conn_name,
    ).process(wb_get_resp.workbook)

    wb_create_resp = await wb_ops_facade.create_fake_workbook(
        ext.FakeWorkbookCreateRequest(
            workbook_id=new_wb_id,
            workbook=ext.WorkbookConnectionsOnly(connections=[target_conn]),
            connection_secrets=[ext.ConnectionSecret(secret=conn_secret, conn_name=conn_name)],
        )
    )
    assert wb_create_resp is not None

    wb_write_resp = await wb_ops_facade.write_workbook(
        ext.WorkbookWriteRequest(
            workbook_id=new_wb_id,
            workbook=wb,
        )
    )
    assert wb_write_resp is not None


@skip_outside_devhost
@pytest.mark.asyncio
async def test_clusterize_wb_by_folder(wb_ops_facade, pseudo_wb_path, dashes_in_hierarchy: list[EntrySummary]):
    wb_clusterize_resp = await wb_ops_facade.clusterize_workbook(ext.WorkbookClusterizeRequest(
        navigation_folder_path=pseudo_wb_path,
    ))
    assert {(nme.unique_entry_id, nme.legacy_location) for nme in wb_clusterize_resp.name_map} == {
        (summary.id, tuple(summary.workbook_id.split("/") + [summary.name]))
        for summary in dashes_in_hierarchy
    }


@skip_outside_devhost
@pytest.mark.asyncio
async def test_clusterize_wb_by_dash_ids(wb_ops_facade, pseudo_wb_path, dashes_in_hierarchy: list[EntrySummary]):
    wb_clusterize_resp = await wb_ops_facade.clusterize_workbook(ext.WorkbookClusterizeRequest(
        dash_id_list=[summary.id for summary in dashes_in_hierarchy],
    ))
    assert {(nme.unique_entry_id, nme.legacy_location) for nme in wb_clusterize_resp.name_map} == {
        (summary.id, tuple(summary.workbook_id.split("/") + [summary.name]))
        for summary in dashes_in_hierarchy
    }
