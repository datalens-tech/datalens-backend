import attr
import pytest

from dl_constants.enums import CalcMode, AggregationFunction, BIType
from bi_external_api.converter.charts.ad_hoc_field_extra_resolver import AdHocFieldExtraResolver
from bi_external_api.converter.charts.chart_converter import BaseChartConverter
from bi_external_api.converter.converter_ctx import ConverterContext
from bi_external_api.converter.main import DatasetConverter
from bi_external_api.converter.workbook_ctx_loader import WorkbookContextLoader
from bi_external_api.domain import external as ext
from bi_external_api.domain.internal import datasets
from bi_external_api.domain.internal.charts import ChartActionField
from bi_external_api.domain.internal.dl_common import EntrySummary, EntryScope

PG_CONN_NAME_PLACEHOLDER = "--PG_CONN_NAME--"


def replace_conn_name(dataset: ext.Dataset, search: str, replace: str) -> ext.Dataset:
    return attr.evolve(
        dataset,
        sources=[
            attr.evolve(src, connection_ref=replace) if src.connection_ref == search else src
            for src in dataset.sources
        ],
    )


@pytest.mark.parametrize("code,input_dataset,expected_dataset", [
    [
        "full",
        ext.Dataset(
            avatars=None,
            sources=[
                ext.DataSource(
                    id="tbl", title="tbl",
                    connection_ref=PG_CONN_NAME_PLACEHOLDER,
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
        ),
        ext.Dataset(
            sources=[
                ext.DataSource(
                    id="tbl", title="tbl",
                    connection_ref=PG_CONN_NAME_PLACEHOLDER,
                    spec=ext.SubSelectDataSourceSpec(
                        sql="SELECT 1 as ololo",
                    ),
                ),
            ],
            fields=[
                ext.DatasetField(
                    aggregation=ext.Aggregation.none,
                    cast=ext.FieldType.integer,
                    description=None,
                    id="ololo",
                    title="Ololo",
                    calc_spec=ext.DirectCS(
                        avatar_id="tbl",
                        field_name="ololo",
                    ),
                ),
            ],
            avatars=ext.AvatarsConfig(
                definitions=(ext.AvatarDef(id="tbl", source_id="tbl", title="tbl"),),
                root="tbl",
                joins=(),
            )
        )
    ],
])
@pytest.mark.asyncio
async def test_set_default_avatar_config(
        code, input_dataset, expected_dataset,
        wb_ctx_loader, pseudo_wb_path, pg_connection
):
    wb_ctx = await wb_ctx_loader.load(pseudo_wb_path)
    converter = DatasetConverter(wb_ctx, ConverterContext())

    actual_dataset = converter.fill_defaults(
        replace_conn_name(input_dataset, search=PG_CONN_NAME_PLACEHOLDER, replace=pg_connection.name)
    )

    assert actual_dataset == replace_conn_name(
        expected_dataset,
        search=PG_CONN_NAME_PLACEHOLDER,
        replace=pg_connection.name
    )


@pytest.mark.parametrize("code,input_dataset", [
    [
        "simple",
        ext.Dataset(
            sources=[
                ext.DataSource(
                    id="tbl", title="tbl",
                    connection_ref=PG_CONN_NAME_PLACEHOLDER,
                    spec=ext.SubSelectDataSourceSpec(
                        sql="SELECT 1 as ololo",
                    ),
                ),
            ],
            fields=[
                ext.DatasetField(
                    aggregation=ext.Aggregation.none,
                    cast=ext.FieldType.integer,
                    description="Some description",
                    id="ololo",
                    title="Ololo",
                    calc_spec=ext.DirectCS(
                        avatar_id="tbl",
                        field_name="ololo",
                    ),
                ),
            ],
            avatars=ext.AvatarsConfig(
                definitions=[ext.AvatarDef(id="tbl", source_id="tbl", title="tbl")],
                root="tbl",
                joins=[],
            )
        )]
])
@pytest.mark.asyncio
async def test_round_trip(
        code, input_dataset,
        wb_ctx_loader, pseudo_wb_path, pg_connection,
        bi_ext_api_test_env_bi_api_control_plane_client,
):
    int_api_cli = bi_ext_api_test_env_bi_api_control_plane_client

    normalized_ext_dataset = replace_conn_name(
        input_dataset,
        search=PG_CONN_NAME_PLACEHOLDER,
        replace=pg_connection.name,
    )

    wb_ctx = await wb_ctx_loader.load(pseudo_wb_path)
    converter = DatasetConverter(wb_ctx, ConverterContext())

    actions = converter.convert_public_dataset_to_actions(normalized_ext_dataset)

    int_dataset, _ = await int_api_cli.build_dataset_config_by_actions(actions)

    actual_ext_dataset = converter.convert_internal_dataset_to_public_dataset(int_dataset)

    assert actual_ext_dataset == normalized_ext_dataset


_AH_DFT_KW = dict(id="subj_to_change", title="AH", description=None, hidden=False)
_UPD_CF_DFT_KW = dict(
    guid="subj_to_change", title="AH", description="", hidden=False, default_value=None,
    datasetId="subj_to_change",
)
_AD_HOC_FIELDS_TEST_CASES = [
    (
        "title_formula",
        ext.AdHocField(field=ext.DatasetField(
            calc_spec=ext.FormulaCS(formula="[The Num] * 2"),
            cast=ext.FieldType.float,
            aggregation=ext.Aggregation.none,
            **_AH_DFT_KW,
        )),
        ChartActionField(
            calc_mode=CalcMode.formula,
            source="",
            avatar_id=None,
            formula="[The Num] * 2",
            guid_formula="[num] * 2",
            aggregation=AggregationFunction.none,
            cast=BIType.float,
            **_UPD_CF_DFT_KW,
        ),
    ),
    (
        "guid_formula",
        ext.AdHocField(field=ext.DatasetField(
            calc_spec=ext.IDFormulaCS(formula="[num] * 2"),
            cast=ext.FieldType.float,
            aggregation=ext.Aggregation.none,
            **_AH_DFT_KW,
        )),
        ChartActionField(
            calc_mode=CalcMode.formula,
            source="",
            avatar_id=None,
            formula="[The Num] * 2",
            guid_formula="[num] * 2",
            aggregation=AggregationFunction.none,
            cast=BIType.float,
            **_UPD_CF_DFT_KW,
        ),
    ),
    (
        "direct_field",
        ext.AdHocField(field=ext.DatasetField(
            calc_spec=ext.DirectCS(field_name="num", avatar_id="main"),
            cast=ext.FieldType.integer,
            aggregation=ext.Aggregation.none,
            **_AH_DFT_KW,
        )),
        ChartActionField(
            calc_mode=CalcMode.direct,
            source="num",
            avatar_id="main",
            formula="",
            guid_formula="",
            aggregation=AggregationFunction.none,
            cast=BIType.integer,
            **_UPD_CF_DFT_KW,
        ),
    ),
]


@pytest.mark.parametrize(
    "ad_hoc_field,expected_field_in_upd",
    [(ahf, caf) for _, ahf, caf in _AD_HOC_FIELDS_TEST_CASES],
    ids=[case[0] for case in _AD_HOC_FIELDS_TEST_CASES]
)
@pytest.mark.asyncio
async def test_chart_ad_hoc_fields(
        ad_hoc_field,
        expected_field_in_upd,
        #
        pseudo_wb_path: str,
        wb_ctx_loader: WorkbookContextLoader,
        bi_ext_api_test_env_bi_api_control_plane_client,
        pg_connection,
):
    ds_cp_cli = bi_ext_api_test_env_bi_api_control_plane_client

    normalized_ext_dataset = replace_conn_name(
        ext.Dataset(
            sources=[
                ext.DataSource(
                    id="main", title="SQL",
                    connection_ref=PG_CONN_NAME_PLACEHOLDER,
                    spec=ext.SubSelectDataSourceSpec(
                        sql="SELECT 1 as num",
                    ),
                ),
            ],
            fields=[
                ext.DatasetField(
                    aggregation=ext.Aggregation.none,
                    cast=ext.FieldType.integer,
                    description=None,
                    id="num",
                    title="The Num",
                    calc_spec=ext.DirectCS(field_name="num"),
                ),
            ],
            avatars=None
        ),
        search=PG_CONN_NAME_PLACEHOLDER,
        replace=pg_connection.name,
    )
    wb_ctx = await wb_ctx_loader.load(pseudo_wb_path)
    ds_converter = DatasetConverter(wb_ctx, ConverterContext())

    ds_pseudo_id = "ds_pseudo_id"
    ds_pseudo_name = "the_ds"
    actions_to_create_ds = ds_converter.convert_public_dataset_to_actions(
        ds_converter.fill_defaults(normalized_ext_dataset)
    )
    int_dataset, _ = await ds_cp_cli.build_dataset_config_by_actions(actions_to_create_ds)

    # Preparing context with dataset
    wb_ctx = wb_ctx.add_entries([
        datasets.DatasetInstance(
            dataset=int_dataset,
            summary=EntrySummary(
                scope=EntryScope.dataset,
                id=ds_pseudo_id,
                name=ds_pseudo_name,
                workbook_id=pseudo_wb_path,
            ),
        )
    ])

    # Preparing chart converters
    ad_hoc_field_resolver = AdHocFieldExtraResolver(
        wb_ctx,
        ds_cp_cli,
    )
    chart_converter = BaseChartConverter(wb_ctx, ConverterContext())

    # Target chart
    ext_chart = chart_converter.fill_defaults(ext.Chart(
        visualization=ext.Indicator(field=ext.ChartField(source=ext.ChartFieldRef("ad_hoc_f_id"))),
        ad_hoc_fields=[attr.evolve(
            ad_hoc_field,
            field=attr.evolve(
                ad_hoc_field.field,
                id="ad_hoc_f_id"
            )
        )],
        datasets=[ds_pseudo_name],
    ))

    # Validating all updates to resolve only-backend-resolvable attributes like field-type
    datasets_with_applied_actions = await ad_hoc_field_resolver.resolve_updates(
        chart_converter.convert_ext_chart_ad_hoc_fields_to_chart_actions(ext_chart),
        map_ds_id_to_source_actions={ds_pseudo_id: actions_to_create_ds},
    )

    int_chart = chart_converter.convert_chart_ext_to_int(
        ext_chart,
        datasets_with_applied_actions=datasets_with_applied_actions
    )

    # Ensure that ad-hoc fields was converters as it was expected
    assert int_chart.updates[0].field == attr.evolve(expected_field_in_upd, guid="ad_hoc_f_id", datasetId=ds_pseudo_id)
