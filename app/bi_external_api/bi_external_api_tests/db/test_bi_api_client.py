import random

import attr
import pytest

from bi_constants.enums import ManagedBy, CalcMode, AggregationFunction, BIType
from bi_external_api.domain.internal import datasets
from bi_external_api.domain.internal.dl_common import EntrySummary, EntryScope
from bi_external_api.internal_api_clients import exc_api
from bi_testing_ya.api_wrappers import Req

from bi_connector_postgresql.core.postgresql.constants import CONNECTION_TYPE_POSTGRES


@pytest.mark.asyncio
async def test_ping(bi_ext_api_test_env_bi_api_control_plane_client):
    int_api_cli = bi_ext_api_test_env_bi_api_control_plane_client

    resp = await int_api_cli.make_request(Req(method="GET", url="ping"))
    assert resp.status == 200


@pytest.mark.asyncio
async def test_get_wb_info(bi_ext_api_test_env_bi_api_control_plane_client):
    int_api_cli = bi_ext_api_test_env_bi_api_control_plane_client

    wb_path = f"home/api_user/{random.randint(0, 10000000)}"
    conn_name = f"test__{random.randint(0, 10000000)}"
    conn_data = dict(
        name=conn_name,
        dir_path=wb_path,
        type="postgres",
        host="localhost",
        port=1337,
        username="root",
        password="qwerty",
        db_name="test_db",
        cache_ttl_sec=None,
    )

    created_conn = await int_api_cli.create_connection(wb_id=wb_path, name=conn_name, conn_data=conn_data)

    assert isinstance(created_conn, datasets.ConnectionInstance)
    assert created_conn.summary.id is not None

    assert created_conn == datasets.ConnectionInstance(
        summary=EntrySummary(
            scope=EntryScope.connection,
            id=created_conn.summary.id,
            name=conn_name,
            workbook_id=wb_path,
        ),
        type=CONNECTION_TYPE_POSTGRES,
    )

    toc = await int_api_cli.get_workbook_backend_toc(workbook_id=wb_path)

    assert toc.connections == {created_conn.to_bi_connection_summary()}


def _create_actions_to_add_pg_subsql_source(
        *,
        pg_conn_id: str,
        sql: str,
) -> tuple[datasets.DataSourcePGSubSQL, datasets.Avatar]:
    source = datasets.DataSourcePGSubSQL(
        id="src_1",
        title="SRC 1",
        connection_id=pg_conn_id,
        parameters=datasets.DataSourceParamsSubSQL(subsql=sql)
    )
    avatar = datasets.Avatar(
        is_root=True,
        id="src_1",
        title="SRC 1",
        source_id=source.id,
    )
    return source, avatar


@pytest.mark.asyncio
async def test_dataset_actions_single_source(bi_ext_api_test_env_bi_api_control_plane_client, pg_connection):
    int_api_cli = bi_ext_api_test_env_bi_api_control_plane_client

    source, avatar = _create_actions_to_add_pg_subsql_source(
        pg_conn_id=pg_connection.id,
        sql="SELECT 1 as num"
    )
    actions = [
        datasets.ActionDataSourceAdd(source=source),
        datasets.ActionAvatarAdd(source_avatar=avatar, disable_fields_update=False),
    ]

    dateset, raw_response = await int_api_cli.build_dataset_config_by_actions(actions)

    assert dateset.sources == (attr.evolve(source, managed_by=ManagedBy.user),)
    assert dateset.source_avatars == (avatar,)
    assert len(dateset.result_schema) == 1

    single_field = dateset.result_schema[0]

    # Check downgrade is OK
    single_field.to_writable_result_schema()

    assert single_field.avatar_id == avatar.id
    assert single_field.source == "num"

    # Check no fields if update is disabled
    actions = [
        datasets.ActionDataSourceAdd(source=source),
        datasets.ActionAvatarAdd(source_avatar=avatar, disable_fields_update=True),
    ]

    dateset, raw_response = await int_api_cli.build_dataset_config_by_actions(actions)
    assert len(dateset.result_schema) == 0


@pytest.mark.parametrize("bi_type,param_default_value", [
    [BIType.string, datasets.DefaultValueString("DEFAULT!!!!")],
    [BIType.integer, datasets.DefaultValueInteger(1)],
    [BIType.float, datasets.DefaultValueFloat(1.5)],
])
@pytest.mark.asyncio
async def test_dataset_with_parameter_round_trip(
        bi_ext_api_test_env_bi_api_control_plane_client,
        pg_connection,
        bi_type: BIType,
        param_default_value: datasets.DefaultValue,
):
    int_api_cli = bi_ext_api_test_env_bi_api_control_plane_client

    source, avatar = _create_actions_to_add_pg_subsql_source(
        pg_conn_id=pg_connection.id,
        sql="SELECT 1 as num"
    )
    writable_field = datasets.ResultSchemaField(
        guid="some_param_fid",
        title="Parampampam",
        source="",
        calc_mode=CalcMode.parameter,
        formula="",
        guid_formula="",
        hidden=False,
        description="",
        aggregation=AggregationFunction.none,
        cast=bi_type,
        avatar_id=None,
        default_value=param_default_value,
    )

    actions = [
        datasets.ActionDataSourceAdd(source=source),
        datasets.ActionAvatarAdd(source_avatar=avatar, disable_fields_update=False),
        datasets.ActionFieldAdd(order_index=0, field=writable_field),
    ]
    dateset, raw_response = await int_api_cli.build_dataset_config_by_actions(actions)

    actual_field = dateset.result_schema[0]
    assert actual_field.to_writable_result_schema() == writable_field


DS_VALIDATION_ERRORS_TEST_CASES = (
    (
        "non_existing_direct_field",
        [
            datasets.ResultSchemaField(
                guid="fid",
                title="Broken field",
                source="non_existing_column_name",
                calc_mode=CalcMode.direct,
                formula="",
                hidden=False,
                description="",
                aggregation=AggregationFunction.none,
                cast=BIType.string,
                avatar_id="N/A",
            ),
        ],
        [
            datasets.SingleComponentErrorContainer(
                id="fid",
                type="field",
                errors=[
                    datasets.ComponentError(
                        message="Unknown referenced source column: non_existing_column_name",
                        code="ERR.DS_API.FORMULA.UNKNOWN_SOURCE_COLUMN",
                    )
                ]
            ),
        ],
    ),
    # TODO FIX: Uncomment when ID formula replacement will be fixed.
    #  At this moment if existing title used in GUID formula it will be resolved sucessfully
    # (
    #     "title_ref_in_guid_formula",
    #     [
    #         datasets.ResultSchemaField(
    #             guid="the_num",
    #             title="The Num",
    #             source="num",
    #             calc_mode=CalcMode.direct,
    #             formula="",
    #             hidden=False,
    #             description="",
    #             aggregation=AggregationFunction.none,
    #             cast=BIType.integer,
    #             avatar_id="N/A",
    #         ),
    #         datasets.ResultSchemaField(
    #             guid="fid",
    #             title="Broken field",
    #             source="",
    #             calc_mode=CalcMode.formula,
    #             formula="",
    #             # Use title instead of formula to trigger error
    #             guid_formula="[The Num] * 2",
    #             hidden=False,
    #             description="",
    #             aggregation=AggregationFunction.none,
    #             cast=BIType.float,
    #             avatar_id="",
    #         ),
    #     ],
    #     [
    #         datasets.SingleComponentErrorContainer(
    #             id="fid",
    #             type="field",
    #             errors=[
    #                 datasets.ComponentError(
    #                     message='Unknown field found in formula: The Num',
    #                     code='ERR.DS_API.FORMULA.UNKNOWN_FIELD_IN_FORMULA',
    #                 )
    #             ]
    #         ),
    #     ],
    # ),
    (
        "invalid_ref_in_guid_formula",
        [
            datasets.ResultSchemaField(
                guid="fid",
                title="Broken field",
                source="non_existing_column_name",
                calc_mode=CalcMode.formula,
                formula="",
                guid_formula="[not_a_field]",
                hidden=False,
                description="",
                aggregation=AggregationFunction.none,
                cast=BIType.string,
                avatar_id="N/A",
            ),
        ],
        [
            datasets.SingleComponentErrorContainer(
                id="fid",
                type="field",
                errors=[
                    datasets.ComponentError(
                        message="Unknown field found in formula: not_a_field",
                        code="ERR.DS_API.FORMULA.UNKNOWN_FIELD_IN_FORMULA",
                    )
                ]
            ),
        ],
    ),
)


@pytest.mark.parametrize(
    "fields_to_add,errors",
    [(fields, errors) for _, fields, errors in DS_VALIDATION_ERRORS_TEST_CASES],
    ids=[case[0] for case in DS_VALIDATION_ERRORS_TEST_CASES],
)
@pytest.mark.asyncio
async def test_dataset_validation_error_handling(
        bi_ext_api_test_env_bi_api_control_plane_client,
        pg_connection,
        fields_to_add: list[datasets.ResultSchemaField],
        errors: list[datasets.SingleComponentErrorContainer],
):
    int_api_cli = bi_ext_api_test_env_bi_api_control_plane_client

    source, avatar = _create_actions_to_add_pg_subsql_source(
        pg_conn_id=pg_connection.id,
        sql="SELECT 1 as num"
    )

    actions = [
        datasets.ActionDataSourceAdd(source=source),
        datasets.ActionAvatarAdd(source_avatar=avatar, disable_fields_update=True),
        *[
            datasets.ActionFieldAdd(
                order_index=idx,
                field=attr.evolve(rs_field, avatar_id=avatar.id) if rs_field.calc_mode == CalcMode.direct else rs_field
            )
            for idx, rs_field in enumerate(fields_to_add)
        ]
    ]

    with pytest.raises(exc_api.DatasetValidationError) as exc_info:
        await int_api_cli.build_dataset_config_by_actions(actions)

    exc: exc_api.DatasetValidationError = exc_info.value
    assert exc.data.dataset is not None
    assert exc.data.dataset.component_errors.items == tuple(errors)
