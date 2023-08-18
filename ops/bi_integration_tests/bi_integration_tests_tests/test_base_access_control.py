from __future__ import annotations

import uuid

import pytest
import shortuuid

from bi_integration_tests import datasets
from bi_integration_tests.common import get_dataset_json_from_validation, get_sample_db_dataset_json
from bi_integration_tests.request_executors.bi_api_client import get_connection_json
from bi_integration_tests.steps import ScenarioStep, StepExecutor
from bi_testing.dlenv import DLEnv
from bi_testing.utils import skip_outside_devhost

tag_init = "tag_init"
tag_cleanup = "tag_cleanup"


main_api_scenario = [
    # Connection
    ScenarioStep(
        "post", "/api/v1/connections/test_connection_params",
        data=lambda context: get_connection_json(base_dir=context['base_dir'], **(context["pg_0"])),
    ),
    ScenarioStep(
        "post", "/api/v1/connections/",
        data=lambda context: get_connection_json(base_dir=context['base_dir'], **(context["pg_0"])),
        response_handler=lambda resp, context: context.update(pg_conn_id=resp["id"]),
        tags={tag_init},
    ),
    ScenarioStep(
        "put", "/api/v1/connections/{pg_conn_id}/",
        data=lambda context: get_connection_json(
            edit=True, name=f"pg_{shortuuid.random(8)}_renamed", base_dir=context['base_dir'], **(context["pg_0"])
        )
    ),
    ScenarioStep("get", "/api/v1/connections/{pg_conn_id}/"),
    ScenarioStep("get", "/api/v1/connections/{pg_conn_id}/info/sources"),
    #
    # Dataset
    #
    ScenarioStep(
        "post", "/api/data/v1/datasets/data/preview",
        data=lambda context: get_sample_db_dataset_json(context['pg_conn_id'], test_dataset=datasets.PG_SQL_FEATURES)),
    ScenarioStep(
        "post", "/api/v1/datasets/validators/dataset",
        data=lambda context: get_sample_db_dataset_json(context['pg_conn_id'], test_dataset=datasets.PG_SQL_FEATURES),
        response_handler=lambda resp, context: context.update(
            pg_dataset_validation_1_resp=resp,
            pg_dataset_str_field_id=next(
                field["guid"] for field in resp["dataset"]["result_schema"] if field["title"] == 'feature_name'
            ),
            pg_dataset_to_delete_field_id=next(
                field["guid"] for field in resp["dataset"]["result_schema"] if field["title"] == 'is_verified_by'
            ),
        ),
        tags={tag_init},
    ),
    ScenarioStep(
        "post", "/api/v1/datasets/",
        data=lambda context: get_dataset_json_from_validation(
            dataset_name=f"Test PG dataset {shortuuid.random(6)}", base_dir=context["base_dir"],
            validation_response_dataset=context["pg_dataset_validation_1_resp"]["dataset"]
        ),
        response_handler=lambda resp, context: context.update(
            pg_dataset_id=resp["id"],
        ),
        tags={tag_init},
    ),
    ScenarioStep(
        "post", "/api/v1/datasets/{pg_dataset_id}/versions/draft/validators/schema",
        data=lambda context: {
            "updates": [
                {
                    "action": "delete_field",
                    "field": {"guid": context["pg_dataset_to_delete_field_id"]}
                }
            ],
        },
    ),
    ScenarioStep(
        "put", "/api/v1/datasets/{pg_dataset_id}/versions/draft/",
        data=lambda context: get_dataset_json_from_validation(
            validation_response_dataset=context["pg_dataset_validation_1_resp"]["dataset"]
        )
    ),
    ScenarioStep("get", "/api/v1/datasets/{pg_dataset_id}/versions/draft/"),
    ScenarioStep("get", "/api/v1/datasets/{pg_dataset_id}/fields"),

    ScenarioStep(
        "post", "/api/v1/datasets/{pg_dataset_id}/copy",
        data=lambda context: {
            "new_key": f"{context['base_dir']}/dataset {context['pg_dataset_id']} copy",
        },
        response_handler=lambda resp, context: context.update(pg_dataset_copy_id=resp["id"]),
        tags={tag_init},
    ),
    ScenarioStep("delete", "/api/v1/datasets/{pg_dataset_copy_id}", tags={tag_cleanup}),
    ScenarioStep("post", "/api/v1/datasets/validators/field", data=lambda context: {
        "multisource": True,
        **{
            k: context["pg_dataset_validation_1_resp"]["dataset"][k]
            for k in ("result_schema", "rls", "sources", "source_avatars", "avatar_relations")
        },
        "updates": [],
        'field': {'guid': str(uuid.uuid4()), 'title': "formula_field", 'formula': "count(1)"}
    }),
    ScenarioStep("post", "/api/v1/datasets/{pg_dataset_id}/versions/draft/validators/field", data={
        'field': {'guid': str(uuid.uuid4()), 'title': "formula_field", 'formula': "count(1)"}
    }),
    #
    # Dataset data
    #
    ScenarioStep("post", "/api/data/v1/datasets/{pg_dataset_id}/versions/draft/result", data=lambda context: {
        "columns": [context["pg_dataset_str_field_id"]],
    }),
    ScenarioStep("post", "/api/data/v1/datasets/{pg_dataset_id}/versions/draft/values/distinct", data=lambda context: {
        "field_guid": context["pg_dataset_str_field_id"],
    }),
    ScenarioStep("post", "/api/data/v1/datasets/{pg_dataset_id}/versions/draft/values/range", data=lambda context: {
        "field_guid": context["pg_dataset_str_field_id"],
    }),
    ScenarioStep("post", "/api/data/v1/datasets/{pg_dataset_id}/versions/draft/preview", data=lambda context: {
        "columns": [context["pg_dataset_str_field_id"]],
    }),
    ScenarioStep("get", "/api/data/v1/datasets/{pg_dataset_id}/fields"),
    #
    # Info
    #
    ScenarioStep("get", "/api/v1/info/field_types"),
    ScenarioStep("post", "/api/v1/info/datasets_publicity_checker", data=lambda context: {
        "datasets": [context["pg_dataset_id"]]
    }),
    ScenarioStep("get", "/api/v1/info/connectors"),
    ScenarioStep("get", "/api/v1/info/internal/pseudo_workbook/{base_dir}"),
    #
    # Finalizing
    #
    ScenarioStep("delete", "/api/v1/datasets/{pg_dataset_id}", tags={tag_cleanup}),
    ScenarioStep("delete", "/api/v1/connections/{pg_conn_id}/", tags={tag_cleanup}),
]


@pytest.mark.parametrize("dl_env", [
    DLEnv.cloud_preprod,
    DLEnv.cloud_prod,
], indirect=True)
@skip_outside_devhost
@pytest.mark.asyncio
async def test_no_access_for_user_without_datalens_role(
    dl_env,
    integration_tests_reporter,
    role_check_user_configuration,
    integration_tests_folder_id,
    integration_tests_postgres_1,
    ext_sys_requisites,
    tenant,
    workbook_id
):
    base_url_map = {
        "main": ext_sys_requisites.DATALENS_API_LB_MAIN_BASE_URL,
        "uploads": ext_sys_requisites.DATALENS_API_LB_UPLOADS_BASE_URL,
    }

    base_dir = "access_control_tests/test_no_access_for_user_without_datalens_role"

    ok_step_executor = StepExecutor(
        base_url_map,
        folder_id=integration_tests_folder_id,
        subject_user_creds=role_check_user_configuration.with_dl_inst_use_role,
        tenant=tenant
    )

    forbidden_step_executor = StepExecutor(
        base_url_map,
        folder_id=integration_tests_folder_id,
        subject_user_creds=role_check_user_configuration.without_dl_inst_use_role,
        tenant=tenant
    )

    default_init_context = dict(
        base_dir=base_dir,
        pg_0=integration_tests_postgres_1,
        workbook_id=workbook_id
    )

    #
    # Ok tests
    #
    ok_context = dict(default_init_context)

    with integration_tests_reporter.step_exc_handler("Has role results (failed)"):
        ok_execution_reports = await ok_step_executor.execute_all_steps(
            main_api_scenario,
            ok_context,
            handle_responses=True,
        )

    integration_tests_reporter.send_steps_exec_results(
        "Has role results",
        ok_execution_reports,
    )

    integration_tests_reporter.send_steps_exec_results(
        "Has role results (unexpected status code)",
        [
            exec_rpt for exec_rpt in ok_execution_reports
            if exec_rpt.response.status != exec_rpt.step.expected_status
        ],
    )

    #
    # Forbidden tests
    #
    init_only_scenario = [step for step in main_api_scenario if tag_init in step.tags]
    cleanup_only_scenario = [step for step in main_api_scenario if tag_cleanup in step.tags]

    forbidden_context = dict(default_init_context)

    # Initializing context (creating entities that will be used in URL/requests for )
    init_execution_reports = await ok_step_executor.execute_all_steps(
        init_only_scenario, forbidden_context, handle_responses=True
    )
    # Ensure that initialization done successfully
    assert all([exec_rpt.response.status == exec_rpt.step.expected_status for exec_rpt in init_execution_reports])

    try:
        with integration_tests_reporter.step_exc_handler("No role results (failed)"):
            no_role_execution_reports = await forbidden_step_executor.execute_all_steps(
                main_api_scenario, forbidden_context, handle_responses=False
            )

        integration_tests_reporter.send_steps_exec_results(
            "No role results",
            no_role_execution_reports
        )

    finally:
        cleanup_reports = await ok_step_executor.execute_all_steps(
            cleanup_only_scenario, forbidden_context, handle_responses=True, ignore_step_exceptions=True
        )
        integration_tests_reporter.send_steps_exec_results(
            "Final cleanup results",
            cleanup_reports
        )

    assert all(exec_rpt.response.status == 403 for exec_rpt in no_role_execution_reports), \
        "Not all handles return 403 for user without appropriate permissions: \n" + "\n".join([
            exec_rpt.to_short_string()
            for exec_rpt in no_role_execution_reports
        ])
