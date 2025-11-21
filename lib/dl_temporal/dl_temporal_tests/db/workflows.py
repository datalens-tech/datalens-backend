import temporalio.workflow


with temporalio.workflow.unsafe.imports_passed_through():
    import dl_pydantic
    import dl_temporal_tests.db.activities as activities

import dl_temporal


class WorkflowParams(dl_temporal.BaseWorkflowParams):
    workflow_int_param: int
    workflow_str_param: str
    workflow_bool_param: bool
    workflow_list_param: list[int]
    workflow_dict_param: dict[str, int]
    workflow_timedelta_param: dl_pydantic.JsonableTimedelta
    workflow_uuid_param: dl_pydantic.JsonableUUID
    workflow_date_param: dl_pydantic.JsonableDate
    workflow_datetime_param: dl_pydantic.JsonableDatetime
    workflow_datetime_with_timezone_param: dl_pydantic.JsonableDatetimeWithTimeZone

    execution_timeout: dl_pydantic.JsonableTimedelta = dl_pydantic.JsonableTimedelta(seconds=1)


class WorkflowResult(dl_temporal.BaseWorkflowResult):
    workflow_int_result: int
    workflow_str_result: str
    workflow_bool_result: bool
    workflow_list_result: list[int]
    workflow_dict_result: dict[str, int]
    workflow_timedelta_result: dl_pydantic.JsonableTimedelta
    workflow_uuid_result: dl_pydantic.JsonableUUID
    workflow_date_result: dl_pydantic.JsonableDate
    workflow_datetime_result: dl_pydantic.JsonableDatetime
    workflow_datetime_with_timezone_result: dl_pydantic.JsonableDatetimeWithTimeZone


@dl_temporal.define_workflow
class Workflow(dl_temporal.BaseWorkflow):
    name = "test_workflow"
    Params = WorkflowParams
    Result = WorkflowResult

    async def run(self, params: WorkflowParams) -> WorkflowResult:
        result = await self.execute_activity(
            activities.Activity,
            activities.Activity.Params(
                activity_int_param=params.workflow_int_param + 1,
                activity_str_param=params.workflow_str_param,
                activity_bool_param=params.workflow_bool_param,
                activity_list_param=params.workflow_list_param,
                activity_dict_param=params.workflow_dict_param,
                activity_timedelta_param=params.workflow_timedelta_param,
                activity_uuid_param=params.workflow_uuid_param,
                activity_date_param=params.workflow_date_param,
                activity_datetime_param=params.workflow_datetime_param,
                activity_datetime_with_timezone_param=params.workflow_datetime_with_timezone_param,
            ),
        )
        return self.Result(
            workflow_int_result=result.activity_int_result + 1,
            workflow_str_result=result.activity_str_result,
            workflow_bool_result=result.activity_bool_result,
            workflow_list_result=result.activity_list_result,
            workflow_dict_result=result.activity_dict_result,
            workflow_timedelta_result=result.activity_timedelta_result,
            workflow_uuid_result=result.activity_uuid_result,
            workflow_date_result=result.activity_date_result,
            workflow_datetime_result=result.activity_datetime_result,
            workflow_datetime_with_timezone_result=result.activity_datetime_with_timezone_result,
        )
