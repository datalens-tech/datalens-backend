import dl_pydantic
import dl_temporal


class ActivityParams(dl_temporal.BaseActivityParams):
    activity_int_param: int
    activity_str_param: str
    activity_bool_param: bool
    activity_list_param: list[int]
    activity_dict_param: dict[str, int]
    activity_timedelta_param: dl_pydantic.JsonableTimedelta
    activity_uuid_param: dl_pydantic.JsonableUUID
    activity_date_param: dl_pydantic.JsonableDate
    activity_datetime_param: dl_pydantic.JsonableDatetime

    start_to_close_timeout: dl_pydantic.JsonableTimedelta = dl_pydantic.JsonableTimedelta(seconds=30)


class ActivityResult(dl_temporal.BaseActivityResult):
    activity_int_result: int
    activity_str_result: str
    activity_bool_result: bool
    activity_list_result: list[int]
    activity_dict_result: dict[str, int]
    activity_timedelta_result: dl_pydantic.JsonableTimedelta
    activity_uuid_result: dl_pydantic.JsonableUUID
    activity_date_result: dl_pydantic.JsonableDate
    activity_datetime_result: dl_pydantic.JsonableDatetime


@dl_temporal.define_activity
class Activity(dl_temporal.BaseActivity):
    name = "test_activity"
    Params = ActivityParams
    Result = ActivityResult

    async def run(self, params: ActivityParams) -> ActivityResult:
        return self.Result(
            activity_int_result=params.activity_int_param + 1,
            activity_str_result=params.activity_str_param,
            activity_bool_result=params.activity_bool_param,
            activity_list_result=params.activity_list_param,
            activity_dict_result=params.activity_dict_param,
            activity_timedelta_result=params.activity_timedelta_param,
            activity_uuid_result=params.activity_uuid_param,
            activity_datetime_result=params.activity_datetime_param,
            activity_date_result=params.activity_date_param,
        )
