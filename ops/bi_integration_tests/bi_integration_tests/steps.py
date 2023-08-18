from __future__ import annotations

from typing import Union, Dict, List, Any, Optional, Callable, Set, Sequence

import attr
from aiohttp import FormData, ClientSession
from bi_api_commons.base_models import TenantDef

from bi_testing.api_wrappers import Req, Resp, APIClient, HTTPClientWrapper
from bi_testing.cloud_tokens import AccountCredentials

_DataType = Union[Dict, List, FormData, bytes, str]
_Context = Dict[str, Any]


def file_form(
        file_content: bytes,
        file_content_type: str,
        file_name: str,
        other: Optional[Dict[str, str]]
) -> FormData:
    form_data = FormData()
    form_data.add_field("file", file_content, content_type=file_content_type, filename=file_name)
    if other:
        for key, value in other.items():
            form_data.add_field(key, value)
    return form_data


@attr.s(auto_attribs=True, frozen=True)
class ScenarioStep:
    method: str
    url: str
    params: Union[None, Dict[str, Union[str, int]], Callable[[_Context], Dict[str, Union[str, int]]]] = None
    headers: Union[None, Dict[str, str], Callable[[_Context], Dict[str, str]]] = None
    data: Union[None, _DataType, Callable[[_Context], _DataType]] = None
    response_handler: Optional[Callable[[Dict, _Context], Any]] = None
    expected_status: int = 200
    tags: Set[str] = attr.ib(factory=set)
    api_code: str = "main"


@attr.s(auto_attribs=True, frozen=True)
class StepExecutionReport:
    step: ScenarioStep
    request: Optional[Req]
    response: Optional[Resp]
    exception: Optional[Exception]

    def to_short_string(self):
        return f"{self.response.status} {self.step.method:6} {self.step.url} {self.response.req_id}"


class StepExecutionException(Exception):
    def __init__(
            self,
            msg: str,
            step: ScenarioStep,
            context: _Context,
            done_steps: Optional[Sequence[StepExecutionReport]]
    ):
        super().__init__(msg)
        self.step = step
        self.context = context
        self.done_steps = done_steps


@attr.s
class StepExecutor:
    base_url_map: Dict[str, str] = attr.ib()
    subject_user_creds: AccountCredentials = attr.ib()
    folder_id: str = attr.ib()
    tenant: Optional[TenantDef] = attr.ib()

    _api_client_map: Dict[str, APIClient] = attr.ib(init=False, default=None)

    def __attrs_post_init__(self):
        self._api_client_map = {
            api_code: self._create_api_client(
                base_url=base_url,
                folder_id=self.folder_id,
                user_creds=self.subject_user_creds,
                tenant=self.tenant
            )
            for api_code, base_url in self.base_url_map.items()
        }

    @staticmethod
    def _create_api_client(
            base_url: str,
            folder_id: str,
            user_creds: AccountCredentials,
            tenant: Optional[TenantDef] = None
    ):
        return APIClient(
            HTTPClientWrapper(
                session=ClientSession(),
                base_url=base_url,
            ),
            folder_id=folder_id,
            account_credentials=user_creds,
            tenant=tenant
        )

    async def execute_all_steps(
            self,
            steps: Sequence[ScenarioStep],
            initial_context: Dict[str, Any],
            handle_responses: bool = False,
            ignore_step_exceptions: bool = False
    ):
        report_list: List[StepExecutionReport] = []
        for step in steps:
            try:
                report = await self.execute_step(
                    step, initial_context,
                    handle_responses=handle_responses,
                    ignore_step_exceptions=ignore_step_exceptions,
                )
                report_list.append(report)
            except StepExecutionException as exc:
                exc.done_steps = tuple(report_list)
                raise exc
            except Exception as exc:
                raise StepExecutionException(
                    "Exception during step execution",
                    step,
                    dict(initial_context),
                    tuple(report_list),
                ) from exc
        return tuple(report_list)

    @staticmethod
    def _prepare_request(step: ScenarioStep, context: _Context) -> Req:
        request = Req(step.method, step.url.format(**context), params=step.params, require_ok=False)

        data = step.data(context) if callable(step.data) else step.data
        if isinstance(data, (dict, list)):
            request = attr.evolve(request, data_json=data)
        elif data is None:
            pass
        else:
            request = attr.evolve(request, data=data)

        headers = step.headers(context) if callable(step.headers) else step.headers

        if headers:
            request = attr.evolve(request, extra_headers=headers)

        params = step.params(context) if callable(step.params) else step.params
        if params:
            request = attr.evolve(request, params=params)
        return request

    async def execute_step(
            self,
            step: ScenarioStep,
            context: _Context,
            handle_responses: bool = False,
            ignore_step_exceptions: bool = False
    ) -> StepExecutionReport:
        try:
            request = self._prepare_request(step, context)
        except Exception as exc:
            if ignore_step_exceptions:
                return StepExecutionReport(step, None, None, exc)
            else:
                raise StepExecutionException(
                    "Exception during constructing HTTP request for step",
                    step,
                    context,
                    done_steps=None,  # To be replaced
                ) from exc
        try:
            client = self._api_client_map[step.api_code]
            response = await client.make_request(request)
        except Exception as exc:
            if ignore_step_exceptions:
                return StepExecutionReport(step, request, None, exc)
            else:
                raise StepExecutionException(
                    "Exception during sending HTTP request for step",
                    step,
                    context,
                    done_steps=None,  # To be replaced
                ) from exc

        if handle_responses and step.response_handler is not None and response.status == step.expected_status:
            try:
                step.response_handler(response.json, context)
            except Exception as exc:
                if not ignore_step_exceptions:
                    raise StepExecutionException(
                        "Exception during handling HTTP response",
                        step,
                        context,
                        done_steps=None,  # To be replaced
                    ) from exc

        return StepExecutionReport(step, request, response, None)
