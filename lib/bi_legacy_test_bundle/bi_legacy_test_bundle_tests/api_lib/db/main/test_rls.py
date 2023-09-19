from __future__ import annotations

import json
import pkgutil
from typing import (
    AsyncGenerator,
    Dict,
    Iterable,
    List,
    Optional,
)

import attr
import pytest
import responses

from bi_api_commons_ya_cloud.cloud_manager import CloudManagerAPI
from bi_api_commons_ya_cloud.models import TenantYCOrganization
from bi_cloud_integration.yc_subjects import (
    DLYCMSClient,
    SubjectInfo,
)
from bi_dls_client.dls_client import DLSClient
from bi_service_registry_ya_cloud.iam_subject_resolver import IAMSubjectResolver
from bi_service_registry_ya_team.yt_service_registry import YTServiceRegistry
from dl_api_commons.base_models import RequestContextInfo
from dl_api_lib.dataset.view import DatasetView
from dl_api_lib.query.formalization.block_formalizer import BlockFormalizer
from dl_api_lib.query.formalization.legend_formalizer import ResultLegendFormalizer
from dl_api_lib.query.formalization.raw_specs import (
    IdFieldRef,
    RawQuerySpecUnion,
    RawSelectFieldSpec,
)
from dl_api_lib.service_registry.service_registry import DefaultBiApiServiceRegistry
from dl_api_lib.utils.rls import FieldRLSSerializer
from dl_constants.enums import RLSSubjectType
from dl_core.rls import (
    RLS,
    RLSEntry,
    RLSPatternType,
    RLSSubject,
)
from dl_core.us_dataset import Dataset
from dl_core.us_manager.us_manager_async import AsyncUSManager


PKG = __name__.rsplit(".", 1)[0]


def load_config(name, binary=False):
    data = pkgutil.get_data(PKG, "rls_configs/" + name)
    if binary:
        return data
    return data.decode("utf-8")


def load_rls(name: str) -> List[RLSEntry]:
    data = json.loads(load_config(name))
    # TODO: use the actual deserialization for this?
    return [
        RLSEntry(
            field_guid="",  # not used in this case
            pattern_type=RLSPatternType[entry.get("pattern_type", "value")],
            allowed_value=entry["allowed_value"],
            subject=RLSSubject(
                subject_type=RLSSubjectType[entry["subject"]["subject_type"]],
                subject_id=entry["subject"]["subject_id"],
                subject_name=entry["subject"]["subject_name"],
            ),
        )
        for entry in data
    ]


def config_to_comparable(conf: str):
    return set((line.split(": ")[0], ",".join(sorted(line.split(": ")[1]))) for line in conf.strip().split("\n"))


TEXT_CONFIG_CASES = [
    dict(
        name="simple",
        field_guid="c139ac51-a519-49a9-996a-49c7656fe56b",
        config=load_config("simple"),
        config_to_compare=load_config("simple_to_compare"),
        rls_entries=load_rls("simple.json"),
        config_updated=load_config("simple_updated"),
        rls_entries_updated=load_rls("simple_updated.json"),
        iam_subjects=json.loads(load_config("iam_subjects.json")),
    ),
    dict(
        name="wildcards",
        field_guid="c139ac51-a519-49a9-996a-49c7656fe56b",
        config=load_config("wildcards"),
        config_to_compare=load_config("wildcards_to_compare"),
        rls_entries=load_rls("wildcards.json"),
        iam_subjects=json.loads(load_config("iam_subjects.json")),
    ),
    dict(
        name="missing_login",
        field_guid="c139ac51-a519-49a9-996a-49c7656fe56b",
        config=load_config("missing_login"),
        config_to_compare=load_config("missing_login_to_compare"),
        rls_entries=load_rls("missing_login.json"),
        config_updated=load_config("missing_login_updated"),
        rls_entries_updated=load_rls("missing_login_updated.json"),
        iam_subjects=json.loads(load_config("iam_subjects.json")),
    ),
]
MAIN_TEST_CASE = TEXT_CONFIG_CASES[0]


@pytest.fixture(scope="session")
def dls_subjects_data():
    entrysets = [
        entryset
        for case in TEXT_CONFIG_CASES
        for entryset in (case.get("rls_entries"), case.get("rls_entries_updated"))
        if entryset
    ]

    result = {
        entry.subject.subject_name: {
            "name": f"{entry.subject.subject_type.value}:{entry.subject.subject_id}",
        }
        for entryset in entrysets
        for entry in entryset
    }
    return result


@pytest.fixture(scope="session")
def iam_subjects_data() -> Iterable[SubjectInfo]:
    entrysets = [case.get("iam_subjects") for case in TEXT_CONFIG_CASES]
    subjects = []
    for entryset in entrysets:
        for entry in entryset:
            subjects.append(SubjectInfo(**entry))
    return subjects


@pytest.fixture(scope="function")
def dls_subjects_mock(app, dls_subjects_data, us_host, monkeypatch, bi_test_config, default_service_registry):
    def subjects_callback(request):
        """
        Mockup:

            POST .../render_subjects_by_login/
            {"subjects": ["subject1", "subject2"]}
            ->
            {"results": {"subject1": {...}, "subject2": {...}}}
        """
        payload = json.loads(request.body)
        try:
            subject_names = payload["subjects"]
        except KeyError:
            return 400, {}, json.dumps(dict(message="no 'subjects' in request body"))
        missing = [name for name in subject_names if name not in dls_subjects_data]
        if missing:
            return 404, {}, json.dumps(dict(error="missing_subjects", missing=missing))
        response = {"results": {subject_name: dls_subjects_data[subject_name] for subject_name in subject_names}}
        headers = {}
        return 200, headers, json.dumps(response)

    def get_subject_resolver(self):  # type: ignore
        return default_service_registry.get_installation_specific_service_registry(
            YTServiceRegistry
        ).get_subject_resolver()

    monkeypatch.setattr(DefaultBiApiServiceRegistry, "get_subject_resolver", get_subject_resolver)

    dls_subjects_url = f'{app.config["DLS_HOST"]}/_dls/batch/render_subjects_by_login/'

    responses_mock = responses.RequestsMock(
        # Without this, the fixture effectively requires to be used.
        # assert_all_requests_are_fired=False,
    )
    responses_mock.add_passthru(us_host)
    responses_mock.add_callback(
        responses.POST, dls_subjects_url, callback=subjects_callback, content_type="application/json"
    )

    with responses_mock:
        yield responses_mock


def check_text_config_to_rls_entries(case, subject_resolver):
    field_guid, config, expected_rls_entries = case["field_guid"], case["config"], case["rls_entries"]
    entries = FieldRLSSerializer.from_text_config(config, field_guid, subject_resolver=subject_resolver)

    assert len(entries) == len(expected_rls_entries)

    for exp_entry in expected_rls_entries:
        gotten_entries = [
            entry
            for entry in entries
            if entry.pattern_type == exp_entry.pattern_type
            and entry.allowed_value == exp_entry.allowed_value
            and entry.subject.subject_id == exp_entry.subject.subject_id
            and entry.subject.subject_type.name == exp_entry.subject.subject_type.name
            and entry.subject.subject_name == exp_entry.subject.subject_name
        ]
        assert gotten_entries, ("Expected to find an entry", exp_entry)
        assert len(gotten_entries) < 2, ("Expected to find exactly one entry", exp_entry)


@pytest.mark.asyncio
@pytest.mark.parametrize("case", TEXT_CONFIG_CASES, ids=[testcase["name"] for testcase in TEXT_CONFIG_CASES])
async def test_text_config_to_rls_entries_dls(case, client, dls_subjects_mock, default_service_registry_per_test):
    # ^ `client` for the flask context.
    # check default behaviour
    subject_resolver = await default_service_registry_per_test.get_subject_resolver()
    check_text_config_to_rls_entries(case, subject_resolver)


class DummyFSCli:
    async def resolve_folder_id_to_cloud_id(self, folder_id: str) -> str:
        return folder_id


@attr.s
class DummyMSCli:
    _data: Iterable[SubjectInfo] = attr.ib()

    def make_email_queries_generator(self, emails: Iterable[str]) -> Iterable[str]:
        yield ""

    async def list_members_by_emails(
        self,
        cloud_id: str = "",
        org_id: str = "",
        filter: str = "",
        emails: Optional[Iterable[str]] = None,
        max_pages: int = 9000,
        page_size: int = 1000,
    ) -> AsyncGenerator[Dict[str, SubjectInfo], None]:
        yield {subj.id: subj for subj in self._data if subj.email in emails}

    @staticmethod
    def quote_filter_string(value: str, quote: str = '"') -> str:
        return DLYCMSClient.quote_filter_string(value, quote)


@pytest.mark.asyncio
@pytest.mark.parametrize("case", TEXT_CONFIG_CASES, ids=[testcase["name"] for testcase in TEXT_CONFIG_CASES])
def test_text_config_to_rls_entries_iam(case, iam_subjects_data):
    subject_resolver = IAMSubjectResolver(
        cloud_manager=CloudManagerAPI(
            tenant=TenantYCOrganization(org_id="123"),
            yc_fs_cli=DummyFSCli(),  # type: ignore
            yc_ms_cli=DummyMSCli(data=iam_subjects_data),  # type: ignore
        ),
    )
    check_text_config_to_rls_entries(case, subject_resolver)


@pytest.mark.parametrize("case", TEXT_CONFIG_CASES, ids=[c["name"] for c in TEXT_CONFIG_CASES])
def test_rls_entries_to_text_config(case):
    expected_config = case["config_to_compare"]
    rls_entries = case["rls_entries"]

    config = FieldRLSSerializer.to_text_config(rls_entries)

    assert config_to_comparable(config) == config_to_comparable(expected_config)


def test_get_field_without_configured_rls(client, dataset_id):
    ds = client.get("/api/v1/datasets/{}/versions/draft".format(dataset_id)).json

    assert all(v == "" for v in ds["dataset"]["rls"].values())


def _create_rls_for_dataset(client, config, dataset_id):
    ds = client.get("/api/v1/datasets/{}/versions/draft".format(dataset_id)).json["dataset"]

    field_guid = ds["result_schema"][0]["guid"]

    response = client.put(
        "/api/v1/datasets/{}/versions/draft".format(dataset_id),
        data=json.dumps(
            {
                "dataset": dict(ds, rls={field_guid: config}),
            }
        ),
        content_type="application/json",
    )

    return response, field_guid


@pytest.mark.parametrize("case", TEXT_CONFIG_CASES, ids=[c["name"] for c in TEXT_CONFIG_CASES])
def test_create_and_update_rls(app, client, dataset_id, case, dls_subjects_mock):
    config = case["config"]
    response, field_guid = _create_rls_for_dataset(client, config, dataset_id)

    assert response.status_code == 200

    response = client.get(
        "/api/v1/datasets/{}/versions/draft".format(dataset_id),
    )

    assert response.status_code == 200
    assert config_to_comparable(response.json["dataset"]["rls"][field_guid]) == config_to_comparable(
        case["config_to_compare"]
    )

    config_updated = case.get("config_updated")
    if config_updated is None:
        return

    response, field_guid = _create_rls_for_dataset(client, config_updated, dataset_id)
    assert response.status_code == 200

    response = client.get("/api/v1/datasets/{}/versions/draft".format(dataset_id))

    assert response.status_code == 200
    assert config_to_comparable(response.json["dataset"]["rls"][field_guid]) == config_to_comparable(config_updated)


@pytest.fixture
def dataset_id_with_rls(app, client, dataset_id, dls_subjects_mock):
    """Mutates the dataset under the `dataset_id` fixture to have RLS on it"""

    case = MAIN_TEST_CASE
    response, _ = _create_rls_for_dataset(client, case["config"], dataset_id)

    assert response.status_code == 200
    return dataset_id


def test_create_rls_from_invalid_config(client, dataset_id):
    config = load_config("bad")
    response, _ = _create_rls_for_dataset(client, config, dataset_id)

    assert response.status_code == 400
    assert response.json["message"] == "RLS: Parsing failed at line 2"
    assert response.json["details"]["description"] == "Wrong format"


def test_create_rls_for_nonexistent_user(client, dataset_id, dls_subjects_mock):
    config = load_config("bad_login")
    response, _ = _create_rls_for_dataset(client, config, dataset_id)

    assert response.status_code == 200


@pytest.mark.asyncio
async def test_filter_expr(dataset_id, default_async_usm_per_test: AsyncUSManager):
    us_manager = default_async_usm_per_test
    ds = await us_manager.get_by_id(dataset_id, expected_type=Dataset)
    await us_manager.load_dependencies(ds)

    subject_id = "Newark"  # a hack-value to match the sample data
    subject_name = "the_test_subject"
    subject = RLSSubject(subject_id=subject_id, subject_name=subject_name, subject_type=RLSSubjectType.user)

    # result_schema: ['Category', 'City', ...]
    result_schema = ds.result_schema
    field_a = result_schema[0].guid
    field_b = result_schema[1].guid

    field_a_allowed_values = ["Technology"]
    field_b_allowed_values = ["Tucson"]
    rls_entries = (
        [RLSEntry(field_guid=field_a, subject=subject, allowed_value=value) for value in field_a_allowed_values]
        + [RLSEntry(field_guid=field_b, subject=subject, allowed_value=value) for value in field_b_allowed_values]
        + [
            RLSEntry(
                field_guid=field_b,
                subject=RLSSubject(subject_id="", subject_name="userid", subject_type=RLSSubjectType.userid),
                pattern_type=RLSPatternType.userid,
                allowed_value=None,
            )
        ]
    )

    ds.data.rls = RLS(items=rls_entries)  # TODO remove direct access to .data

    rci = RequestContextInfo(user_id=subject_id)
    raw_query_spec_union = RawQuerySpecUnion(
        select_specs=[
            RawSelectFieldSpec(ref=IdFieldRef(id=field_a)),
            RawSelectFieldSpec(ref=IdFieldRef(id=field_b)),
        ],
    )
    legend = ResultLegendFormalizer(dataset=ds).make_legend(raw_query_spec_union=raw_query_spec_union)
    block_legend = BlockFormalizer(dataset=ds).make_block_legend(
        raw_query_spec_union=raw_query_spec_union, legend=legend
    )
    ds_view = DatasetView(
        ds,
        us_manager=us_manager,
        block_spec=block_legend.blocks[0],
        rci=rci,
    )

    exec_info = ds_view.build_exec_info()
    src_query = next(iter(exec_info.translated_multi_query.iter_queries()))
    assert len(src_query.where) == 2  # field_a in ... and field_b in ...

    executed_query = await ds_view.get_data_async(exec_info=exec_info)
    field_a_values = set(row[0] for row in executed_query.rows)
    field_b_values = set(row[1] for row in executed_query.rows)
    assert executed_query.rows, "should not be empty"
    assert field_a_values == set(field_a_allowed_values)
    assert field_b_values == (set(field_b_allowed_values) | {subject_id})


def _preview_test(client, dataset_id_with_rls, data_api_v1, monkeypatch, modify_rls):
    dataset_id = dataset_id_with_rls
    ds = client.get("/api/v1/datasets/{}/versions/draft".format(dataset_id)).json
    rls_val_modifier = "\n'x': *\n" if modify_rls else ""
    dataset_data = ds["dataset"]
    preview_req = {
        "dataset": {
            "result_schema": dataset_data["result_schema"],
            "rls": {key: val + rls_val_modifier for key, val in dataset_data["rls"].items() if val},
            "source_avatars": dataset_data["source_avatars"],
            "sources": dataset_data["sources"],
            "revision_id": dataset_data["revision_id"],
        },
        "limit": 13,
    }

    def catch_mock(*args, **kwargs):
        raise Exception("Should not be here")

    monkeypatch.setattr(DLSClient, "_request", catch_mock)
    response = data_api_v1.get_response_for_dataset_preview(
        dataset_id=dataset_id,
        raw_body=preview_req,
    )
    return response


def test_preview_with_saved_rls(client, dataset_id_with_rls, data_api_v1, monkeypatch):
    preview_response = _preview_test(client, dataset_id_with_rls, data_api_v1, monkeypatch, modify_rls=False)
    rd = preview_response.json
    assert preview_response.status_code == 200, rd
    assert preview_response


def test_preview_with_updated_rls(client, dataset_id_with_rls, data_api_v1, monkeypatch):
    preview_response = _preview_test(client, dataset_id_with_rls, data_api_v1, monkeypatch, modify_rls=True)
    rd = preview_response.json
    assert preview_response.status_code == 400, rd
    assert "save dataset after editing the RLS" in rd["message"]
