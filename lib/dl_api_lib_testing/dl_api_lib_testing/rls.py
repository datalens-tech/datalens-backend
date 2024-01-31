import json
import pkgutil

from dl_api_lib.utils.rls import FieldRLSSerializer
import dl_api_lib_testing.test_data
from dl_constants.enums import RLSSubjectType
from dl_core.rls import (
    BaseSubjectResolver,
    RLSEntry,
    RLSPatternType,
    RLSSubject,
)


def load_rls_config(name: str) -> str:
    data = pkgutil.get_data(dl_api_lib_testing.test_data.__name__, "rls_configs/" + name)
    return data.decode("utf-8")  # type: ignore  # 2024-01-29 # TODO: Item "None" of "bytes | None" has no attribute "decode"  [union-attr]


def load_rls(name: str) -> list[RLSEntry]:
    data = json.loads(load_rls_config(name))
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


RLS_CONFIG_CASES = [
    dict(
        name="simple",
        field_guid="c139ac51-a519-49a9-996a-49c7656fe56b",
        config=load_rls_config("simple"),
        config_to_compare=load_rls_config("simple_to_compare"),
        rls_entries=load_rls("simple.json"),
        config_updated=load_rls_config("simple_updated"),
        rls_entries_updated=load_rls("simple_updated.json"),
    ),
    dict(
        name="wildcards",
        field_guid="c139ac51-a519-49a9-996a-49c7656fe56b",
        config=load_rls_config("wildcards"),
        config_to_compare=load_rls_config("wildcards_to_compare"),
        rls_entries=load_rls("wildcards.json"),
    ),
    dict(
        name="missing_login",
        field_guid="c139ac51-a519-49a9-996a-49c7656fe56b",
        config=load_rls_config("missing_login"),
        config_to_compare=load_rls_config("missing_login_to_compare"),
        rls_entries=load_rls("missing_login.json"),
        config_updated=load_rls_config("missing_login_updated"),
        rls_entries_updated=load_rls("missing_login_updated.json"),
    ),
]


def config_to_comparable(conf: str):  # type: ignore  # 2024-01-29 # TODO: Function is missing a return type annotation  [no-untyped-def]
    return set((line.split(": ")[0], ",".join(sorted(line.split(": ")[1]))) for line in conf.strip().split("\n"))


def check_text_config_to_rls_entries(case: dict, subject_resolver: BaseSubjectResolver) -> None:
    field_guid, config, expected_rls_entries = case["field_guid"], case["config"], case["rls_entries"]
    entries = FieldRLSSerializer.from_text_config(config, field_guid, subject_resolver=subject_resolver)

    assert len(entries) == len(expected_rls_entries)

    for exp_entry in expected_rls_entries:
        matched_entries = [
            entry
            for entry in entries
            if entry.pattern_type == exp_entry.pattern_type
            and entry.allowed_value == exp_entry.allowed_value
            and entry.subject.subject_id == exp_entry.subject.subject_id
            and entry.subject.subject_type.name == exp_entry.subject.subject_type.name
            and entry.subject.subject_name == exp_entry.subject.subject_name
        ]
        assert matched_entries, ("Expected to find an entry", exp_entry)
        assert len(matched_entries) < 2, ("Expected to find exactly one entry", exp_entry)
