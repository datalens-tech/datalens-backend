import json
import pkgutil
from typing import Optional

import attr

from dl_constants.enums import RLSSubjectType
from dl_rls.models import (
    RLSEntry,
    RLSPatternType,
    RLSSubject,
)
from dl_rls.serializer import FieldRLSSerializer
from dl_rls.subject_resolver import BaseSubjectResolver


@attr.s
class RLS2ConfigEntry:
    # model only for testing with allowed missing values
    subject: RLSSubject = attr.ib()
    field_guid: Optional[str] = attr.ib(default=None)
    allowed_value: Optional[str] = attr.ib(default=None)
    pattern_type: RLSPatternType = attr.ib(default=RLSPatternType.value)


def load_rls_config(name: str) -> str:
    data = pkgutil.get_data(__package__, f"rls_configs/{name}")
    assert data is not None
    return data.decode("utf-8")


def load_rls_v2_config(name: str) -> list[RLS2ConfigEntry]:
    data = json.loads(load_rls_config(name))
    result: list[RLS2ConfigEntry] = []
    for entry in data:
        rls_entry = RLS2ConfigEntry(**entry)
        rls_entry.pattern_type = RLSPatternType[entry.get("pattern_type", "value")]
        rls_entry.subject = RLSSubject(**entry["subject"])
        rls_entry.subject.subject_type = RLSSubjectType[entry["subject"]["subject_type"]]
        result.append(rls_entry)
    return result


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
        config=load_rls_config("simple"),
        config_v2=load_rls_v2_config("simple_v2.json"),
        config_to_compare=load_rls_config("simple_to_compare"),
        config_to_compare_v2=load_rls_config("simple_v1_from_v2"),
        rls_entries=load_rls("simple.json"),
        config_updated=load_rls_config("simple_updated"),
        rls_entries_updated=load_rls("simple_updated.json"),
    ),
    dict(
        name="wildcards",
        config=load_rls_config("wildcards"),
        config_to_compare=load_rls_config("wildcards_to_compare"),
        rls_entries=load_rls("wildcards.json"),
    ),
    dict(
        name="groups",
        config=load_rls_config("groups"),
        config_to_compare=load_rls_config("groups_to_compare"),
        rls_entries=load_rls("groups.json"),
    ),
    dict(
        name="missing_login",
        config=load_rls_config("missing_login"),
        config_to_compare=load_rls_config("missing_login_to_compare"),
        rls_entries=load_rls("missing_login.json"),
        config_updated=load_rls_config("missing_login_updated"),
        rls_entries_updated=load_rls("missing_login_updated.json"),
    ),
]


def config_to_comparable(conf: str) -> set[tuple]:
    return set((line.split(": ")[0], ",".join(sorted(line.split(": ")[1]))) for line in conf.strip().split("\n"))


def check_text_config_to_rls_entries(case: dict, subject_resolver: BaseSubjectResolver) -> None:
    config, expected_rls_entries = case["config"], case["rls_entries"]
    entries = FieldRLSSerializer.from_text_config(config, "field_guid", subject_resolver=subject_resolver)

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
