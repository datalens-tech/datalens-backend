"""
RLS - Row-Level Security

Module implements RLS management functionality
"""

from __future__ import annotations

from typing import (
    NamedTuple,
    Optional,
    cast,
)

import attr

from dl_constants.enums import (
    RLSPatternType,
    RLSSubjectType,
)
from dl_rls.models import RLSEntry


class FieldRestrictions(NamedTuple):
    allow_all_values: bool
    allow_userid: bool
    allowed_values: list[str]


@attr.s
class RLS:
    items: list[RLSEntry] = attr.ib(factory=list)
    allowed_groups: set[str] = attr.ib(factory=set)

    @property
    def has_restrictions(self) -> bool:
        return bool(self.items)

    @property
    def fields_with_rls(self) -> list[str]:
        return list(set(item.field_guid for item in self.items))

    def get_entries(
        self, field_guid: str, subject_type: RLSSubjectType, subject_id: str, add_userid_entry: bool = True
    ) -> list[RLSEntry]:
        return [
            item
            for item in self.items
            if item.field_guid == field_guid
            and (
                # Same subject
                (item.subject.subject_type == subject_type and item.subject.subject_id == subject_id)
                # user is in the group
                or (
                    item.subject.subject_type == RLSSubjectType.group and item.subject.subject_id in self.allowed_groups
                )
                # 'all subjects' matches any subject.
                or item.subject.subject_type == RLSSubjectType.all
                # `userid: userid`
                or (add_userid_entry and item.pattern_type == RLSPatternType.userid)
            )
        ]

    def get_field_restriction_for_subject(
        self,
        field_guid: str,
        subject_type: RLSSubjectType,
        subject_id: str,
    ) -> FieldRestrictions:
        """
        For subject and field, return `allow_all_values, allowed_values`.
        """
        rls_entries = self.get_entries(field_guid=field_guid, subject_type=subject_type, subject_id=subject_id)

        # There's a `*: {current_user}` entry, no need to filter.
        if any(rls_entry.pattern_type == RLSPatternType.all for rls_entry in rls_entries):
            return FieldRestrictions(allow_all_values=True, allow_userid=False, allowed_values=[])

        # Pick out userid-entry, if any
        userid_entry: Optional[RLSEntry] = None
        for rls_entry in rls_entries:
            if rls_entry.pattern_type != RLSPatternType.userid:
                continue
            if userid_entry is not None:
                raise ValueError("Expected no more than one userid entries")
            userid_entry = rls_entry
        if userid_entry is not None:
            rls_entries.remove(userid_entry)

        # normal values
        assert all(
            rls_entry.pattern_type == RLSPatternType.value for rls_entry in rls_entries
        ), "only simple values should remain at this point"
        allowed_values = cast(list[str], [rls_entry.allowed_value for rls_entry in rls_entries])  # cast for mypy
        assert all(value is not None for value in allowed_values)

        # `userid: userid` case
        allow_userid = False
        if userid_entry is not None:
            assert userid_entry.pattern_type == RLSPatternType.userid
            assert userid_entry.allowed_value is None
            # only `userid: userid` makes sense
            assert userid_entry.subject.subject_type == RLSSubjectType.userid
            allow_userid = True

        return FieldRestrictions(allow_all_values=False, allow_userid=allow_userid, allowed_values=allowed_values)

    def get_subject_restrictions(
        self,
        subject_type: RLSSubjectType,
        subject_id: str,
    ) -> dict[str, list[str]]:
        result = {}
        for field_guid in self.fields_with_rls:
            allow_all_values, allow_userid, allowed_values = self.get_field_restriction_for_subject(
                field_guid=field_guid, subject_type=subject_type, subject_id=subject_id
            )
            if allow_all_values:
                # `*: subject` => 'not restricted'
                continue

            # For `userid: userid`, add the subject id to the values.
            if allow_userid:
                allowed_values = list(allowed_values) + [subject_id]

            result[field_guid] = allowed_values

        return result
