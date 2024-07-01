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

    @property
    def has_restrictions(self) -> bool:
        return bool(self.items)

    @property
    def fields_with_rls(self) -> list[str]:
        return list(set(item.field_guid for item in self.items))

    def get_entries(self, field_guid: str, user_id: str) -> list[RLSEntry]:
        return [
            item
            for item in self.items
            if item.field_guid == field_guid
            and (
                # Same subject
                (item.subject.subject_type == RLSSubjectType.user and item.subject.subject_id == user_id)
                # 'all subjects' matches any subject.
                or item.subject.subject_type == RLSSubjectType.all
                # `userid: userid`
                or item.pattern_type == RLSPatternType.userid
            )
        ]

    def get_field_restriction_for_user(self, field_guid: str, user_id: str) -> FieldRestrictions:
        """
        For user and field, return `allow_all_values, allowed_values`.
        """
        rls_entries = self.get_entries(field_guid=field_guid, user_id=user_id)

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

    def get_user_restrictions(self, user_id: str) -> dict[str, list[str]]:
        result = {}
        for field_guid in self.fields_with_rls:
            allow_all_values, allow_userid, allowed_values = self.get_field_restriction_for_user(
                field_guid=field_guid, user_id=user_id
            )
            if allow_all_values:
                # `*: subject` => 'not restricted'
                continue

            # For `userid: userid`, add the subject id to the values.
            if allow_userid:
                allowed_values = list(allowed_values) + [user_id]

            result[field_guid] = allowed_values

        return result
