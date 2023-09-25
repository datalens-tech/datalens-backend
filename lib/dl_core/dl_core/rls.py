"""
RLS - Row-Level Security

Module implements RLS management functionality
"""

from __future__ import annotations

import abc
import copy
from typing import (
    Dict,
    List,
    Optional,
    Tuple,
)

import attr

from dl_constants.enums import (
    RLSPatternType,
    RLSSubjectType,
)
from dl_utils.utils import split_list


@attr.s(slots=True)
class RLSSubject:
    subject_type: RLSSubjectType = attr.ib()
    subject_id: str = attr.ib()
    subject_name: str = attr.ib()  # login, group name, etc


# Special type subject that denotes 'all subjects'.
RLS_ALL_SUBJECT_NAME = "*"
RLS_ALL_SUBJECT = RLSSubject(
    subject_type=RLSSubjectType.all, subject_id=RLS_ALL_SUBJECT_NAME, subject_name=RLS_ALL_SUBJECT_NAME
)
RLS_FAILED_USER_NAME_PREFIX = "!FAILED_"
# Special type subject that denotes 'insert userid'.
RLS_USERID_SUBJECT_NAME = "userid"
RLS_USERID_SUBJECT = RLSSubject(subject_type=RLSSubjectType.userid, subject_id="", subject_name=RLS_USERID_SUBJECT_NAME)


@attr.s(slots=True)
class RLSEntry:
    field_guid: str = attr.ib()
    allowed_value: Optional[str] = attr.ib()
    subject: RLSSubject = attr.ib()
    # Note: this is a bit of a hack to avoid very extensive splitting of the
    # RLSEntry into multiple classes.
    # For `pattern_type=RLSPatternType.all`, `allowed_value` must be `None`.
    # For `pattern_type=RLSPatternType.userid`, `allowed_value` must be `None`,
    # and `subject` must be `RLSSubjectType.userid`.
    pattern_type: RLSPatternType = attr.ib(default=RLSPatternType.value)

    def ensure_removed_failed_subject_prefix(self) -> RLSEntry:
        rls_entry = copy.deepcopy(self)
        rls_entry.subject.subject_name = rls_entry.subject.subject_name.removeprefix(RLS_FAILED_USER_NAME_PREFIX)
        return rls_entry


@attr.s
class RLS:
    items: List[RLSEntry] = attr.ib(factory=list)

    @property
    def has_restrictions(self) -> bool:
        return bool(self.items)

    @property
    def fields_with_rls(self) -> List[str]:
        return list(set(item.field_guid for item in self.items))

    def get_entries(
        self, field_guid: str, subject_type: RLSSubjectType, subject_id: str, add_userid_entry: bool = True
    ) -> List[RLSEntry]:
        return [
            item
            for item in self.items
            if item.field_guid == field_guid
            and (
                # Same subject
                (item.subject.subject_type == subject_type and item.subject.subject_id == subject_id)
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
    ) -> Tuple[bool, bool, Optional[List[str]]]:
        """
        For subject and field, return `allow_all_values, allowed_values`.
        """
        rls_entries = self.get_entries(field_guid=field_guid, subject_type=subject_type, subject_id=subject_id)

        # There's a `*: {current_user}` entry, no need to filter.
        if any(rls_entry.pattern_type == RLSPatternType.all for rls_entry in rls_entries):
            return True, False, None

        # Pick out userid-entry, if any
        userid_entries, rls_entries = split_list(
            rls_entries, lambda rls_entry: rls_entry.pattern_type == RLSPatternType.userid
        )

        # normal values
        assert all(
            rls_entry.pattern_type == RLSPatternType.value for rls_entry in rls_entries
        ), "only simple values should remain at this point"
        allowed_values = [rls_entry.allowed_value for rls_entry in rls_entries]

        # `userid: userid` case
        allow_userid = False
        if userid_entries:
            assert len(userid_entries) == 1
            userid_entry = userid_entries[0]
            assert userid_entry.pattern_type == RLSPatternType.userid
            assert userid_entry.allowed_value is None
            # only `userid: userid` makes sense
            assert userid_entry.subject.subject_type == RLSSubjectType.userid
            allow_userid = True

        return False, allow_userid, allowed_values  # type: ignore  # TODO: fix

    def _should_add_entry(
        self,
        field_guid: str,
        subject_type: RLSSubjectType,
        subject_id: str,
        allowed_value: Optional[str],
        pattern_type: RLSPatternType,
    ) -> bool:
        existing_allow_all_values, allow_userid, existing_allowed_values = self.get_field_restriction_for_subject(
            field_guid=field_guid, subject_type=subject_type, subject_id=subject_id
        )
        if existing_allow_all_values:  # already wildcarded, skip
            return False
        if pattern_type == RLSPatternType.userid:
            return not allow_userid  # add if it's not already set.
        # Add if not existing:
        return allowed_value not in existing_allowed_values  # type: ignore  # TODO: fix

    def add_field_restriction_for_subject(
        self,
        field_guid: str,
        subject_type: RLSSubjectType,
        subject_id: str,
        subject_name: str,
        allowed_value: Optional[str],
        pattern_type: RLSPatternType = RLSPatternType.value,
        force: bool = False,
    ) -> None:
        """
        Register a new RLS restriction.

        Currently, only used in tests.
        """
        if force or self._should_add_entry(
            field_guid=field_guid,
            subject_type=subject_type,
            subject_id=subject_id,
            allowed_value=allowed_value,
            pattern_type=pattern_type,
        ):
            # TODO?: if allow_all_values, drop all prior items for subject+field?
            self.items.append(
                RLSEntry(
                    field_guid=field_guid,
                    allowed_value=allowed_value,
                    pattern_type=pattern_type,
                    subject=RLSSubject(
                        subject_type=subject_type,
                        subject_name=subject_name,
                        subject_id=subject_id,
                    ),
                )
            )

    def get_subject_restrictions(
        self,
        subject_type: RLSSubjectType,
        subject_id: str,
    ) -> Dict[str, List[str]]:
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
                allowed_values = list(allowed_values) + [subject_id]  # type: ignore  # TODO: fix

            result[field_guid] = allowed_values

        return result  # type: ignore  # TODO: fix


class BaseSubjectResolver(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def get_subjects_by_names(self, names: List[str]) -> List[RLSSubject]:
        raise NotImplementedError
