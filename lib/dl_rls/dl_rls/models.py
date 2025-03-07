from __future__ import annotations

from copy import deepcopy
from typing import Optional

import attr

from dl_constants.enums import (
    RLSPatternType,
    RLSSubjectType,
)


@attr.s(slots=True)
class RLSSubject:
    subject_type: RLSSubjectType = attr.ib()
    subject_id: str = attr.ib()
    subject_name: str = attr.ib()  # login, group name, etc


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
        rls_entry = deepcopy(self)
        rls_entry.subject.subject_name = rls_entry.subject.subject_name.removeprefix(RLS_FAILED_USER_NAME_PREFIX)
        return rls_entry


RLS_FAILED_USER_NAME_PREFIX = "!FAILED_"


@attr.s
class RLS2Subject:
    subject_type: RLSSubjectType = attr.ib()
    subject_id: str = attr.ib()
    subject_name: Optional[str] = attr.ib(default=None)


@attr.s
class RLS2ConfigEntry:
    subject: RLS2Subject = attr.ib()
    field_guid: Optional[str] = attr.ib(default=None)
    allowed_value: Optional[str] = attr.ib(default=None)
    pattern_type: RLSPatternType = attr.ib(default=RLSPatternType.value)
