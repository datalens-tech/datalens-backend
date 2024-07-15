import abc

import attr

from dl_api_commons.base_models import RequestContextInfo
from dl_constants.enums import RLSSubjectType
from dl_rls.models import (
    RLS_FAILED_USER_NAME_PREFIX,
    RLSSubject,
)


class BaseSubjectResolver(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def get_subjects_by_names(self, names: list[str]) -> list[RLSSubject]:
        raise NotImplementedError

    @abc.abstractmethod
    def get_groups_by_subject(self, rci: RequestContextInfo) -> list[str]:
        raise NotImplementedError


@attr.s
class NotFoundSubjectResolver(BaseSubjectResolver):
    def get_subjects_by_names(self, names: list[str]) -> list[RLSSubject]:
        return [
            RLSSubject(
                subject_id="",
                subject_type=RLSSubjectType.notfound,
                subject_name=RLS_FAILED_USER_NAME_PREFIX + name,
            )
            for name in names
        ]

    def get_groups_by_subject(self, rci: RequestContextInfo) -> list[str]:
        return []
