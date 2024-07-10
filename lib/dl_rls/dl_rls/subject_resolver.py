import abc

from dl_api_commons.base_models import RequestContextInfo
from dl_rls.models import RLSSubject


class BaseSubjectResolver(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def get_subjects_by_names(self, names: list[str]) -> list[RLSSubject]:
        raise NotImplementedError

    @abc.abstractmethod
    def get_groups_by_subject(self, rci: RequestContextInfo) -> list[str]:
        raise NotImplementedError
