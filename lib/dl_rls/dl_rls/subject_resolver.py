import abc

from dl_rls.models import RLSSubject


class BaseSubjectResolver(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def get_subjects_by_names(self, names: list[str]) -> list[RLSSubject]:
        raise NotImplementedError
