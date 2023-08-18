import attr
import logging

from bi_constants.enums import RLSSubjectType
from bi_core.rls import BaseSubjectResolver, RLSSubject, RLS_FAILED_USER_NAME_PREFIX

from bi_dls_client.dls_client import DLSClient
from bi_dls_client.exc import DLSSubjectNotFound

LOGGER = logging.getLogger(__name__)


@attr.s
class DLSSubjectResolver(BaseSubjectResolver):
    _dls_client: DLSClient = attr.ib()

    def get_subjects_by_names(self, names: list[str]) -> list[RLSSubject]:
        try:
            return self._dls_client.get_subjects_by_names(names, fail_on_error=True)
        except DLSSubjectNotFound as e:
            LOGGER.info(e.message)
            missing_names = e.details.get('missing_names', [])
            names = [name for name in names if name not in missing_names]
            found_subjects = self._dls_client.get_subjects_by_names(names, fail_on_error=True)
            not_found_subjects = [RLSSubject(
                subject_id='',
                subject_type=RLSSubjectType.notfound,
                subject_name=RLS_FAILED_USER_NAME_PREFIX + name,
            ) for name in missing_names]
            return found_subjects + not_found_subjects
