import logging

import attr

from bi_dls_client.dls_client import DLSClient
from bi_dls_client.exc import DLSSubjectNotFound
from dl_constants.enums import RLSSubjectType
from dl_core.exc import RLSSubjectNotFound
from dl_core.rls import (
    RLS_FAILED_USER_NAME_PREFIX,
    BaseSubjectResolver,
    RLSSubject,
)


LOGGER = logging.getLogger(__name__)


@attr.s
class DLSSubjectResolver(BaseSubjectResolver):
    _dls_client: DLSClient = attr.ib()

    def get_subjects_by_names(self, names: list[str]) -> list[RLSSubject]:
        try:
            try:
                return self._dls_client.get_subjects_by_names(names, fail_on_error=True)
            except DLSSubjectNotFound as e:
                LOGGER.info(e.message)
                missing_names = e.details.get("missing_names", [])
                names = [name for name in names if name not in missing_names]
                found_subjects = self._dls_client.get_subjects_by_names(names, fail_on_error=True)
                not_found_subjects = [
                    RLSSubject(
                        subject_id="",
                        subject_type=RLSSubjectType.notfound,
                        subject_name=RLS_FAILED_USER_NAME_PREFIX + name,
                    )
                    for name in missing_names
                ]
                return found_subjects + not_found_subjects
        except DLSSubjectNotFound as e:
            raise RLSSubjectNotFound(e.message) from e
