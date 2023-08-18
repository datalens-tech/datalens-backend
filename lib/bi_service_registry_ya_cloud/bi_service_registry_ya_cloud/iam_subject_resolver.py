import logging

import attr

from bi_api_commons.cloud_manager import CloudManagerAPI, SubjectInfo
from bi_constants.enums import RLSSubjectType
from bi_core.rls import (
    BaseSubjectResolver,
    RLS_FAILED_USER_NAME_PREFIX,
    RLSSubject,
)

from bi_utils.aio import await_sync


LOGGER = logging.getLogger(__name__)


ST_DLS_TO_RLS = {
    'user': RLSSubjectType.user,
}


@attr.s
class IAMSubjectResolver(BaseSubjectResolver):
    _cloud_manager: CloudManagerAPI = attr.ib()

    def get_subjects_by_names(self, names: list[str]) -> list[RLSSubject]:
        requested_subjects = names
        subject_emails = list(set(_login_to_email(subject) for subject in requested_subjects))

        results_pre = await_sync(
            self._cloud_manager.subject_emails_to_infos(subject_emails)
        )
        results_pre = {
            _canonize_subject_name(subject_name): subject
            for subject_name, subject in results_pre.items()
        }
        results = {
            subject_name: results_pre.get(_canonize_subject_name(subject_name))
            for subject_name in requested_subjects
        }

        missing_names = sorted(name for name, info in results.items() if info is None)
        if missing_names:
            LOGGER.info('Logins do not exist: {}'.format(', '.join(missing_names)))
        not_found_subjects = [RLSSubject(
            subject_id='',
            subject_type=RLSSubjectType.notfound,
            subject_name=RLS_FAILED_USER_NAME_PREFIX + name,
        ) for name in missing_names]

        return [
            self._make_subject(name, info)
            for name, info in results.items()
            if info is not None
        ] + not_found_subjects

    def _make_subject(self, name: str, info: SubjectInfo) -> RLSSubject:
        return RLSSubject(
            subject_id=info.subj_id,
            subject_type=ST_DLS_TO_RLS[info.subj_type],
            subject_name=name,
        )


def _canonize_subject_name(login: str, suffix: str = '@' + CloudManagerAPI.DEFAULT_DOMAIN) -> str:
    login = login.strip()
    login = login.lower()
    return login.removesuffix(suffix)


def _login_to_email(login: str, default_domain: str = CloudManagerAPI.DEFAULT_DOMAIN) -> str:
    # Compatibility to-be-deprecated mapping
    # should we enable it only for yacloud env?
    if '@' in login:
        return login
    return f'{login}@{default_domain}'
