import attr

from bi_constants.enums import RLSSubjectType
from bi_core.services_registry.inst_specific_sr import (
    InstallationSpecificServiceRegistry,
    InstallationSpecificServiceRegistryFactory,
)
from bi_core.rls import BaseSubjectResolver, RLSSubject, RLS_FAILED_USER_NAME_PREFIX
from bi_core.utils import FutureRef
from bi_core.services_registry.top_level import ServicesRegistry


@attr.s
class NotFoundSubjectResolver(BaseSubjectResolver):
    def get_subjects_by_names(self, names: list[str]) -> list[RLSSubject]:
        return [
            RLSSubject(
                subject_id='',
                subject_type=RLSSubjectType.notfound,
                subject_name=RLS_FAILED_USER_NAME_PREFIX + name,
            ) for name in names
        ]


@attr.s
class StandaloneServiceRegistry(InstallationSpecificServiceRegistry):
    async def get_subject_resolver(self) -> BaseSubjectResolver:
        return NotFoundSubjectResolver()


@attr.s
class StandaloneServiceRegistryFactory(InstallationSpecificServiceRegistryFactory):
    def get_inst_specific_sr(self, sr_ref: FutureRef[ServicesRegistry]) -> StandaloneServiceRegistry:
        return StandaloneServiceRegistry(service_registry_ref=sr_ref)
