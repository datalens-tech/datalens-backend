from typing import ClassVar
from importlib.metadata import packages_distributions

import attr

from dl_repmanager.primitives import ReqPackageSpec, LocalReqPackageSpec, PypiReqPackageSpec
from dl_repmanager.package_index import PackageIndex


@attr.s
class PackageReference:
    package_index: PackageIndex = attr.ib(kw_only=True)

    _CUSTOM_MODULE_PACKAGE_MAP: ClassVar[dict[str, str]] = {
        # FIXME: This is very bad. Find some better way to do this or move to config
        'flask_marshmallow': 'flask-marshmallow',
        'flask_restx': 'flask-restx',
        'jaeger_client': 'jaeger-client',
        'jwt': 'pyjwt',
        'typing_extensions': 'typing-extensions',
        'marshmallow_enum': 'marshmallow-enum',
        'marshmallow_oneofschema': 'marshmallow-oneofschema',
        'more_itertools': 'more-itertools',
        'psycopg2': 'psycopg2-binary',
        'pytest_lazyfixture': 'pytest-lazyfixture',
        'pyuwsgi': 'uwsgi',
        'sentry_sdk': 'sentry-sdk',
    }

    def get_package_req_spec(self, module_name: str) -> ReqPackageSpec:
        spec: ReqPackageSpec
        try:
            local_pkg_info = self.package_index.get_package_info_from_module_name(package_module_name=module_name)
            spec = LocalReqPackageSpec(
                package_name=local_pkg_info.package_reg_name,
                path=local_pkg_info.abs_path,  # FIXME: Use some kind of relative path here
            )
        except KeyError:
            try:
                package_name = packages_distributions()[module_name][0]
            except KeyError:
                package_name = self._CUSTOM_MODULE_PACKAGE_MAP.get(module_name, module_name)

            spec = PypiReqPackageSpec(package_name=package_name, version=None)

        return spec
