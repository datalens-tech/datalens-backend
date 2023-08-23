from importlib.metadata import packages_distributions

import attr

from dl_repmanager.primitives import ReqPackageSpec, LocalReqPackageSpec, PypiReqPackageSpec
from dl_repmanager.env import RepoEnvironment
from dl_repmanager.package_index import PackageIndex


@attr.s
class PackageReference:
    repo_env: RepoEnvironment = attr.ib(kw_only=True)
    package_index: PackageIndex = attr.ib(kw_only=True)

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
                package_name = self.repo_env.custom_package_map.get(module_name, module_name)

            spec = PypiReqPackageSpec(package_name=package_name, version=None)

        return spec
