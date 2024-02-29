import attr

from dl_repmanager.package_index import PackageIndex
from dl_repmanager.primitives import LocalReqPackageSpec


@attr.s(frozen=True)
class DeepDependencyIndex:
    _reqs_by_dep_package: dict[str, set[str]] = attr.ib(kw_only=True)
    _deps_by_req_package: dict[str, set[str]] = attr.ib(kw_only=True)

    def get_deep_requirement_names(self, package_reg_name: str) -> set[str]:
        return self._reqs_by_dep_package[package_reg_name]

    def get_deep_dependency_names(self, package_reg_name: str) -> set[str]:
        return self._deps_by_req_package[package_reg_name]


@attr.s(frozen=True)
class DependencyDescr:
    dep_pkg_name: str = attr.ib(kw_only=True)
    req_pkg_name: str = attr.ib(kw_only=True)
    is_main_list_dep: bool = attr.ib(kw_only=True)


@attr.s
class DeepIndexBuilder:
    def build_deep_index(
        self,
        package_index: PackageIndex,
        main_section_name: str,
    ) -> DeepDependencyIndex:
        shallow_reqs_by_dep_package: dict[str, set[DependencyDescr]] = {}
        shallow_deps_by_req_package: dict[str, set[DependencyDescr]] = {}
        for package_info in package_index.list_package_infos():
            shallow_reqs_by_dep_package[package_info.package_reg_name] = set()
            shallow_deps_by_req_package[package_info.package_reg_name] = set()

        # First generate both-way shallow dependencies
        for package_info in package_index.list_package_infos():
            for req_list_name, req_list in package_info.requirement_lists.items():
                for req_spec in req_list.req_specs:
                    if not isinstance(req_spec, LocalReqPackageSpec):
                        # Ignore non-local dependencies
                        continue

                    dependency = DependencyDescr(
                        dep_pkg_name=package_info.package_reg_name,
                        req_pkg_name=req_spec.package_name,
                        is_main_list_dep=req_list_name == main_section_name,
                    )
                    shallow_reqs_by_dep_package[package_info.package_reg_name].add(dependency)
                    shallow_deps_by_req_package[req_spec.package_name].add(dependency)

        # Now do the deep dependencies
        deep_reqs_by_dep_package: dict[str, set[str]] = {}
        deep_deps_by_req_package: dict[str, set[str]] = {}
        for package_info in package_index.list_package_infos():
            deep_reqs_by_dep_package[package_info.package_reg_name] = set()
            deep_deps_by_req_package[package_info.package_reg_name] = set()

        def deepen_deps(cur_package_reg_name: str, req_reg_name: str, root: bool = False) -> None:
            if not root and cur_package_reg_name == req_reg_name:
                raise ValueError(f"Dependency recursion detected for {req_reg_name}")

            for dependency in shallow_deps_by_req_package.get(cur_package_reg_name, ()):
                if req_reg_name in deep_reqs_by_dep_package[dependency.dep_pkg_name]:
                    # Already registered
                    continue
                deep_reqs_by_dep_package[dependency.dep_pkg_name].add(req_reg_name)
                deep_deps_by_req_package[req_reg_name].add(dependency.dep_pkg_name)
                if dependency.is_main_list_dep:
                    # Test dependencies do not propagate to parents
                    deepen_deps(cur_package_reg_name=dependency.dep_pkg_name, req_reg_name=req_reg_name)

        for package_info in package_index.list_package_infos():
            deepen_deps(
                cur_package_reg_name=package_info.package_reg_name,
                req_reg_name=package_info.package_reg_name,
                root=True,
            )

        deep_dep_index = DeepDependencyIndex(
            reqs_by_dep_package=deep_reqs_by_dep_package,
            deps_by_req_package=deep_deps_by_req_package,
        )
        return deep_dep_index
