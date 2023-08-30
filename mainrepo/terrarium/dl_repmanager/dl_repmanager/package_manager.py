import os
from typing import Mapping, Optional, Sequence

import attr

from dl_repmanager.primitives import PackageInfo, RequirementList, ReqPackageSpec
from dl_repmanager.env import RepoEnvironment
from dl_repmanager.package_navigator import PackageNavigator
from dl_repmanager.fs_editor import FilesystemEditor
from dl_repmanager.package_index import PackageIndex
from dl_repmanager.package_reference import PackageReference


@attr.s
class PackageGenerator:
    package_index: PackageIndex = attr.ib(kw_only=True)
    repo_env: RepoEnvironment = attr.ib(kw_only=True)

    def _generate_new_package_abs_path(self, package_module_name: str, package_type: str) -> str:
        """Generate package path for new package"""
        parts = [
            self.repo_env.get_root_package_dir(package_type=package_type),
            package_module_name,
        ]
        return os.path.join(*parts)

    @staticmethod
    def _generate_new_package_reg_name(package_name: str) -> str:
        internal_name = package_name.replace('_', '-')
        return internal_name

    def get_boilerplate_package_info(self, package_type: str) -> PackageInfo:
        return self.package_index.get_package_info_from_path(
            self.repo_env.get_boilerplate_package_dir(package_type=package_type),
        )

    def generate_package_info_from_boilerplate(
            self, boilerplate_info: PackageInfo, package_type: str, package_module_name: str,
    ) -> PackageInfo:

        package_test_dirs: list[str] = []
        for boilerplate_test_dir in boilerplate_info.test_dirs:
            package_test_dir = boilerplate_test_dir.replace(  # FIXME: define in templates
                boilerplate_info.single_module_name, package_module_name
            )
            package_test_dirs.append(package_test_dir)

        generated_package_info = PackageInfo(
            package_type=package_type,
            package_reg_name=self._generate_new_package_reg_name(package_module_name),
            module_names=(package_module_name,),  # FIXME: define in template
            abs_path=self._generate_new_package_abs_path(
                package_module_name=package_module_name, package_type=package_type,
            ),
            test_dirs=tuple(package_test_dirs),
        )
        return generated_package_info


@attr.s
class PackageManager:
    repo_env: RepoEnvironment = attr.ib(kw_only=True)
    package_index: PackageIndex = attr.ib(kw_only=True)
    package_navigator: PackageNavigator = attr.ib(kw_only=True)
    package_generator: PackageGenerator = attr.ib(kw_only=True)
    package_reference: PackageReference = attr.ib(kw_only=True)
    fs_editor: FilesystemEditor = attr.ib(init=False)

    @fs_editor.default
    def _make_fs_editor(self) -> FilesystemEditor:
        return self.repo_env.get_fs_editor()

    def _replace_imports(self, old_import_name: str, new_import_name: str) -> list[str]:
        """Replace imports in all project files, return list of updated files"""

        updated_files: list[str] = []
        for file_path in self.package_navigator.recurse_files_with_import(old_import_name):
            # TODO: Refactor replacement to work by finding the imports in the module AST
            self.fs_editor.replace_text_in_file(file_path, old_import_name, new_import_name)
            updated_files.append(file_path)

        return updated_files

    def init_package(self, package_module_name: str, package_type: str) -> None:
        boilerplate_info = self.package_generator.get_boilerplate_package_info(package_type=package_type)
        package_info = self.package_generator.generate_package_info_from_boilerplate(
            boilerplate_info=boilerplate_info,
            package_module_name=package_module_name,
            package_type=package_type,
        )
        self._init_package_from_boilerplate(boilerplate_info, package_info)

    def copy_package(self, package_module_name: str, from_package_module_name: str) -> None:
        boilerplate_info = self.package_index.get_package_info_from_module_name(from_package_module_name)
        package_info = self.package_generator.generate_package_info_from_boilerplate(
            boilerplate_info=boilerplate_info,
            package_module_name=package_module_name,
            package_type=boilerplate_info.package_type,
        )
        self._init_package_from_boilerplate(boilerplate_info, package_info)

    def _init_package_from_boilerplate(self, boilerplate_info: PackageInfo, package_info: PackageInfo) -> None:
        # Create a copy of the boilerplate dir
        boilerplate_dir = boilerplate_info.abs_path
        new_pkg_dir = package_info.abs_path
        self.fs_editor.copy_dir(src_dir=boilerplate_dir, dst_dir=new_pkg_dir)

        # Rename the code dir
        old_code_path = os.path.join(new_pkg_dir, boilerplate_info.single_module_name)
        new_code_path = os.path.join(new_pkg_dir, package_info.single_module_name)
        self.fs_editor.move_path(old_path=old_code_path, new_path=new_code_path)

        # Rename the tests dir
        for boilerplate_test_dir, package_test_dir in zip(boilerplate_info.test_dirs, package_info.test_dirs):
            old_tests_path = os.path.join(new_pkg_dir, boilerplate_test_dir)
            new_tests_path = os.path.join(new_pkg_dir, package_test_dir)
            self.fs_editor.move_path(old_path=old_tests_path, new_path=new_tests_path)

        # Replace all package name occurrences with the given name
        for boilerplate_mod_name, package_mod_name in zip(boilerplate_info.module_names, package_info.module_names):
            self.fs_editor.replace_text_in_dir(
                old_text=boilerplate_mod_name,
                new_text=package_mod_name,
                path=new_pkg_dir,
            )

        self.fs_editor.replace_text_in_dir(
            old_text=boilerplate_info.package_reg_name,
            new_text=package_info.package_reg_name,
            path=new_pkg_dir,
        )

        # Register package
        for mng_plugin in self.repo_env.get_plugins_for_package_type(
                package_type=package_info.package_type, package_index=self.package_index):
            mng_plugin.register_package(package_info=package_info)

    def change_package_type(self, package_module_name: str, new_package_type: str) -> None:
        old_package_info = self.package_index.get_package_info_from_module_name(package_module_name)

        # Move the package dir
        old_pkg_path = old_package_info.abs_path
        new_pkg_path = os.path.join(
            self.repo_env.get_root_package_dir(package_type=new_package_type),
            os.path.basename(old_package_info.abs_path)
        )
        self.fs_editor.move_path(old_path=old_pkg_path, new_path=new_pkg_path)

        # Update package info
        new_package_info = old_package_info.clone(package_type=new_package_type, abs_path=new_pkg_path)

        # Re-register package
        for mng_plugin in self.repo_env.get_plugins_for_package_type(
                package_type=new_package_info.package_type, package_index=self.package_index):
            mng_plugin.re_register_package(old_package_info=old_package_info, new_package_info=new_package_info)

    def rename_module(
            self, old_import_name: str, new_import_name: str,
            fix_imports: bool, move_files: bool,
    ) -> None:
        """Move code from one package to another or within one package"""

        if move_files:
            if self.package_navigator.module_is_package(old_import_name):
                old_path = self.package_navigator.make_package_module_path(old_import_name)
                new_path = self.package_navigator.make_package_module_path(new_import_name)
            else:
                old_path = self.package_navigator.make_file_module_path(old_import_name)
                new_path = self.package_navigator.make_file_module_path(new_import_name)

            self.fs_editor.move_path(old_path, new_path)

        if fix_imports:
            self._replace_imports(old_import_name, new_import_name)

    def get_imports(self, module_name: str) -> list[str]:
        """Move code from one package to another or within one package"""

        package_info = self.package_index.get_package_info_from_module_name(
            package_module_name=module_name,
        )
        return sorted(self.package_navigator.collect_import_bases_from_module(package_info.single_module_name))

    def get_requirements(self, module_name: str) -> Mapping[str, RequirementList]:
        package_info = self.package_index.get_package_info_from_module_name(
            package_module_name=module_name,
        )
        return package_info.requirement_lists

    def compare_imports_and_requirements(
            self, package_module_name: str, ignore_prefix: Optional[str] = None,
            tests: bool = False,
    ) -> tuple[list[ReqPackageSpec], list[ReqPackageSpec]]:
        # TODO: Add support for namespace packages (e.g. google-api-core, google-auth)

        def _normalize_name(name: str) -> str:
            name = name.lower()
            if ignore_prefix is not None and name.startswith(ignore_prefix):
                name = name[len(ignore_prefix):]
            return name

        package_info = self.package_index.get_package_info_from_module_name(
            package_module_name=package_module_name,
        )

        def _get_imports(scan_modules: Sequence[str]) -> dict[str, ReqPackageSpec]:
            _result: dict[str, ReqPackageSpec] = {}
            for module_name in scan_modules:
                for req_module_name in self.package_navigator.collect_import_bases_from_module(module_name):
                    if req_module_name in scan_modules:
                        # Skip the package itself
                        continue
                    req_spec = self.package_reference.get_package_req_spec(req_module_name)
                    _result[_normalize_name(req_spec.package_name)] = req_spec
            return _result

        search_module_names = package_info.test_dirs if tests else package_info.module_names
        import_req_specs = _get_imports(search_module_names)
        if tests:
            main_pkg_import_req_specs = _get_imports(package_info.module_names)
            import_req_specs = {
                key: spec for key, spec in import_req_specs.items()
                if key not in main_pkg_import_req_specs
            }

        req_section_name: str
        if tests:
            req_section_name = 'tool.poetry.group.tests.dependencies'
        else:
            req_section_name = 'tool.poetry.dependencies'

        actual_req_specs: dict[str, ReqPackageSpec] = {
            _normalize_name(req_spec.package_name): req_spec
            for req_spec in package_info.requirement_lists.get(req_section_name, RequirementList()).req_specs
        }

        special_excludes = {
            # Always ignore these imports/requirements
            'python', '',
            _normalize_name(package_info.package_reg_name),
        }

        extra_import_specs: list[ReqPackageSpec] = []
        extra_req_specs: list[ReqPackageSpec] = []
        all_package_names = sorted((set(import_req_specs) | set(actual_req_specs)) - special_excludes)
        for package_name in all_package_names:
            if package_name not in actual_req_specs:
                extra_import_specs.append(import_req_specs[package_name])
            if package_name not in import_req_specs:
                extra_req_specs.append(actual_req_specs[package_name])

        return extra_import_specs, extra_req_specs
