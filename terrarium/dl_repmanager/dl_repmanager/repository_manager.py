import itertools
import os
from pathlib import Path
import re
from typing import (
    Callable,
    Mapping,
    Optional,
    Sequence,
)

import attr

from dl_repmanager.fs_editor import FilesystemEditor
from dl_repmanager.package_index import PackageIndex
from dl_repmanager.package_meta_reader import PackageMetaIOFactory
from dl_repmanager.package_reference import PackageReference
from dl_repmanager.primitives import (
    LocaleDomainSpec,
    LocalReqPackageSpec,
    PackageInfo,
    ReqPackageSpec,
    RequirementList,
)
from dl_repmanager.repository_env import RepoEnvironment
from dl_repmanager.repository_navigator import RepositoryNavigator


@attr.s
class PackageGenerator:
    package_index: PackageIndex = attr.ib(kw_only=True)
    repository_env: RepoEnvironment = attr.ib(kw_only=True)

    def _generate_new_package_abs_path(self, package_module_name: str, package_type: str) -> Path:
        """Generate package path for new package"""
        return self.repository_env.get_root_package_dir(package_type=package_type) / package_module_name

    @staticmethod
    def generate_new_package_reg_name(package_name: str) -> str:
        internal_name = package_name.replace("_", "-").replace("dl-", "datalens-")
        return internal_name

    @staticmethod
    def generate_default_test_dir_name(package_name: str) -> str:
        return f"{package_name}_tests"

    def get_boilerplate_package_info(self, package_type: str) -> PackageInfo:
        return self.package_index.get_package_info_from_path(
            self.repository_env.get_boilerplate_package_dir(package_type=package_type),
        )

    def generate_package_info_from_boilerplate(
        self,
        boilerplate_info: PackageInfo,
        package_type: str,
        package_module_name: str,
    ) -> PackageInfo:
        package_test_dirs: list[str] = []
        for boilerplate_test_dir in boilerplate_info.test_dirs:
            package_test_dir = boilerplate_test_dir.replace(  # FIXME: define in templates
                boilerplate_info.single_module_name, package_module_name
            )
            package_test_dirs.append(package_test_dir)

        generated_package_info = PackageInfo(
            package_type=package_type,
            package_reg_name=self.generate_new_package_reg_name(package_module_name),
            module_names=(package_module_name,),  # FIXME: define in template
            abs_path=self._generate_new_package_abs_path(
                package_module_name=package_module_name,
                package_type=package_type,
            ),
            test_dirs=tuple(package_test_dirs),
        )
        return generated_package_info


@attr.s
class RepositoryManager:
    repository_env: RepoEnvironment = attr.ib(kw_only=True)
    package_index: PackageIndex = attr.ib(kw_only=True)
    repository_navigator: RepositoryNavigator = attr.ib(kw_only=True)
    package_generator: PackageGenerator = attr.ib(kw_only=True)
    package_reference: PackageReference = attr.ib(kw_only=True)
    fs_editor: FilesystemEditor = attr.ib(init=False)

    @fs_editor.default
    def _make_fs_editor(self) -> FilesystemEditor:
        return self.repository_env.get_fs_editor()

    def _replace_imports(self, old_import_name: str, new_import_name: str) -> list[Path]:
        """Replace imports in all project files, return list of updated files"""

        regex, repl = self._make_regex_and_repl_for_sub(
            old_str=old_import_name,
            new_str=new_import_name,
            allow_dash=True,  # dash can be next to the module name
        )

        updated_files: list[Path] = []
        for file_path in self.repository_navigator.recurse_files_with_import(old_import_name):
            # TODO: Refactor replacement to work by finding the imports in the module AST
            self.fs_editor.replace_regex_in_file(file_path=file_path, regex=regex, repl=repl)
            updated_files.append(file_path)

        return updated_files

    def _register_package(self, package_info: PackageInfo) -> None:
        for mng_plugin in self.repository_env.get_plugins(package_index=self.package_index):
            mng_plugin.register_package(package_info=package_info)

    def _unregister_package(self, package_info: PackageInfo) -> None:
        for mng_plugin in self.repository_env.get_plugins(package_index=self.package_index):
            mng_plugin.unregister_package(package_info=package_info)

    def _re_register_package(self, old_package_info: PackageInfo, new_package_info: PackageInfo) -> None:
        for mng_plugin in self.repository_env.get_plugins(package_index=self.package_index):
            mng_plugin.re_register_package(old_package_info=old_package_info, new_package_info=new_package_info)

    def init_package(self, package_module_name: str, package_type: str) -> PackageInfo:
        # Get boilerplate and generate new package meta
        boilerplate_info = self.package_generator.get_boilerplate_package_info(package_type=package_type)
        package_info = self.package_generator.generate_package_info_from_boilerplate(
            boilerplate_info=boilerplate_info,
            package_module_name=package_module_name,
            package_type=package_type,
        )

        # Initialize the package
        self._init_package_from_boilerplate(boilerplate_info=boilerplate_info, package_info=package_info)
        return package_info

    def copy_package(self, package_module_name: str, from_package_module_name: str) -> PackageInfo:
        # Get boilerplate and generate new package meta
        boilerplate_info = self.package_index.get_package_info_from_module_name(from_package_module_name)
        package_info = self.package_generator.generate_package_info_from_boilerplate(
            boilerplate_info=boilerplate_info,
            package_module_name=package_module_name,
            package_type=boilerplate_info.package_type,
        )

        # Initialize the package
        self._init_package_from_boilerplate(boilerplate_info=boilerplate_info, package_info=package_info)
        return package_info

    def remove_package(self, package_module_name: str) -> None:
        package_info = self.package_index.get_package_info_from_module_name(package_module_name)

        # Unregister package
        self._unregister_package(package_info=package_info)

        # Remove the directory
        self.fs_editor.remove_path(package_info.abs_path)

    def _generate_info_for_renamed_package(
        self,
        old_package_info: PackageInfo,
        new_package_module_name: str,
    ) -> PackageInfo:
        """Update all paths, locale domains, etc."""

        # Generate i18n domains
        new_i18n_domain_list: list[LocaleDomainSpec] = []
        for old_domain_spec in old_package_info.i18n_domains:
            new_domain_spec = old_domain_spec.clone(
                domain_name=old_domain_spec.domain_name.replace(
                    old_package_info.single_module_name,
                    new_package_module_name,
                ),
            )
            new_i18n_domain_list.append(new_domain_spec)

        # Generate new path
        new_package_abs_path = old_package_info.abs_path.parent / new_package_module_name

        new_package_info = old_package_info.clone(
            package_reg_name=self.package_generator.generate_new_package_reg_name(new_package_module_name),
            module_names=(new_package_module_name,),
            test_dirs=(self.package_generator.generate_default_test_dir_name(new_package_module_name),),
            abs_path=new_package_abs_path,
            i18n_domains=tuple(new_i18n_domain_list),
        )
        return new_package_info

    def rename_package(self, old_package_module_name: str, new_package_module_name: str) -> PackageInfo:
        old_package_info = self.package_index.get_package_info_from_module_name(old_package_module_name)

        if new_package_module_name == old_package_module_name:
            return old_package_info

        new_package_info = self._generate_info_for_renamed_package(
            old_package_info,
            new_package_module_name=new_package_module_name,
        )

        # Update it
        self._update_package(old_package_info=old_package_info, new_package_info=new_package_info)
        return new_package_info

    def _update_package(self, old_package_info: PackageInfo, new_package_info: PackageInfo) -> PackageInfo:
        # Move the package dir
        if new_package_info.abs_path != old_package_info.abs_path:
            old_pkg_path = old_package_info.abs_path
            new_pkg_path = new_package_info.abs_path
            self.fs_editor.move_path(old_path=old_pkg_path, new_path=new_pkg_path)

        # Rename and replace all the internals and update imports
        if new_package_info.single_module_name != old_package_info.single_module_name:
            self._rename_package_internals(old_package_info=old_package_info, new_package_info=new_package_info)
            self._replace_imports(old_package_info.single_module_name, new_package_info.single_module_name)
            self._rename_in_dependent_meta_files(old_package_info=old_package_info, new_package_info=new_package_info)

        # Rename locale files to match the new domain names
        if new_package_info.i18n_domains != old_package_info.i18n_domains:
            self._rename_i18n_files(old_package_info=old_package_info, new_package_info=new_package_info)

        # Re-register package under new name
        self._re_register_package(old_package_info=old_package_info, new_package_info=new_package_info)
        return new_package_info

    def _init_package_from_boilerplate(self, boilerplate_info: PackageInfo, package_info: PackageInfo) -> None:
        # Create a copy of the boilerplate dir
        boilerplate_dir = boilerplate_info.abs_path
        new_pkg_dir = package_info.abs_path
        self.fs_editor.copy_path(src_path=boilerplate_dir, dst_path=new_pkg_dir)

        # Rename and replace all the internals
        self._rename_package_internals(old_package_info=boilerplate_info, new_package_info=package_info)

        # Register package
        self._register_package(package_info=package_info)

    def _make_regex_and_repl_for_sub(
        self,
        old_str: str,
        new_str: str,
        allow_dash: bool = True,
    ) -> tuple[re.Pattern, Callable[[re.Match], str]]:
        """
        Generate a tuple whose contains can be used as parameters for re.sub.
        Use this to replace module or package names in files
        """

        def _repl_mod_name(match: re.Match) -> str:
            return (
                match.string[match.start() : match.start("mod_name")]
                + new_str
                + match.string[match.end("mod_name") : match.end()]
            )

        regex: re.Pattern
        if allow_dash:
            assert "-" not in old_str and "-" not in new_str
            regex = re.compile(rf"(^|[^\w])(?P<mod_name>{re.escape(old_str)})($|[^\w])")
        else:
            regex = re.compile(rf"(^|[^\w\-])(?P<mod_name>{re.escape(old_str)})($|[^\w\-])")

        return regex, _repl_mod_name

    def _rename_i18n_files(self, old_package_info: PackageInfo, new_package_info: PackageInfo) -> None:
        """Rename the locale files in accordance with the new domain names"""
        assert len(new_package_info.i18n_domains) == len(old_package_info.i18n_domains)
        zipped_domain_pairs = zip(old_package_info.i18n_domains, new_package_info.i18n_domains)
        for old_domain_spec, new_domain_spec in zipped_domain_pairs:
            if new_domain_spec.domain_name == old_domain_spec.domain_name:
                continue

            # Update it in pyproject.toml
            regex, repl = self._make_regex_and_repl_for_sub(
                old_str=old_domain_spec.domain_name,
                new_str=new_domain_spec.domain_name,
                allow_dash=True,
            )
            self.fs_editor.replace_regex_in_file(regex=regex, repl=repl, file_path=new_package_info.toml_path)

            # Rename the files
            locales_path = new_package_info.abs_path / new_package_info.single_module_name / "locales"
            for locale_dir in locales_path.iterdir():
                old_path_base = locales_path / locale_dir.name / "LC_MESSAGES" / old_domain_spec.domain_name
                new_path_base = locales_path / locale_dir.name / "LC_MESSAGES" / new_domain_spec.domain_name
                for ext in ("po", "mo"):
                    old_path = Path(f"{str(old_path_base)}.{ext}")
                    new_path = Path(f"{str(new_path_base)}.{ext}")
                    self.fs_editor.move_path(old_path=old_path, new_path=new_path)

    def _rename_in_dependent_meta_files(self, old_package_info: PackageInfo, new_package_info: PackageInfo) -> None:
        meta_files = (
            "pyproject.toml",
            "README.md",
        )
        for other_package_info in self.package_index.list_package_infos():
            if other_package_info == old_package_info:
                continue
            regex, repl = self._make_regex_and_repl_for_sub(
                old_str=old_package_info.single_module_name,
                new_str=new_package_info.single_module_name,
                allow_dash=True,  # dash can be next to the module name
            )
            for meta_file_name in meta_files:
                meta_file_path = other_package_info.abs_path / meta_file_name
                if not meta_file_path.exists():
                    continue
                self.fs_editor.replace_regex_in_file(regex=regex, repl=repl, file_path=meta_file_path)

    def _rename_package_internals(self, old_package_info: PackageInfo, new_package_info: PackageInfo) -> None:
        new_pkg_dir = new_package_info.abs_path

        # Rename the code dir
        old_code_path = new_pkg_dir / old_package_info.single_module_name
        new_code_path = new_pkg_dir / new_package_info.single_module_name
        self.fs_editor.move_path(old_path=old_code_path, new_path=new_code_path)

        # Rename the tests dir
        for boilerplate_test_dir, package_test_dir in zip(old_package_info.test_dirs, new_package_info.test_dirs):
            old_tests_path = new_pkg_dir / boilerplate_test_dir
            new_tests_path = new_pkg_dir / package_test_dir
            self.fs_editor.move_path(old_path=old_tests_path, new_path=new_tests_path)

        # Masks for files that should not be edited
        edit_exclude_masks = self.repository_env.get_edit_exclude_masks()

        # Replace all package name occurrences with the given name
        all_zipped_modules = itertools.chain(
            zip(old_package_info.module_names, new_package_info.module_names),
            zip(old_package_info.test_dirs, new_package_info.test_dirs),
        )
        for boilerplate_mod_name, package_mod_name in all_zipped_modules:
            if boilerplate_mod_name == "tests" or package_mod_name == "tests":
                # FIXME: Do something
                continue
            regex, repl = self._make_regex_and_repl_for_sub(
                old_str=boilerplate_mod_name,
                new_str=package_mod_name,
                allow_dash=True,  # dash can be next to the module name
            )
            self.fs_editor.replace_regex_in_dir(
                regex=regex,
                repl=repl,
                path=new_pkg_dir,
                exclude_masks=edit_exclude_masks,
            )

        # Update reg name
        pkg_meta_io_factory = PackageMetaIOFactory(fs_editor=self.fs_editor)
        with pkg_meta_io_factory.package_meta_writer(file_path=new_package_info.toml_path) as pkg_meta_writer:
            pkg_meta_writer.update_package_name(new_name=new_package_info.package_reg_name)

    def change_package_type(self, package_module_name: str, new_package_type: str) -> PackageInfo:
        old_package_info = self.package_index.get_package_info_from_module_name(package_module_name)

        root_pkg_dir = self.repository_env.get_root_package_dir(package_type=new_package_type)
        new_pkg_path = root_pkg_dir / old_package_info.abs_path.name

        new_package_info = old_package_info.clone(
            package_type=new_package_type,
            abs_path=new_pkg_path,
        )

        # Update it
        self._update_package(old_package_info=old_package_info, new_package_info=new_package_info)

        return new_package_info

    def rename_module(
        self,
        old_import_name: str,
        new_import_name: str,
        fix_imports: bool,
        move_files: bool,
    ) -> None:
        """Move code from one package to another or within one package"""

        if move_files:
            if self.repository_navigator.module_is_dir(old_import_name):
                old_path = self.repository_navigator.make_dir_module_path(old_import_name)
                new_path = self.repository_navigator.make_dir_module_path(new_import_name)
            else:
                old_path = self.repository_navigator.make_file_module_path(old_import_name)
                new_path = self.repository_navigator.make_file_module_path(new_import_name)

            self.fs_editor.move_path(old_path, new_path)

        if fix_imports:
            self._replace_imports(old_import_name, new_import_name)

    def get_imports(self, module_name: str) -> list[str]:
        """Move code from one package to another or within one package"""

        package_info = self.package_index.get_package_info_from_module_name(
            package_module_name=module_name,
        )
        return sorted(self.repository_navigator.collect_import_bases_from_module(package_info.single_module_name))

    def get_requirements(self, module_name: str) -> Mapping[str, RequirementList]:
        package_info = self.package_index.get_package_info_from_module_name(
            package_module_name=module_name,
        )
        return package_info.requirement_lists

    def compare_imports_and_requirements(
        self,
        package_module_name: str,
        ignore_prefix: Optional[str] = None,
        tests: bool = False,
    ) -> tuple[list[ReqPackageSpec], list[ReqPackageSpec]]:
        # TODO: Add support for namespace packages (e.g. google-api-core, google-auth)

        def _normalize_name(name: str) -> str:
            name = name.lower()
            if ignore_prefix is not None and name.startswith(ignore_prefix):
                name = name[len(ignore_prefix) :]
            return name

        package_info = self.package_index.get_package_info_from_module_name(
            package_module_name=package_module_name,
        )

        def _get_imports(scan_modules: Sequence[str]) -> dict[str, ReqPackageSpec]:
            _result: dict[str, ReqPackageSpec] = {}
            for module_name in scan_modules:
                for req_module_name in self.repository_navigator.collect_import_bases_from_module(module_name):
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
                key: spec for key, spec in import_req_specs.items() if key not in main_pkg_import_req_specs
            }

        req_section_name: str
        if tests:
            req_section_name = "tool.poetry.group.tests.dependencies"
        else:
            req_section_name = "tool.poetry.dependencies"

        actual_req_specs: dict[str, ReqPackageSpec] = {
            _normalize_name(req_spec.package_name): req_spec
            for req_spec in package_info.requirement_lists.get(req_section_name, RequirementList()).req_specs
        }
        actual_req_specs = {
            req_name: req_spec
            for req_name, req_spec in actual_req_specs.items()
            if req_name not in package_info.implicit_deps
        }
        # TODO: Check that implicit deps are all listed in either the main or the test section

        special_excludes = {
            # Always ignore these imports/requirements
            "python",
            "",
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

        def _normalize_req_path(req: ReqPackageSpec) -> ReqPackageSpec:
            if isinstance(req, LocalReqPackageSpec):
                req_pkg_info = self.package_index.get_package_info_by_reg_name(req.package_name)
                req_path = Path(os.path.relpath(req_pkg_info.abs_path, package_info.abs_path))
                req = req.clone(path=req_path)
            return req

        extra_import_specs = [_normalize_req_path(req) for req in extra_import_specs]
        extra_req_specs = [_normalize_req_path(req) for req in extra_req_specs]

        return extra_import_specs, extra_req_specs
