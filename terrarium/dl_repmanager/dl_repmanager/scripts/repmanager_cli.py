from __future__ import annotations

import argparse
import difflib
import os
from pathlib import Path
from pprint import pprint
import sys
from typing import Optional

import attr

from dl_cli_tools.cli_base import CliToolBase
from dl_cli_tools.logging import setup_basic_logging
from dl_repmanager.git_manager import GitManager
from dl_repmanager.metapkg_manager import MetaPackageManager
from dl_repmanager.package_index import (
    PackageIndex,
    PackageIndexBuilder,
)
from dl_repmanager.package_reference import PackageReference
from dl_repmanager.primitives import (
    EntityReference,
    EntityReferenceType,
)
from dl_repmanager.project_editor import PyPrjEditor
from dl_repmanager.repository_env import (
    DEFAULT_CONFIG_FILE_NAME,
    RepoEnvironment,
    RepoEnvironmentLoader,
)
from dl_repmanager.repository_manager import (
    PackageGenerator,
    RepositoryManager,
)
from dl_repmanager.repository_navigator import RepositoryNavigator
from dl_repmanager.scripts.package_meta_cli import (
    DlPackageMetaTool,
    add_package_commands,
)


def _entity_ref_type_type(value: str | EntityReferenceType) -> EntityReferenceType:
    if isinstance(value, str):
        value = EntityReferenceType[value.replace("-", "_")]
    assert isinstance(value, EntityReferenceType)
    return value


def _entity_ref_type(value: str | EntityReference) -> EntityReference:
    if isinstance(value, str):
        ref_type_str, ref_name = value.split(":")
        value = EntityReference(ref_type=EntityReferenceType[ref_type_str], name=ref_name)
    assert isinstance(value, EntityReference)
    return value


def _entity_ref_list_type(value: str | list[EntityReference]) -> list[EntityReference]:
    if isinstance(value, str):
        value = [_entity_ref_type(part) for part in value.split(",")]
    assert isinstance(value, list)
    return value


_CWD = Path.cwd()


def make_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="DL Repository Management CLI")
    parser.add_argument("--config", help="Specify configuration file", default=DEFAULT_CONFIG_FILE_NAME)
    parser.add_argument("--base-path", type=Path, help="Base repository path", default=_CWD)
    parser.add_argument("--fs-editor", help="Override the FS editor type")
    parser.add_argument("--dry-run", action="store_true", help="Force usage of virtual FS editor")

    # mix-in parsers
    package_name_parser = argparse.ArgumentParser(add_help=False)
    package_name_parser.add_argument(
        "--package-name",
        required=True,
        help="Name of the package folder",
    )
    package_type_parser = argparse.ArgumentParser(add_help=False)
    package_type_parser.add_argument(
        "--package-type",
        required=True,
        help="Type of the package. See the config file (dl-repo.yml) for available package types",
    )
    old_new_import_names_parser = argparse.ArgumentParser(add_help=False)
    old_new_import_names_parser.add_argument("--old-import-name", required=True, help="Old import (module) name")
    old_new_import_names_parser.add_argument("--new-import-name", required=True, help="New import (module) name")

    # commands
    subparsers = parser.add_subparsers(title="command", dest="command")

    subparsers.add_parser(
        "init",
        parents=[package_name_parser, package_type_parser],
        help="Initialize new package",
    )

    copy_subparser = subparsers.add_parser(
        "copy",
        parents=[package_name_parser],
        help="Copy package",
    )
    copy_subparser.add_argument(
        "--from-package-name",
        required=True,
        help="Name of the package folder to copy from",
    )

    rename_parser = subparsers.add_parser(
        "rename",
        parents=[package_name_parser],
        help="Rename package",
    )
    rename_parser.add_argument(
        "--new-package-name",
        required=True,
        help="New name of the package",
    )

    move_code_parser = subparsers.add_parser(
        "rename-module",
        parents=[old_new_import_names_parser],
        help="Rename module (move code from one package to another or within one package)",
    )
    move_code_parser.add_argument(
        "--no-fix-imports",
        action="store_true",
        help="Don't update module name in import statements and other references",
    )
    move_code_parser.add_argument(
        "--no-move-files",
        action="store_true",
        help="Don't move the actual files",
    )

    subparsers.add_parser(
        "ch-package-type",
        parents=[package_name_parser, package_type_parser],
        help="Change package type (move it to another directory)",
    )

    package_list_parser = subparsers.add_parser(
        "package-list",
        parents=[package_type_parser],
        help="List all packages of a given type",
    )
    package_list_parser.add_argument("--mask", help="Formatting mask")
    package_list_parser.add_argument("--base-path", default=".", help="Base path for formatting relative package paths")

    subparsers.add_parser(
        "import-list",
        parents=[package_name_parser],
        help="List imports in a package",
    )

    subparsers.add_parser(
        "req-list",
        parents=[package_name_parser],
        help="List requirements of a package",
    )

    req_check_parser = subparsers.add_parser(
        "req-check",
        parents=[package_name_parser],
        help="Compare imports and requirements of a package",
    )
    req_check_parser.add_argument("--ignore-prefix", type=str, help="Package prefix to ignore when comparing")
    req_check_parser.add_argument("--tests", action="store_true", help="Check for tests, not the main package")

    metapackage_parser = argparse.ArgumentParser(add_help=False)
    metapackage_parser.add_argument("--metapackage", required=True, help="Metapackage name from repo config")

    subparsers.add_parser(
        "ensure-mypy-common",
        parents=[metapackage_parser],
        help="Checks and updates all sub projects with mypy config using template in the meta package",
    )

    compare_resulting_dependencies = subparsers.add_parser(
        "compare-resulting-deps",
        help=(
            "Compare resulting dependencies (including transitive) for Poetry dependency group."
            " Should be called from meta package dir."
            " Poetry lock for current packages will be called."
            " repository with specified revision (as compare base) will be checked out in ${DL_REPMGR_REPO_CLONE_PATH}."
        ),
    )
    compare_resulting_dependencies.add_argument(
        "--group",
        type=str,
        required=True,
        help="Poetry dependency group to check",
    )
    compare_resulting_dependencies.add_argument(
        "--base-rev", type=str, default="origin/trunk", help="git revision to compare with"
    )

    entities_parser = argparse.ArgumentParser(add_help=False)
    entities_parser.add_argument(
        "--entities",
        type=_entity_ref_list_type,
        required=True,
        help=(
            "Comma-separated list of repository entities which can be: "
            "package_type:<type_name>, package_reg:<package_reg_name> "
            "path:<path_to_py_file>, module:<module_name>"
        ),
    )

    resolve_parser = subparsers.add_parser(
        "resolve",
        parents=[entities_parser],
        help="Resolve repository entities from one type to another",
    )
    resolve_parser.add_argument("--to-type", type=_entity_ref_type_type, required=True, help="Output entity type")

    search_imports_parser = subparsers.add_parser(
        "search-imports",
        parents=[entities_parser],
        help="Search imports of of one set of entities in another",
    )
    search_imports_parser.add_argument(
        "--imported", type=_entity_ref_list_type, required=True, help="List of imported entites to search for"
    )

    recurse_packages_parser = subparsers.add_parser(
        "recurse-packages",
        parents=[entities_parser],
        help="Apply command to multiple packages",
    )
    recurse_packages_subparsers = recurse_packages_parser.add_subparsers(
        title="package_command",
        dest="recurse_packages_command",
    )
    add_package_commands(recurse_packages_subparsers)

    return parser


@attr.s
class DlRepManagerTool(CliToolBase):
    repository_env: RepoEnvironment = attr.ib(kw_only=True)
    package_index: PackageIndex = attr.ib(kw_only=True)
    repository_manager: RepositoryManager = attr.ib(kw_only=True)
    repository_navigator: RepositoryNavigator = attr.ib(kw_only=True)
    py_prj_editor: PyPrjEditor = attr.ib(kw_only=True)

    @classmethod
    def get_parser(cls) -> argparse.ArgumentParser:
        return make_parser()

    def init(self, package_name: str, package_type: str) -> None:
        self.repository_manager.init_package(package_module_name=package_name, package_type=package_type)

    def copy(self, package_name: str, from_package_name: str) -> None:
        self.repository_manager.copy_package(
            package_module_name=package_name, from_package_module_name=from_package_name
        )

    def rename(self, package_name: str, new_package_name: str) -> None:
        self.repository_manager.rename_package(
            old_package_module_name=package_name,
            new_package_module_name=new_package_name,
        )

    def ch_package_type(self, package_name: str, package_type: str) -> None:
        self.repository_manager.change_package_type(package_module_name=package_name, new_package_type=package_type)

    def package_list(self, package_type: str, mask: str, base_path: Path) -> None:
        for package_info in self.package_index.list_package_infos(package_type=package_type):
            printable_values = dict(
                package_type=package_info.package_type,
                abs_path=package_info.abs_path,
                rel_path=os.path.relpath(package_info.abs_path, base_path),
                single_module_name=package_info.single_module_name,
                package_reg_name=package_info.package_reg_name,
            )
            if mask:
                print(mask.format(**printable_values))
            else:
                pprint(printable_values)

    def rename_module(
        self,
        old_import_name: str,
        new_import_name: str,
        no_fix_imports: bool,
        no_move_files: bool,
    ) -> None:
        self.repository_manager.rename_module(
            old_import_name=old_import_name,
            new_import_name=new_import_name,
            fix_imports=not no_fix_imports,
            move_files=not no_move_files,
        )

    def import_list(self, package_name: str) -> None:
        """List imports in a package"""
        imports = self.repository_manager.get_imports(package_name)
        for imported_name in imports:
            print(imported_name)

    def req_list(self, package_name: str) -> None:
        """List requirements of a package"""
        req_lists = self.repository_manager.get_requirements(package_name)
        for req_list_name, req_list in sorted(req_lists.items()):
            print(req_list_name)
            for req_item in req_list.req_specs:
                print(f"    {req_item.pretty()}")

    def check_requirements(self, package_name: str, ignore_prefix: Optional[str] = None, tests: bool = False) -> None:
        """Compares imports and requirements of a package"""
        extra_import_specs, extra_req_specs = self.repository_manager.compare_imports_and_requirements(
            package_name,
            ignore_prefix=ignore_prefix,
            tests=tests,
        )

        if extra_import_specs:
            print("Missing requirements:")
            for req_item in extra_import_specs:
                print(f"    {req_item.as_req_str()}")
            print()
        if extra_req_specs:
            print("Extra requirements:")
            for req_item in extra_req_specs:
                print(f"    {req_item.as_req_str()}")
            print()
        if not extra_req_specs and not extra_import_specs:
            print("Requirements are in sync with imports")

    def ensure_mypy_common(self, metapackage_name: str):
        self.py_prj_editor.update_mypy_common(metapackage_name=metapackage_name)

    def compare_resulting_deps(self, base_revision: str, group: str):
        repo_root = self.py_prj_editor.base_path
        main_git_mgr = GitManager(repo_root)

        repo_clone_dest = Path(os.environ["DL_REPMGR_REPO_CLONE_PATH"])
        assert repo_clone_dest.parent.exists(), "Parent dir for repo clone does not exits"

        cmp_base_git_mgr = main_git_mgr.copy_repo(repo_clone_dest, fetch_if_exists=True)
        cmp_base_git_mgr.checkout(base_revision)

        metapkg_rel_path = Path.cwd().relative_to(repo_root)

        current_metapkg_mgr = MetaPackageManager(Path.cwd())

        current_metapkg_mgr.run_poetry_lock(suppress_stdout=True)

        cmp_base_metapkg_mgr = MetaPackageManager(repo_clone_dest / metapkg_rel_path)

        current_deps = current_metapkg_mgr.export_dependencies(group)
        cmp_base_deps = cmp_base_metapkg_mgr.export_dependencies(group)

        diff_lines = difflib.unified_diff(
            [dep.pretty() for dep in cmp_base_deps],
            [dep.pretty() for dep in current_deps],
            fromfile="cmp_base.txt",
            tofile="actual.txt",
        )
        for line in diff_lines:
            print(line, end=None)

    def resolve_entities(self, entities: list[EntityReference], to_ref_type: EntityReferenceType) -> None:
        resolved_entities = self.repository_navigator.resolve_entities_to_type(entities, to_ref_type=to_ref_type)
        for entity in sorted(resolved_entities, key=lambda item: item.name):
            print(entity.name)

    def search_imports(self, entities: list[EntityReference], imported: list[EntityReference]) -> None:
        import_specs = self.repository_navigator.collect_imports_of_entities_from_entities(
            src_entity_list=entities, imported_entity_list=imported
        )

        base_path_len = len(str(self.repository_env.base_path)) + 1  # +1 for the slash
        formatted_imports = [
            (
                f"{str(import_spec.source_path)[base_path_len:]}:"
                f"{import_spec.import_ast.lineno}: "
                f"{import_spec.import_module_name}"
            )
            for import_spec in sorted(import_specs, key=lambda item: item.source_path)
        ]
        print("\n".join(sorted(formatted_imports)))

    def recurse_packages(
        self,
        entities: list[EntityReference],
        args: argparse.Namespace,
    ) -> None:
        for entity_ref in entities:
            assert entity_ref.ref_type in (
                EntityReferenceType.package_reg,
                EntityReferenceType.package_type,
            ), "Only `package_reg` and `package_type` supported here"

        resolved_entities = self.repository_navigator.resolve_entities_to_type(
            entities,
            to_ref_type=EntityReferenceType.package_reg,
        )
        package_paths = [
            self.package_index.get_package_info_by_reg_name(entity_ref.name).abs_path
            for entity_ref in resolved_entities
        ]
        for package_path in package_paths:
            print(f"Processing {package_path}")
            DlPackageMetaTool.run_for_package_path(
                fs_editor=self.repository_env.get_fs_editor(),
                package_path=package_path,
                package_command=args.recurse_packages_command,
                args=args,
            )

    @classmethod
    def initialize(cls, base_path: Path, config_file_name: str, fs_editor_type: str) -> DlRepManagerTool:
        repository_env = RepoEnvironmentLoader(
            config_file_name=config_file_name,
            override_fs_editor_type=fs_editor_type,
        ).load_env(base_path=base_path)

        index_builder = PackageIndexBuilder(repository_env=repository_env)
        package_index = index_builder.build_index()

        repository_navigator = RepositoryNavigator(repository_env=repository_env, package_index=package_index)
        tool = cls(
            repository_env=repository_env,
            package_index=package_index,
            repository_navigator=repository_navigator,
            repository_manager=RepositoryManager(
                repository_env=repository_env,
                package_index=package_index,
                repository_navigator=repository_navigator,
                package_generator=PackageGenerator(package_index=package_index, repository_env=repository_env),
                package_reference=PackageReference(package_index=package_index, repository_env=repository_env),
            ),
            py_prj_editor=PyPrjEditor(
                repository_env=repository_env,
                package_index=package_index,
            ),
        )
        return tool

    @classmethod
    def run_parsed_args(cls, args: argparse.Namespace) -> None:
        config_file_name = args.config
        base_path = args.base_path
        fs_editor_type = args.fs_editor if not args.dry_run else "virtual"
        tool = cls.initialize(base_path=base_path, config_file_name=config_file_name, fs_editor_type=fs_editor_type)

        match args.command:
            case "init":
                tool.init(package_name=args.package_name, package_type=args.package_type)
            case "copy":
                tool.copy(package_name=args.package_name, from_package_name=args.from_package_name)
            case "rename":
                tool.rename(package_name=args.package_name, new_package_name=args.new_package_name)
            case "ch-package-type":
                tool.ch_package_type(package_name=args.package_name, package_type=args.package_type)
            case "package-list":
                tool.package_list(package_type=args.package_type, mask=args.mask, base_path=Path(args.base_path))
            case "rename-module":
                tool.rename_module(
                    old_import_name=args.old_import_name,
                    new_import_name=args.new_import_name,
                    no_fix_imports=args.no_fix_imports,
                    no_move_files=args.no_move_files,
                )
            case "import-list":
                tool.import_list(package_name=args.package_name)
            case "req-list":
                tool.req_list(package_name=args.package_name)
            case "req-check":
                tool.check_requirements(
                    package_name=args.package_name, ignore_prefix=args.ignore_prefix, tests=args.tests
                )
            case "ensure-mypy-common":
                tool.ensure_mypy_common(metapackage_name=args.metapackage)
            case "compare-resulting-deps":
                tool.compare_resulting_deps(base_revision=args.base_rev, group=args.group)
            case "resolve":
                tool.resolve_entities(entities=args.entities, to_ref_type=args.to_type)
            case "search-imports":
                tool.search_imports(entities=args.entities, imported=args.imported)
            case "recurse-packages":
                tool.recurse_packages(entities=args.entities, args=args)
            case _:
                raise RuntimeError(f"Got unknown command: {args.command}")


def main() -> None:
    setup_basic_logging()
    DlRepManagerTool.run(sys.argv[1:])


if __name__ == "__main__":
    main()
