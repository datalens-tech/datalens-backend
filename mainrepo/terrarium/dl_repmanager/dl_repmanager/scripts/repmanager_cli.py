"""
Tool for management and analysis of bi packages

Examples:

    python tools/local_dev/bi_package.py init-lib --package-name bi_connector_clickhouse

    python tools/local_dev/bi_package.py move-code --old-import-name bi_core.connectors.clickhouse --new-import-name bi_connector_clickhouse.core

"""

from __future__ import annotations

import argparse
import difflib
import os
from pathlib import Path
from pprint import pprint
from typing import Optional

import attr

from dl_repmanager.env import DEFAULT_CONFIG_FILE_NAME, RepoEnvironmentLoader
from dl_repmanager.git_manager import GitManager
from dl_repmanager.logging import setup_basic_logging
from dl_repmanager.metapkg_manager import MetaPackageManager
from dl_repmanager.package_index import PackageIndexBuilder, PackageIndex
from dl_repmanager.package_manager import PackageManager, PackageGenerator
from dl_repmanager.package_navigator import PackageNavigator
from dl_repmanager.package_reference import PackageReference
from dl_repmanager.project_editor import PyPrjEditor


def make_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog='DL Repository Management CLI')
    parser.add_argument('--config', help='Specify configuration file', default=DEFAULT_CONFIG_FILE_NAME)
    parser.add_argument('--fs-editor', help='Override the FS editor type')
    parser.add_argument('--dry-run', action='store_true', help='Force usage of virtual FS editor')

    # mix-in parsers
    package_name_parser = argparse.ArgumentParser(add_help=False)
    package_name_parser.add_argument(
        '--package-name', required=True, help='Name of the package folder',
    )
    package_type_parser = argparse.ArgumentParser(add_help=False)
    package_type_parser.add_argument(
        '--package-type', required=True,
        help='Type of the package. See the config file (dl-repo.yml) for available package types',
    )
    rename_parser = argparse.ArgumentParser(add_help=False)
    rename_parser.add_argument('--old-import-name', required=True, help='Old import (module) name')
    rename_parser.add_argument('--new-import-name', required=True, help='New import (module) name')

    # commands
    subparsers = parser.add_subparsers(title='command', dest='command')

    subparsers.add_parser(
        'init', parents=[package_name_parser, package_type_parser],
        help='Initialize new package',
    )

    copy_subparser = subparsers.add_parser(
        'copy', parents=[package_name_parser],
        help='Copy package',
    )
    copy_subparser.add_argument(
        '--from-package-name', required=True, help='Name of the package folder to copy from',
    )

    rename_subparser = subparsers.add_parser(
        'rename', parents=[package_name_parser],
        help='Rename package',
    )
    rename_subparser.add_argument(
        '--new-package-name', required=True, help='New name of the package',
    )

    move_code_parser = subparsers.add_parser(
        'rename-module', parents=[rename_parser],
        help='Rename module (move code from one package to another or within one package)',
    )
    move_code_parser.add_argument(
        '--no-fix-imports', action='store_true',
        help='Don\'t update module name in import statements and other references',
    )
    move_code_parser.add_argument(
        '--no-move-files', action='store_true',
        help='Don\'t move the actual files',
    )

    subparsers.add_parser(
        'ch-package-type', parents=[package_name_parser, package_type_parser],
        help='Change package type (move it to another directory)',
    )

    package_list_subparser = subparsers.add_parser(
        'package-list', parents=[package_type_parser],
        help='List all packages of a given type',
    )
    package_list_subparser.add_argument('--mask', help='Formatting mask')
    package_list_subparser.add_argument(
        '--base-path', default='.',
        help='Base path for formatting relative package paths'
    )

    subparsers.add_parser(
        'import-list', parents=[package_name_parser],
        help='List imports in a package',
    )

    subparsers.add_parser(
        'req-list', parents=[package_name_parser],
        help='List requirements of a package',
    )

    req_check_subparser = subparsers.add_parser(
        'req-check', parents=[package_name_parser],
        help='Compare imports and requirements of a package',
    )
    req_check_subparser.add_argument('--ignore-prefix', type=str, help='Package prefix to ignore when comparing')
    req_check_subparser.add_argument('--tests', action='store_true', help='Check for tests, not the main package')

    subparsers.add_parser(
        'ensure-mypy-common',
        help='Checks and updates all sub projects with mypy config using template in the meta package',
    )

    compare_resulting_dependencies = subparsers.add_parser(
        'compare-resulting-deps',
        help=(
            'Compare resulting dependencies (including transitive) for Poetry dependency group.'
            ' Should be called from meta package dir.'
            ' Poetry lock for current packages will be called.'
            ' repository with specified revision (as compare base) will be checked out in ${DL_REPMGR_REPO_CLONE_PATH}.'
        )
    )
    compare_resulting_dependencies.add_argument(
        '--group', type=str, required=True,
        help='Poetry dependency group to check',
    )
    compare_resulting_dependencies.add_argument(
        '--base-rev', type=str, default='origin/trunk',
        help='git revision to compare with'
    )
    return parser


_BASE_DIR = Path.cwd()


@attr.s
class DlRepManagerTool:
    package_index: PackageIndex = attr.ib(kw_only=True)
    package_manager: PackageManager = attr.ib(kw_only=True)
    package_navigator: PackageNavigator = attr.ib(kw_only=True)
    py_prj_editor: PyPrjEditor = attr.ib(kw_only=True)

    @classmethod
    def validate_env(cls) -> None:
        """Validate that the tool is being run correctly"""

    def init(self, package_name: str, package_type: str) -> None:
        self.package_manager.init_package(package_module_name=package_name, package_type=package_type)

    def copy(self, package_name: str, from_package_name: str) -> None:
        self.package_manager.copy_package(package_module_name=package_name, from_package_module_name=from_package_name)

    def rename(self, package_name: str, new_package_name: str) -> None:
        self.package_manager.rename_package(
            old_package_module_name=package_name,
            new_package_module_name=new_package_name,
        )

    def ch_package_type(self, package_name: str, package_type: str) -> None:
        self.package_manager.change_package_type(package_module_name=package_name, new_package_type=package_type)

    def package_list(self, package_type: str, mask: str, base_path: Path) -> None:
        for package_info in self.package_index.list_package_infos():
            if package_type.lower() != "__all__" and package_info.package_type != package_type:
                continue

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
            self, old_import_name: str, new_import_name: str,
            no_fix_imports: bool, no_move_files: bool,
    ) -> None:
        self.package_manager.rename_module(
            old_import_name=old_import_name, new_import_name=new_import_name,
            fix_imports=not no_fix_imports, move_files=not no_move_files,
        )

    def import_list(self, package_name: str) -> None:
        """List imports in a package"""
        imports = self.package_manager.get_imports(package_name)
        for imported_name in imports:
            print(imported_name)

    def req_list(self, package_name: str) -> None:
        """List requirements of a package"""
        req_lists = self.package_manager.get_requirements(package_name)
        for req_list_name, req_list in sorted(req_lists.items()):
            print(req_list_name)
            for req_item in req_list.req_specs:
                print(f'    {req_item.pretty()}')

    def check_requirements(self, package_name: str, ignore_prefix: Optional[str] = None, tests: bool = False) -> None:
        """Compares imports and requirements of a package"""
        extra_import_specs, extra_req_specs = self.package_manager.compare_imports_and_requirements(
            package_name, ignore_prefix=ignore_prefix, tests=tests,
        )

        if extra_import_specs:
            print('Missing requirements:')
            for req_item in extra_import_specs:
                print(f'    {req_item.as_req_str()}')
            print()
        if extra_req_specs:
            print('Extra requirements:')
            for req_item in extra_req_specs:
                print(f'    {req_item.as_req_str()}')
            print()
        if not extra_req_specs and not extra_import_specs:
            print('Requirements are in sync with imports')

    def ensure_mypy_common(self):
        self.py_prj_editor.update_mypy_common()

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
            tofile="actual.txt"
        )
        for line in diff_lines:
            print(line, end=None)

    @classmethod
    def run(cls, args: argparse.Namespace) -> None:
        config_file_name = args.config

        fs_editor_type = args.fs_editor if not args.dry_run else 'virtual'
        repo_env = RepoEnvironmentLoader(
            config_file_name=config_file_name,
            override_fs_editor_type=fs_editor_type,
        ).load_env(base_path=_BASE_DIR)

        index_builder = PackageIndexBuilder(repo_env=repo_env)
        package_index = index_builder.build_index()

        package_navigator = PackageNavigator(repo_env=repo_env, package_index=package_index)
        tool = cls(
            package_index=package_index,
            package_navigator=package_navigator,
            package_manager=PackageManager(
                repo_env=repo_env,
                package_index=package_index,
                package_navigator=package_navigator,
                package_generator=PackageGenerator(package_index=package_index, repo_env=repo_env),
                package_reference=PackageReference(package_index=package_index, repo_env=repo_env),
            ),
            py_prj_editor=PyPrjEditor(
                repo_env=repo_env,
                package_index=package_index,

            ),
        )

        tool.validate_env()

        match args.command:
            case 'init':
                tool.init(package_name=args.package_name, package_type=args.package_type)
            case 'copy':
                tool.copy(package_name=args.package_name, from_package_name=args.from_package_name)
            case 'rename':
                tool.rename(package_name=args.package_name, new_package_name=args.new_package_name)
            case 'ch-package-type':
                tool.ch_package_type(package_name=args.package_name, package_type=args.package_type)
            case 'package-list':
                tool.package_list(package_type=args.package_type, mask=args.mask, base_path=Path(args.base_path))
            case 'rename-module':
                tool.rename_module(
                    old_import_name=args.old_import_name, new_import_name=args.new_import_name,
                    no_fix_imports=args.no_fix_imports, no_move_files=args.no_move_files,
                )
            case 'import-list':
                tool.import_list(package_name=args.package_name)
            case 'req-list':
                tool.req_list(package_name=args.package_name)
            case 'req-check':
                tool.check_requirements(package_name=args.package_name, ignore_prefix=args.ignore_prefix,
                                        tests=args.tests)
            case 'ensure-mypy-common':
                tool.ensure_mypy_common()
            case 'compare-resulting-deps':
                tool.compare_resulting_deps(base_revision=args.base_rev, group=args.group)
            case _:
                raise RuntimeError(f'Got unknown command: {args.command}')


def main() -> None:
    setup_basic_logging()
    parser = make_parser()
    DlRepManagerTool.run(parser.parse_args())


if __name__ == '__main__':
    main()
