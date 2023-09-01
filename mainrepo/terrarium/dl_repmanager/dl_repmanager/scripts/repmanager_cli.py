"""
Tool for management and analysis of bi packages

Examples:

    python tools/local_dev/bi_package.py init-lib --package-name bi_connector_clickhouse

    python tools/local_dev/bi_package.py move-code --old-import-name bi_core.connectors.clickhouse --new-import-name bi_connector_clickhouse.core

"""

from __future__ import annotations

import argparse
import os
from pathlib import Path
from typing import Optional

import attr

from dl_repmanager.env import DEFAULT_CONFIG_FILE_NAME, RepoEnvironmentLoader
from dl_repmanager.package_index import PackageIndexBuilder, PackageIndex
from dl_repmanager.package_navigator import PackageNavigator
from dl_repmanager.package_reference import PackageReference
from dl_repmanager.package_manager import PackageManager, PackageGenerator


def make_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog='DL Repository Management CLI')
    parser.add_argument('--config', help='Specify configuration file', default=DEFAULT_CONFIG_FILE_NAME)

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

    return parser


_BASE_DIR = Path.cwd()


@attr.s
class DlRepManagerTool:
    package_index: PackageIndex = attr.ib(kw_only=True)
    package_manager: PackageManager = attr.ib(kw_only=True)
    package_navigator: PackageNavigator = attr.ib(kw_only=True)

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
            if package_info.package_type != package_type:
                continue

            printable_values = dict(
                package_type=package_info.package_type,
                abs_path=package_info.abs_path,
                rel_path=os.path.relpath(package_info.abs_path, base_path),
                single_module_name=package_info.single_module_name,
                package_reg_name=package_info.package_reg_name,
            )
            print(mask.format(**printable_values))

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

    @classmethod
    def run(cls, args: argparse.Namespace) -> None:
        config_file_name = args.config

        repo_env = RepoEnvironmentLoader(config_file_name=config_file_name).load_env(base_path=_BASE_DIR)
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
            )
        )

        tool.validate_env()

        if args.command == 'init':
            tool.init(package_name=args.package_name, package_type=args.package_type)

        elif args.command == 'copy':
            tool.copy(package_name=args.package_name, from_package_name=args.from_package_name)

        elif args.command == 'rename':
            tool.rename(package_name=args.package_name, new_package_name=args.new_package_name)

        elif args.command == 'ch-package-type':
            tool.ch_package_type(package_name=args.package_name, package_type=args.package_type)

        elif args.command == 'package-list':
            tool.package_list(package_type=args.package_type, mask=args.mask, base_path=Path(args.base_path))

        elif args.command == 'rename-module':
            tool.rename_module(
                old_import_name=args.old_import_name, new_import_name=args.new_import_name,
                no_fix_imports=args.no_fix_imports, no_move_files=args.no_move_files,
            )

        elif args.command == 'import-list':
            tool.import_list(package_name=args.package_name)

        elif args.command == 'req-list':
            tool.req_list(package_name=args.package_name)

        elif args.command == 'req-check':
            tool.check_requirements(package_name=args.package_name, ignore_prefix=args.ignore_prefix, tests=args.tests)

        else:
            raise RuntimeError(f'Got unknown command: {args.command}')


def main() -> None:
    parser = make_parser()
    DlRepManagerTool.run(parser.parse_args())


if __name__ == '__main__':
    main()
