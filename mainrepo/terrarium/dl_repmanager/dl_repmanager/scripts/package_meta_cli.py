from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

import attr

from dl_repmanager.fs_editor import DefaultFilesystemEditor
from dl_repmanager.repository_env import DEFAULT_CONFIG_FILE_NAME, discover_config
from dl_repmanager.exceptions import PackageMetaCliError, InconsistentStateError
from dl_repmanager.package_meta_reader import PackageMetaReader, PackageMetaWriter, PackageMetaIOFactory
from dl_repmanager.logging import setup_basic_logging

from dl_repmanager.mypy_stubs_sync import stubs_sync, RequirementsPathProvider


log = logging.getLogger(__name__)


def make_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog='DL Package Meta CLI')
    parser.add_argument('package_path')

    subparsers = parser.add_subparsers(title='command', dest='command')

    subparsers.add_parser('list-i18n-domains', help='List i18n domains and scan paths')
    subparsers.add_parser('list-external-dependencies', help='List external dependencies')

    sync_mypy_stubs = argparse.ArgumentParser(add_help=False)
    sync_mypy_stubs.add_argument("--dry-run", default=True, action=argparse.BooleanOptionalAction)
    sync_mypy_stubs.add_argument("--config", default=DEFAULT_CONFIG_FILE_NAME)
    subparsers.add_parser(
        'sync-mypy-stubs',
        parents=[sync_mypy_stubs],
        help='Update or add types-* packages to third party dependencies',
    )
    return parser


@attr.s
class DlPackageMetaTool:
    meta_reader: PackageMetaReader = attr.ib(kw_only=True)
    meta_writer: PackageMetaWriter = attr.ib(kw_only=True)

    def validate_env(cls) -> None:
        """Validate that the tool is being run correctly"""

    def list_i18n_domains(self) -> None:
        for domain, path_list in sorted(self.meta_reader.get_i18n_domains().items()):
            print(f'{domain}={";".join(path_list)}')

    def sync_mypy_stubs(self, pkg_path: Path, dry_run: bool = True, config_name: str | None = None) -> None:
        base_path = discover_config(pkg_path, config_name or DEFAULT_CONFIG_FILE_NAME).parent
        path_provider = RequirementsPathProvider(base_path=base_path)
        stubs_sync(self.meta_reader, self.meta_writer, path_provider, dry_run)

    @classmethod
    def run(cls, args: argparse.Namespace) -> None:
        toml_path = Path(args.package_path) / 'pyproject.toml'
        fs_editor = DefaultFilesystemEditor(base_path=toml_path.parent)
        package_meta_io_factory = PackageMetaIOFactory(fs_editor=fs_editor)
        with (
            package_meta_io_factory.package_meta_reader(toml_path) as meta_reader,
            package_meta_io_factory.package_meta_writer(toml_path) as meta_writer,
        ):
            tool = cls(meta_reader=meta_reader, meta_writer=meta_writer)

            match args.command:
                case 'list-i18n-domains':
                    tool.list_i18n_domains()
                case 'sync-mypy-stubs':
                    tool.sync_mypy_stubs(Path(args.package_path), args.dry_run, args.config)
                case _:
                    raise RuntimeError(f'Got unknown command: {args.command}')


def main() -> None:
    setup_basic_logging()
    parser = make_parser()
    try:
        DlPackageMetaTool.run(parser.parse_args())
    except InconsistentStateError:
        log.exception("Project inconsistent state discovered during cli command run.")
        sys.exit(1)
    except PackageMetaCliError:
        log.exception("Generic error occurred, exiting")
        sys.exit(10)


if __name__ == '__main__':
    main()
