from __future__ import annotations

import argparse
import logging
from pathlib import Path
import sys
from typing import cast

import attr

from dl_cli_tools.cli_base import CliToolBase  # type: ignore
from dl_cli_tools.logging import setup_basic_logging  # type: ignore
from dl_repmanager.exceptions import (
    InconsistentStateError,
    PackageMetaCliError,
)
from dl_repmanager.fs_editor import (
    DefaultFilesystemEditor,
    FilesystemEditor,
)
from dl_repmanager.mypy_stubs_sync import (
    RequirementsPathProvider,
    stubs_sync,
)
from dl_repmanager.package_meta_reader import (
    PackageMetaIOFactory,
    PackageMetaReader,
    PackageMetaWriter,
)
from dl_repmanager.repository_env import (
    DEFAULT_CONFIG_FILE_NAME,
    discover_config,
)


log = logging.getLogger(__name__)


def make_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="DL Package Meta CLI")
    parser.add_argument("--package-path", type=Path)

    package_subparsers = parser.add_subparsers(title="command", dest="command")
    add_package_commands(package_subparsers)
    return parser


def add_package_commands(package_subparsers: argparse._SubParsersAction) -> None:
    package_subparsers.add_parser("list-i18n-domains", help="List i18n domains and scan paths")
    package_subparsers.add_parser("list-external-dependencies", help="List external dependencies")

    sync_mypy_stubs = argparse.ArgumentParser(add_help=False)
    sync_mypy_stubs.add_argument("--dry-run", action="store_true")
    sync_mypy_stubs.add_argument("--config", default=DEFAULT_CONFIG_FILE_NAME)
    package_subparsers.add_parser(
        "sync-mypy-stubs",
        parents=[sync_mypy_stubs],
        help="Update or add types-* packages to third party dependencies",
    )

    src_path_parser = argparse.ArgumentParser(add_help=False)
    src_path_parser.add_argument("--src-path", type=Path, required=True, help="Source path")

    dst_path_parser = argparse.ArgumentParser(add_help=False)
    dst_path_parser.add_argument("--dst-path", type=Path, required=True, help="Destination path")

    package_subparsers.add_parser(
        "copy",
        parents=[src_path_parser, dst_path_parser],
        help="Copy file to/within package",
    )
    package_subparsers.add_parser("rm", parents=[src_path_parser], help="Remove file or dir in package")

    set_toml_meta_parser = argparse.ArgumentParser(add_help=False)
    set_toml_meta_parser.add_argument("--toml-section", required=True, help="Section name in pyproject.toml")
    set_toml_meta_parser.add_argument("--toml-key", required=True, help="Key within the section")
    set_toml_meta_parser.add_argument("--toml-value", required=True, help="Value for the key")

    package_subparsers.add_parser("set-meta-text", parents=[set_toml_meta_parser], help="Set text value in toml")
    package_subparsers.add_parser("set-meta-array", parents=[set_toml_meta_parser], help="Set array value in toml")


@attr.s
class DlPackageMetaTool(CliToolBase):
    fs_editor: FilesystemEditor = attr.ib(kw_only=True)
    package_path: Path = attr.ib(kw_only=True)
    meta_reader: PackageMetaReader = attr.ib(kw_only=True)
    meta_writer: PackageMetaWriter = attr.ib(kw_only=True)

    @classmethod
    def get_parser(cls) -> argparse.ArgumentParser:
        return make_parser()

    def list_i18n_domains(self) -> None:
        for domain_spec in sorted(self.meta_reader.get_i18n_domains(), key=lambda spec: spec.domain_name):
            paths = [str(path) for path in domain_spec.scan_paths]
            print(f'{domain_spec.domain_name}={";".join(paths)}')

    def sync_mypy_stubs(self, pkg_path: Path, dry_run: bool = True, config_name: str | None = None) -> None:
        base_path = discover_config(pkg_path, config_name or DEFAULT_CONFIG_FILE_NAME).parent
        path_provider = RequirementsPathProvider(base_path=base_path)
        stubs_sync(self.meta_reader, self.meta_writer, path_provider, dry_run)

    def copy_path(self, src_path: Path, dst_path: Path) -> None:
        if src_path.is_absolute():
            abs_src_path = src_path
        else:
            abs_src_path = self.package_path / src_path
        abs_dst_path = self.package_path / dst_path
        if self.fs_editor.path_exists(abs_dst_path):
            self.fs_editor.remove_path(abs_dst_path)
        self.fs_editor.copy_path(abs_src_path, abs_dst_path)

    def rm_path(self, src_path: Path) -> None:
        abs_src_path = self.package_path / src_path
        if self.fs_editor.path_exists(abs_src_path):
            self.fs_editor.remove_path(abs_src_path)

    def set_meta_text(self, toml_section: str, toml_key: str, toml_value: str) -> None:
        self.meta_writer.toml_writer.set_text_value(section_name=toml_section, key=toml_key, value=toml_value)

    def set_meta_array(self, toml_section: str, toml_key: str, toml_value: str) -> None:
        arr_value = toml_value.split(";")
        self.meta_writer.toml_writer.set_array_value(section_name=toml_section, key=toml_key, value=arr_value)

    @classmethod
    def run_parsed_args(cls, args: argparse.Namespace) -> None:
        package_path = cast(Path, args.package_path)
        fs_editor = DefaultFilesystemEditor(base_path=package_path)
        cls.run_for_package_path(
            fs_editor=fs_editor,
            package_path=package_path,
            package_command=args.command,
            args=args,
        )

    @classmethod
    def run_for_package_path(
        cls,
        fs_editor: FilesystemEditor,
        package_path: Path,
        package_command: str,
        args: argparse.Namespace,
    ) -> None:
        toml_path = package_path / "pyproject.toml"
        package_meta_io_factory = PackageMetaIOFactory(fs_editor=fs_editor)
        with (
            package_meta_io_factory.package_meta_reader(toml_path) as meta_reader,
            package_meta_io_factory.package_meta_writer(toml_path) as meta_writer,
        ):
            tool = cls(
                fs_editor=fs_editor,
                package_path=package_path,
                meta_reader=meta_reader,
                meta_writer=meta_writer,
            )

            match package_command:
                case "list-i18n-domains":
                    tool.list_i18n_domains()
                case "sync-mypy-stubs":
                    tool.sync_mypy_stubs(Path(package_path), args.dry_run, args.config)
                case "copy":
                    tool.copy_path(src_path=args.src_path, dst_path=args.dst_path)
                case "rm":
                    tool.rm_path(src_path=args.src_path)
                case "set-meta-text":
                    tool.set_meta_text(
                        toml_section=args.toml_section,
                        toml_key=args.toml_key,
                        toml_value=args.toml_value,
                    )
                case "set-meta-array":
                    tool.set_meta_array(
                        toml_section=args.toml_section,
                        toml_key=args.toml_key,
                        toml_value=args.toml_value,
                    )
                case _:
                    raise RuntimeError(f"Got unknown command: {args.command}")


def main() -> None:
    setup_basic_logging()
    try:
        DlPackageMetaTool.run(sys.argv[1:])
    except InconsistentStateError:
        log.exception("Project inconsistent state discovered during cli command run.")
        sys.exit(1)
    except PackageMetaCliError:
        log.exception("Generic error occurred, exiting")
        sys.exit(10)


if __name__ == "__main__":
    main()
