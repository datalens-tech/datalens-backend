from __future__ import annotations

import argparse
import os

import attr

from dl_repmanager.package_meta_reader import PackageMetaReader


def make_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog='DL Package Meta CLI')
    parser.add_argument('package_path')

    subparsers = parser.add_subparsers(title='command', dest='command')

    subparsers.add_parser('list-i18n-domains', help='List i18n domains and scan paths')

    return parser


@attr.s
class DlPackageMetaTool:
    meta_reader: PackageMetaReader = attr.ib(kw_only=True)

    def validate_env(cls) -> None:
        """Validate that the tool is being run correctly"""

    def list_i18n_domains(self) -> None:
        for domain, path in sorted(self.meta_reader.get_i18n_domains().items()):
            print(f'{domain} = {path}')

    @classmethod
    def run(cls, args: argparse.Namespace) -> None:
        with PackageMetaReader.from_file(os.path.join(args.package_path, 'pyproject.toml')) as meta_reader:
            tool = cls(meta_reader=meta_reader)

            if args.command == 'list-i18n-domains':
                tool.list_i18n_domains()

            else:
                raise RuntimeError(f'Got unknown command: {args.command}')


def main() -> None:
    parser = make_parser()
    DlPackageMetaTool.run(parser.parse_args())


if __name__ == '__main__':
    main()
