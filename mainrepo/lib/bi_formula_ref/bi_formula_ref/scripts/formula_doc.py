from __future__ import annotations

import argparse

from bi_formula_ref.loader import load_formula_ref
from bi_formula_ref.generator import ReferenceDocGenerator, ConfigVersion
from bi_formula_ref.localization import DEFAULT_LOCALE, get_locales


def conf_version_type(s: str) -> ConfigVersion:
    return ConfigVersion[s]

parser = argparse.ArgumentParser(prog='Formula documentation command line tool')
subparsers = parser.add_subparsers(title='command', dest='command')

locale_parser = argparse.ArgumentParser(add_help=False)
locale_parser.add_argument('--locale', help='Locale', default=DEFAULT_LOCALE)

outdir_parser = argparse.ArgumentParser(add_help=False)
outdir_parser.add_argument('outdir', help='Directory where to save generated files')

subparsers.add_parser('locales', help='List available locales')

generate_parser = subparsers.add_parser(
    'generate', parents=[outdir_parser, locale_parser],
    help='Generate function documentation directory'
)
generate_parser.add_argument(
    '--config-version', type=conf_version_type, default=ConfigVersion.yacloud.name,  # FIXME
    help='Configuration version'
)


class FormulaDocTool:
    @staticmethod
    def print_locales():
        print('\n'.join(sorted(get_locales())))

    @classmethod
    def generate_doc(cls, outdir: str, locale: str, config_version: ConfigVersion = ConfigVersion.default):
        ref_doc_generator = ReferenceDocGenerator(locale=locale, config_version=config_version)
        ref_doc_generator.generate_doc_full_dir(outdir=outdir)

    @classmethod
    def run(cls, args):
        tool = cls()

        if args.command == 'locales':
            tool.print_locales()
        elif args.command == 'generate':
            tool.generate_doc(
                outdir=args.outdir, locale=args.locale, config_version=args.config_version,
            )


def main():
    load_formula_ref()
    FormulaDocTool.run(parser.parse_args())


if __name__ == '__main__':
    main()
