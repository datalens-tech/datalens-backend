from __future__ import annotations

import argparse

from dl_formula_ref.generator import (
    ConfigVersion,
    ReferenceDocGenerator,
    get_generator_config,
)
from dl_formula_ref.loader import load_formula_ref
from dl_formula_ref.localization import DEFAULT_LOCALE


def conf_version_type(s: str) -> ConfigVersion:
    return ConfigVersion[s]


parser = argparse.ArgumentParser(prog="Formula documentation command line tool")
subparsers = parser.add_subparsers(title="command", dest="command")

config_version = argparse.ArgumentParser(add_help=False)
config_version.add_argument(
    "--config-version", type=conf_version_type, default=ConfigVersion.default.name, help="Configuration version"
)

locale_parser = argparse.ArgumentParser(add_help=False)
locale_parser.add_argument("--locale", help="Locale", default=DEFAULT_LOCALE)

outdir_parser = argparse.ArgumentParser(add_help=False)
outdir_parser.add_argument("outdir", help="Directory where to save generated files")

subparsers.add_parser("config-versions", help="List available config versions")

subparsers.add_parser("locales", parents=[config_version], help="List available locales")

subparsers.add_parser(
    "generate", parents=[outdir_parser, config_version, locale_parser], help="Generate function documentation directory"
)


class FormulaDocTool:
    @staticmethod
    def print_versions():
        print("\n".join(sorted([version.name for version in ConfigVersion])))

    @staticmethod
    def print_locales(config_version: ConfigVersion):
        gen_config = get_generator_config(version=config_version)
        print("\n".join(sorted(gen_config.supported_locales)))

    @classmethod
    def generate_doc(cls, outdir: str, locale: str, config_version: ConfigVersion):
        ref_doc_generator = ReferenceDocGenerator(locale=locale, config_version=config_version)
        ref_doc_generator.generate_doc_full_dir(outdir=outdir)

    @classmethod
    def run(cls, args):
        tool = cls()

        match args.command:
            case "config-versions":
                tool.print_versions()
            case "locales":
                tool.print_locales(config_version=args.config_version)
            case "generate":
                tool.generate_doc(outdir=args.outdir, locale=args.locale, config_version=args.config_version)


def main():
    load_formula_ref()
    FormulaDocTool.run(parser.parse_args())


if __name__ == "__main__":
    main()
