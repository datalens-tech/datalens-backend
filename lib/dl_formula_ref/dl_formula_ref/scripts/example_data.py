from __future__ import annotations

import argparse

from dl_db_testing.loader import load_db_testing_lib
from dl_formula.core.dialect import (
    DialectCombo,
    get_dialect_from_str,
)
from dl_formula_ref.generator import (
    ConfigVersion,
    ReferenceDocGenerator,
)
from dl_formula_ref.loader import load_formula_ref
from dl_formula_ref.scripts.common import conf_version_type


def dialect_type(s: str) -> DialectCombo:
    return get_dialect_from_str(s)


def get_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="Example data management tool")
    subparsers = parser.add_subparsers(title="command", dest="command")

    generate_parser = subparsers.add_parser("generate", help="Generate data for examples")
    generate_parser.add_argument("--output", help="Output file path")
    generate_parser.add_argument("--db-config", help="Database configuration file")
    generate_parser.add_argument(
        "--default-dialect", type=dialect_type, help="Default dialect to use for example data generation"
    )
    generate_parser.add_argument(
        "--config-version", type=conf_version_type, default=ConfigVersion.default.name, help="Configuration version"
    )
    return parser


class ExampleDataTool:
    @classmethod
    def generate_example_data(
        cls, config_version: ConfigVersion, output_path: str, db_config_path: str, default_dialect: DialectCombo
    ) -> None:
        """
        Requires a DB URL mapping in local file `dl_formula_ref/db_config.json`
        with the following format:

        {
            "CLICKHOUSE_21_8": "clickhouse://datalens:qwerty@localhost:50456/formula_test"
        }

        defining all the required database types:
        - CLICKHOUSE_21_8
        """
        ref_doc_generator = ReferenceDocGenerator(locale="en", config_version=config_version)
        ref_doc_generator.generate_example_data(
            output_path=output_path,
            db_config_path=db_config_path,
            default_dialect=default_dialect,
        )

        print("Generated data successfully")

    @classmethod
    def run(cls, args: argparse.Namespace) -> None:
        tool = cls()

        match args.command:
            case "generate":
                tool.generate_example_data(
                    config_version=args.config_version,
                    output_path=args.output,
                    db_config_path=args.db_config,
                    default_dialect=args.default_dialect,
                )
            case _:
                raise RuntimeError(f"Invalid command {args.command}")


def main() -> None:
    load_formula_ref()
    load_db_testing_lib()
    parser = get_parser()
    ExampleDataTool.run(parser.parse_args())


if __name__ == "__main__":
    main()
