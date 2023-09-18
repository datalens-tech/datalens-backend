from __future__ import annotations

import argparse

from dl_db_testing.loader import load_bi_db_testing
from dl_formula_ref.generator import (
    ConfigVersion,
    ReferenceDocGenerator,
)
from dl_formula_ref.loader import load_formula_ref

parser = argparse.ArgumentParser(prog="Example data management tool")
subparsers = parser.add_subparsers(title="command", dest="command")

subparsers.add_parser("generate", help="Generate data for examples")


class ExampleDataTool:
    @classmethod
    def generate_example_data(cls):
        """
        Requires a DB URL mapping in local file `dl_formula_ref/db_config.json`
        with the following format:

        {
            "CLICKHOUSE_21_8": "clickhouse://datalens:qwerty@localhost:50456/formula_test"
        }

        defining all the required database types:
        - CLICKHOUSE_21_8
        """
        ref_doc_generator = ReferenceDocGenerator(locale="en", config_version=ConfigVersion.default)
        ref_doc_generator.generate_example_data()

        print("Generated data successfully")

    @classmethod
    def run(cls, args):
        tool = cls()

        if args.command == "generate":
            tool.generate_example_data()


def main():
    load_formula_ref()
    load_bi_db_testing()
    ExampleDataTool.run(parser.parse_args())


if __name__ == "__main__":
    main()
