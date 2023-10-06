import abc
import argparse
from typing import Sequence

import attr


@attr.s
class CliToolBase(abc.ABC):
    @classmethod
    @abc.abstractmethod
    def get_parser(cls) -> argparse.ArgumentParser:
        raise NotImplementedError

    @classmethod
    @abc.abstractmethod
    def run_parsed_args(cls, args: argparse.Namespace) -> None:
        raise NotImplementedError

    @classmethod
    def run(cls, argv: Sequence[str]) -> None:
        parser = cls.get_parser()
        cls.run_parsed_args(parser.parse_args(argv))
