import pathlib
import tempfile
import typing
import uuid

import attr
import typing_extensions
import yaml


@attr.s()
class TmpConfigs:
    _dir: tempfile.TemporaryDirectory = attr.ib(factory=tempfile.TemporaryDirectory)

    def __enter__(self) -> typing_extensions.Self:
        return self

    def __exit__(self, exc_type: typing.Any, exc_value: typing.Any, traceback: typing.Any) -> None:
        self.cleanup()

    def cleanup(self) -> None:
        self._dir.cleanup()

    def add(
        self,
        content: dict[str, typing.Any],
    ) -> pathlib.Path:
        file_path = pathlib.Path(self._dir.name) / f"{uuid.uuid4()}.yaml"

        with file_path.open("w") as file:
            yaml.safe_dump(content, file)

        return file_path


__all__ = ["TmpConfigs"]
