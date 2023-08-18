from __future__ import annotations

import re
from os import path, makedirs
from tempfile import TemporaryDirectory
from typing import List, Union, overload, Literal

import attr

BytesOrStr = Union[bytes, str]


def decode_output(b: bytes) -> str:
    return b.decode('ascii', errors='escape')


@overload
def ensure_ascii_string(inp: BytesOrStr) -> str:
    pass


@overload
def ensure_ascii_string(inp: BytesOrStr, return_bytes: Literal[False]) -> str:
    pass


@overload
def ensure_ascii_string(inp: BytesOrStr, return_bytes: Literal[True]) -> bytes:
    pass


def ensure_ascii_string(inp, return_bytes=False):  # type: ignore  # TODO: fix
    prepared_value: bytes
    if isinstance(inp, bytes):
        prepared_value = inp
    elif isinstance(inp, str):
        prepared_value = inp.encode("ascii")
    else:
        raise TypeError("Unexpected type")

    # Space and all printable ascii symbols
    if re.match(b'^[\x20-\x7e]*$', prepared_value):
        if return_bytes:
            return prepared_value
        return prepared_value.decode("ascii")


@attr.s
class BuildContext:
    _temp_folder: TemporaryDirectory = attr.ib(init=False, factory=TemporaryDirectory)

    def put(self, sub_path: str, content: bytes) -> None:
        if path.dirname(sub_path):
            makedirs(path.join(self.location, path.dirname(sub_path)), exist_ok=True)

        dest_path = path.join(self.location, sub_path)

        with open(dest_path, 'wb') as f:
            f.write(content)

    @property
    def location(self) -> str:
        return self._temp_folder.name

    def __enter__(self) -> BuildContext:
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:  # type: ignore  # TODO: fix
        self._temp_folder.cleanup()


@attr.s
class DockerFile:
    _from_img: str = attr.ib()
    _commands_list: List[bytes] = attr.ib(init=False, factory=list)

    def copy(self, src: str, dst: str) -> DockerFile:
        self._commands_list.append(
            ensure_ascii_string(f"COPY {ensure_ascii_string(src)} {ensure_ascii_string(dst)}", return_bytes=True)
        )
        return self

    def run(self, cmd: BytesOrStr) -> DockerFile:
        self._commands_list.append(
            ensure_ascii_string(f"RUN {ensure_ascii_string(cmd)}", return_bytes=True)
        )
        return self

    def env(self, key: BytesOrStr, value: BytesOrStr) -> DockerFile:
        self._commands_list.append(
            ensure_ascii_string(f"ENV {ensure_ascii_string(key)} {ensure_ascii_string(value)}", return_bytes=True)
        )
        return self

    def render(self) -> bytes:
        return b"\n".join([
            ensure_ascii_string(f"FROM {self._from_img}", return_bytes=True),
            *self._commands_list,
        ])
