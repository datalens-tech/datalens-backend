import contextlib
import os
from typing import Iterator, TextIO

import attr

from bi_external_api.attrs_model_mapper_docs.render_units import DocUnit, RenderContext


@attr.s(auto_attribs=True)
class DocWriter:
    render_ctx: RenderContext
    base_dir: str

    _writen_paths: set[str] = attr.ib(init=False, factory=set)

    def was_path_written(self, rel_path: str) -> bool:
        return rel_path in self._writen_paths

    @contextlib.contextmanager
    def _file_cm(self, rel_path: str) -> Iterator[TextIO]:
        if not os.path.isdir(self.base_dir):
            raise ValueError(f"Base directory is not exists: {self.base_dir}")
        if not os.path.isdir(self.base_dir):
            raise ValueError(f"Base directory is not a directory: {self.base_dir}")
        if self.was_path_written(rel_path):
            raise ValueError(f"Attempt to overwrite file: {rel_path}")

        self._writen_paths.add(rel_path)

        full_path = os.path.join(self.base_dir, rel_path)
        os.makedirs(os.path.dirname(full_path), exist_ok=True)

        with open(full_path, "w") as file:
            yield file

    def write(self, doc_unit: DocUnit, rel_path: str, append_nl: bool = False) -> None:
        with self._file_cm(rel_path) as file:
            file.write("\n".join(doc_unit.render_md(self.render_ctx.with_current_file(rel_path))))
            if append_nl:
                file.write("\n")

    def write_text(self, txt: str, rel_path: str) -> None:
        with self._file_cm(rel_path) as file:
            file.write(txt)
