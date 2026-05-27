from __future__ import annotations

import os
import tempfile
from typing import (
    Callable,
    Union,
)
import uuid

from graphviz.dot import Dot


def csv_type[TV](type_func: Callable[[str], TV]) -> Callable[[str], Union[list[TV], str]]:
    def wrapper(s: str) -> Union[list[TV], str]:
        if isinstance(s, str):
            return [type_func(it) for it in s.split(",")]
        return s

    return wrapper


def make_graphviz_graph(dot: Dot, render_to: str, view: bool) -> None:
    if render_to:
        if not render_to.endswith(".png"):
            raise ValueError("--render-to must have .png extension")
        render_to = render_to[:-4]

    if render_to or view:
        if not render_to:
            render_to = os.path.join(tempfile.gettempdir(), f"dotfile_{uuid.uuid4().hex}")

        dot.format = "png"
        dot.render(render_to, view=view, cleanup=True)

    else:
        print(dot.source)
